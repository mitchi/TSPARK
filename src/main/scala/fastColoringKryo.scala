package fastColoringKryo

import central.gen
import central.gen.{filename, verifyTestSuite}
import cmdlineparser.TSPARK.save
import cmdlineparser.TSPARK.compressRuns
import enumerator.distributed_enumerator.fastGenCombos
import fastColoringKryo.fastColoring.distributed_fastcoloring
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import utils.utils
import progressivecoloring.progressive_coloring.assign_numberClauses

import scala.util.Random
import ordercoloring.OrderColoring.coloringToTests
import org.apache.spark.storage.StorageLevel.{MEMORY_ONLY_SER, OFF_HEAP}
import org.roaringbitmap.RoaringBitmap

object fastColoring extends Serializable {

  var debug = false

  /**
    *
    */
  def fastcoloring(combos: RDD[Array[Char]],
                   sc: SparkContext,
                   chunkSize: Int,
                   n: Int, v : Int,
                   algorithm: String = "OC") = {

    val count = combos.count()
    //Toutes nos couleurs sont la
    val colors = new Array[Int](count.toInt)
    val seed = if (debug == true) 20 else System.nanoTime()
    var totalIterations = 0

    //Shuffle the combos before. Doing this ensures a different result every run.
    var mycombos = combos.mapPartitions(it => {
      Random.setSeed(seed)
      Random.shuffle(it)
    }, true)

    //Assign numbers to clauses using a custom algorithm
    val combosNumbered = assign_numberClauses(mycombos, sc).cache()
    println("Shuffling and numbering the clauses is complete")

    //We no longer need the not-numbered combos
    //mycombos.unpersist(false)

    //Before everything, we can color the first vertex with the color 1
    var maxColor = 1
    colors(0) = 1
    var i = 1 //the number of vertices we have colored
    var chunkNo = 0

    var tableau = initTableau(n,v)
    var etoiles = initTableauEtoiles(n)

    //On ajoute l'info du premier combo
    val firstCombo = combosNumbered.flatMap(e => {
      if (e._2 == 0) Some(e)
      else None
    }).collect()

    println("On ajoute l'info du premier combo dans le tableau, et tableau etoiles")
    tableau = addToTableau(tableau, firstCombo, n, v)
    etoiles = addTableauEtoiles(etoiles, firstCombo, n, v)

    loop

    def loop(): Unit = {

      if (i >= count) return //if we have colored everything, we are good to go

      chunkNo += 1
      println(s"Now processing Chunk $chunkNo of the graph...")
      println("Calling Garbage Collection...")
      System.gc() //calling garbage collection here

      //Filter the combos we color in the next OrderColoring iteration
      println("We are now  creating the chunk we need...")
      val someCombos = combosNumbered.flatMap(elem => {
        if (elem._2 < i + chunkSize && elem._2 >= i) { //if chunksize = 10k, then  i < 10001 and 1 >=1. 2ème itération  < 20001 et >=10001
          Some(elem)
        }
        else None
      }).collect().sortBy(_._2)

      val sizeOfChunk = someCombos.size
      println(s"Currently working with a chunk of the graph with $sizeOfChunk vertices.")

      //val filteredCombos = filterBig(combosNumbered, i, sizeOfChunk)

      println("Adding the entries of the chunk to the tableau...")
      tableau = addToTableau(tableau, someCombos, n, v)
      etoiles = addTableauEtoiles(etoiles, someCombos, n, v)

      val r1 = generateadjlist_fastcoloring(i, sizeOfChunk, combosNumbered, tableau, etoiles, sc).persist(MEMORY_ONLY_SER)

      //Use KP when the graph is sparse (not dense)
      val r2 =
        if (algorithm == "KP") {
          progressiveKP(colors, r1, r1.count().toInt, maxColor, sc)
        }
        else { //algorithm = "OC". The single threaded needs a sorted matrix of adjlists
          ordercoloring(colors, r1.collect().sortBy(_._1), i, maxColor)
        }

      if (debug == true) {
        println("On imprime le tableau des couleurs après coloriage du chunk par OrderColoring")
        for (i <- 0 until colors.size) {
          val v = colors(i)
          println(s"vertex $i has color $v")
        }
      }

      //Update the max color
      totalIterations += r2._1
      maxColor = r2._2
      println(s"max color is now $maxColor")

      r1.unpersist(true)
      i += sizeOfChunk //1 + 10000 = 10001

      println(s"i is now $i")

      loop
    }

    val percent = ((totalIterations.toDouble / count.toDouble) * 100)
    val vertexPerIteration = count / totalIterations
    println(s"We did a total of $totalIterations iterations, which is $percent% of total")
    println(s"We also colored $vertexPerIteration vertices per iteration on average")

    //Create tests now
    val bcastcolors = sc.broadcast(colors)
    val properFormRDD = combosNumbered.map(elem => {
      val id = elem._2
      val color = bcastcolors.value(id.toInt)
      (color, elem._1)
    })

    //Transform into tests
    coloringToTests(properFormRDD)
  }


  /**
    * Generate the list of impossible combos by "investing" the list of possible combos, and also using the list of combos with stars
    * @param id
    * @param list
    * @param etoiles
    * @return
    */
  def generateOtherList( id: Long,
                         list : RoaringBitmap,
                         etoiles: RoaringBitmap) = {
    //On construit une lookup table avec la liste des bons combos
    //La liste est exactement de la taille du id
    var lookuptable = new Array[Byte](id.toInt)
    lookuptable = lookuptable.map( e => 0.toByte)

    //We take the data from the tableau, and we add it to the possiblyValidGuys, if the comboId is smaller than our id of course!
    val buffer = new Array[Int](256)
    val ite = list.getBatchIterator
    while (ite.hasNext) {
      val batch = ite.nextBatch(buffer)
      for (i <- 0 until batch) {
        if (buffer(i) < id) lookuptable( buffer(i) ) = 1
      }
    }

    //On ajoute les etoiles également
    val buffer2 = new Array[Int](256)
    val ite2 = etoiles.getBatchIterator
    while (ite2.hasNext) {
      val batch = ite2.nextBatch(buffer2)
      for (i <- 0 until batch) {
        if (buffer2(i) < id) lookuptable( buffer2(i) ) = 1
      }
    }

    //On inverse la lsite maintenant
    var adjlist = new RoaringBitmap()
    var index = 0
    for (i <- 0 until id.toInt) {
      if (lookuptable(i) == 0) {
        adjlist.add(i)
      }
    }

    adjlist
  }

  /**
    *
    * @param id
    * @param combo
    * @param tableau
    * @param etoiles
    * @return
    */
  def comboToADJ(id: Long,
                 combo: Array[Char],
                 tableau: Array[Array[RoaringBitmap]],
                 etoiles: Array[RoaringBitmap]) = {

    var i = 0 //quel paramètre?
    var certifiedInvalidGuys = new RoaringBitmap()

    //On crée le set des validguys a partir de notre tableau rempli
    for (it <- combo) {

      if (it != '*') {
        val paramVal = it - '0'
        val list = tableau(i)(paramVal) //on prend tous les combos qui ont cette valeur. (Liste complète)
        val listEtoiles = etoiles(i) //on va prendre tous les combos qui ont des etoiles pour ce parametre (Liste complète)
        val invalids = generateOtherList(id, list, listEtoiles)

        //On ajoute dans la grosse liste des invalides
        certifiedInvalidGuys.or(invalids)

      }
      //On va chercher la liste des combos qui ont ce paramètre-valeur
      i+=1
    }

    if (debug == true) {
      println(s"list of all certified invalid guys" + certifiedInvalidGuys.toString)
      println("\n\n")
    }

    //On retourne cette liste, qui contient au maximum chunkSize éléments
    //Il faut ajuster la valeur des éléments de cette liste pour les id du chunk
    certifiedInvalidGuys

  }

  /**
    * Input: RDD de combos, tableau, etoiles. Output: un RDD de (id, adjlist) pour colorier le graphew
    * @param i
    * @param step
    * @param combos
    * @param tableau
    * @param etoiles
    * @param sc
    * @return
    */
  def generateadjlist_fastcoloring(i : Long, step: Long,
                                   combos: RDD[(Array[Char], Long)],
                                   tableau: Array[Array[RoaringBitmap]],
                                   etoiles: Array[RoaringBitmap],
                                   sc : SparkContext) =
  {

    //Print the number of partitions we are using
    val partitions = combos.getNumPartitions
    println("Generating the adjacency lists using the fast graph construction algorithm...")
    println(s"Run compression : $compressRuns")
    println(s"Currently using $partitions partitions")
    println("Broadcasting tableau and etoiles...")
    val etoilesdata = sc.broadcast(etoiles)
    val tableaudata = sc.broadcast(tableau)


    val r1 = combos.flatMap( combo => {
      //le id du combat. On n'a pas besoin de mettre des sommets plus gros que lui dans sa liste d'adjacence
      val id = combo._2
      if (id != 0 && id < i+step && id >=i) { //Discard first combo, it is already colored. We don't need to compute its adjlist
        //Pour chaque combo du RDD, on va aller chercher la liste de tous les combos dans le chunk qui sont OK
        val adjlist = comboToADJ(id, combo._1, tableaudata.value, etoilesdata.value)

        if (compressRuns == true) adjlist.runOptimize()

        Some(combo._2, adjlist)
      }
      else {
        //println("Not calculing adjlist for first combo")
        None
      }
    })

    tableaudata.unpersist(false)
    etoilesdata.unpersist(false)

    r1
  }

  /**
    * On filtre les combos du RDD, comme ça on travaille pas avec les combos qui sont pas dans le chunk
    *
    * @param combos
    * @param i
    * @param step
    * @return
    */
  def filterBig(combos: RDD[(Array[Char], Long)], i: Int, step: Int):
  RDD[(Array[Char], Long)] = {
    combos.flatMap(e => {
      if (e._2 > i + step) None
      else Some(e)
    })
  }


  /**
    * On crée le tableau qu'on va utiliser.
    * On skip les * lorsqu'on remplit ce tableau avec les valeurs
    * Ce tableau se fait remplir avec chaque traitement de chunk.
    *
    *  */
  def initTableau(n: Int, v : Int) =
  {
    var tableau = new Array[Array[RoaringBitmap]](n)

    //On met très exactement n paramètres, chacun avec v valeurs. On ne gère pas les *
    for (i <- 0 until n ) {
      tableau(i) = new Array[RoaringBitmap](v)
      for (v <- 0 until v) {
        tableau(i)(v) = new RoaringBitmap()
      }
    }
    tableau
  }

  /**
    * On crée un tableau pour gérer seulement les étoiles
    * Roaring Bitmap dans le tableau
    */
  def initTableauEtoiles(n: Int) =
  {
    var tableauEtoiles = new Array[RoaringBitmap](n)

    for (i <- 0 until n) {
      tableauEtoiles(i) = new RoaringBitmap()
    }

    tableauEtoiles
  }


  def addTableauEtoiles(etoiles: Array[RoaringBitmap],
                        chunk: Array[(Array[Char], Long)], n : Int, v : Int ) = {

    //On remplit cette structure avec notre chunk
    for (combo <- chunk) { //pour chaque combo
      for (i <- 0 until n) { //pour chaque paramètre
        val cc = combo._1(i)
        if (cc == '*') {
          etoiles(i).add(combo._2.toInt)  //on ajoute dans le ArrayBuffer . On pourrait mettre l'index global aussi.mmm
        }
      }
    }
    etoiles
  }


  /**
  On recoit un tableau, et on ajoute l'information avec le chunk de combos
     On ajoute sans cesse dans le tableau
    * @param chunk
    * @param n
    * @param v
    */
  def addToTableau(tableau: Array[Array[RoaringBitmap]],
                   chunk: Array[(Array[Char], Long)], n : Int, v : Int) = {
    //On pourrait également faire Array[Array[Int]] et faire un indexage plus compliqué

    if (debug == true) {
      println("On print les combos du chunk")
      for (c <- chunk) {
        print(c._2 + " ") ; utils.print_helper(c._1)
      }
    }

    var indexCombo = 0
    //On remplit cette structure avec notre chunk
    for (combo <- chunk) { //pour chaque combo
      for (i <- 0 until n) { //pour chaque paramètre
        val cc = combo._1(i)
        if (cc != '*') {
          val vv = combo._1(i) - '0' //on va chercher la valeur
          tableau(i)(vv) add combo._2.toInt //on ajoute dans le ArrayBuffer . On pourrait mettre l'index global aussi.mmm
          //tableau(i)(vv) += indexCombo
        }
      }
      indexCombo +=1
    }

    //On retourne notre travail
    tableau
  }


  /**
    * Color N vertices at a time using the Order Coloring algorithm.
    * The adjvectors are precalculated
    *
    * @return
    */
  def ordercoloring(colors: Array[Int], adjMatrix: Array[(Long, RoaringBitmap)], i: Int,
                    maxColor: Int) = {

    //    import scala.io.StdIn.readLine
    //    println("On pause ici. Va voir la taille du truc dans Spark UI")
    //    var temp = readLine()

    val limit = adjMatrix.size //the number of iterations we do
    var j = 0 //start coloring using the first adjvector of the adjmatrix
    var currentMaxColor = maxColor

    loop

    def loop(): Unit = {

      //Only color n vertices in this loop
      if (j == limit) return

      //Build the neighborcolors data structure for this vertex (j)
      //We iterate through the adjvector to find every colored vertex he's connected to and take their color.
      val neighborcolors = new Array[Int](currentMaxColor + 2)

      //Batch iteration through the neighbors of this guy
      val buffer = new Array[Int](256)
      val it = adjMatrix(j)._2.getBatchIterator
      while (it.hasNext) {
        val batch = it.nextBatch(buffer)
        for (i <- 0 until batch) {
          val neighbor = buffer(i)
          val neighborColor = colors(neighbor)
          neighborcolors(neighborColor) = 1
        }
      }

      val foundColor = color(neighborcolors)
      colors(i + j) = foundColor

      if (foundColor > currentMaxColor) currentMaxColor = foundColor

      j += 1 //we color the next vertex
      loop
    }

    //Return the number of iterations we did, and the maxColor
    (limit, currentMaxColor)
  }

  /**
    * MUTABLE.
    * This function works with a lookup table of colors ( color not here = 0, here = 1)
    * Color using this function and good parameters
    * Use this function the vertex is adjacent to the other, and we want to know what the best color is
    *
    * @param vertices
    * @param vertex
    * @return THE COLOR THAT WE CAN TAKE
    */
  def color(neighbor_colors: Array[Int]): Int = {
    for (i <- 1 to neighbor_colors.length) {
      if (neighbor_colors(i) != 1) //if this color is not present
        return i //we choose it
    }
    return 1 //the case when the color lookuo table is empty
  }


  /**
    * The distributed graph coloring algorithm with the fast coloring construction algorithm
    *
    * @param n
    * @param t
    * @param v
    * @param sc
    * @return
    */
  def distributed_fastcoloring(n: Int, t: Int, v: Int, sc: SparkContext,
                               chunkSize: Int = 4000, algorithm: String = "OC"): Array[Array[Char]] = {
    val expected = utils.numberTWAYCombos(n, t, v)
    import cmdlineparser.TSPARK.compressRuns

    println("Distributed Graph Coloring with FastGraphConstruction algorithm & Roaring Bitmaps and Kryro Serialization")
    println(s"Run compression for Roaring Bitmap = $compressRuns")
    println(s"Using a chunk size = $chunkSize vertices and algorithm = $algorithm")
    println(s"Problem : n=$n,t=$t,v=$v")
    println(s"Expected number of combinations is : $expected ")
    println(s"Formula is C($n,$t) * $v^$t")

    import java.io._
    val pw = new PrintWriter(new FileOutputStream(filename, true))

    val t1 = System.nanoTime()

    val tests = fastcoloring(fastGenCombos(n, t, v, sc).cache(), sc, chunkSize, n, v, algorithm)

    val t2 = System.nanoTime()
    val time_elapsed = (t2 - t1).toDouble / 1000000000

    pw.append(s"$t;$n;$v;FASTCOLORINGKRYO;$time_elapsed;${tests.size}\n")
    println(s"$t;$n;$v;FASTCOLORINGKRYO;$time_elapsed;${tests.size}\n")
    pw.flush()

    //If the option to save to a text file is activated
    if (save == true) {
      println(s"Saving the test suite to a file named $t;$n;$v.txt")
      //Save the test suite to file
      utils.saveTestSuite(s"$t;$n;$v.txt", tests)
    }

    //Return the test suite
    tests
  }


  /**
    * We will modify the colors array.
    * This function returns when the K&P algorithm has colored all new vertices.
    *
    * The adjmatrix can be a RDD or an Array.
    * Ideally, we are using an adjmatrix that spans the whole cluster. This is a more effective way to use
    * all the RAM of the cluster.
    *
    * We can remove vertices when they are colored.
    *
    * @return
    */
  def progressiveKP(colors: Array[Int], adjMatrix: RDD[(Long, RoaringBitmap)], remaining: Int,
                    maxColor: Int, sc: SparkContext) = {


    var iterationCounter = 1
    var currentMaxColor = maxColor
    var remainingToBeColored = remaining //c'est ici le problème
    var rdd = adjMatrix

    loop

    def loop(): Unit = {
      while (true) {

        //The main exit condition. We have colored everything.
        if (remainingToBeColored <= 0) return

        // println(s"Iteration $iterationCounter ")
        //Broadcast the list of colors we need from the driver program
        val colors_bcast = sc.broadcast(colors)

        //From an adjmatrix RDD, produce a RDD of colors
        //We can either become a color, or stay the same.
        var colorsRDD = rdd.flatMap(elem => {

          val thisId = elem._1.toInt
          val adjlist = elem._2
          val colors = colors_bcast.value
          var betterPeasant = false
          var c = 0 //the color we find for this guy

          val neighborcolors = new Array[Int](currentMaxColor + 2)

          //First, check if the node is a peasant. If it is, we can continue working
          //Otherwise, if the node is a knight (the node has a color), we stop working right away.
          if (colors(thisId) == 0) { //this node is a peasant

            //First answer the following question : Can we become a knight in this iteration?
            //For this, we have to have the best tiebreaker. To check for this, we look at our adjlist.
            //If we find an earlier peasant that we are connected to, then we have to wait. If it's a knight and not a peasant, we are fine.
            //To check for peasants, we go through our adjlist from a certain i to a certain j

            //Batch iteration through the neighbors of this guy
            val buffer = new Array[Int](256)
            val it = adjlist.getBatchIterator

            loop //enter loop
            def loop(): Unit = {
              while (it.hasNext) {
                val batch = it.nextBatch(buffer)
                for (i <- 0 until batch) {
                  val neighbor = buffer(i)
                  val neighborColor = colors(neighbor)
                  neighborcolors(neighborColor) = 1
                  if (neighborColor == 0) {
                    betterPeasant = true
                    return
                  }
                }
              }
            } //fin loop

            //We can become a knight here
            if (betterPeasant == false) {
              c = color(neighborcolors)
            }
          } //fin du if this node is a peasant

          //Return the id and the color. If no color was found return None?
          if (c != 0) {
            Some(thisId, c)
          }
          else None
        }) //fin du map

        //Unpersist the colors broadcast data structrure
        colors_bcast.unpersist(false)

        //Every vertex we have colored, we mark it in the colors data structure.
        //We also filter out the vertices that don't improve to a knight
        //val results = colorsRDD.collect().map(e => e.get)
        val results = colorsRDD.collect()

        //Update the colors structure, and update the maximum color at the same time
        results.foreach(elem => {
          colors(elem._1) = elem._2
          remainingToBeColored -= 1
          if (elem._2 > currentMaxColor) currentMaxColor = elem._2
        })

        iterationCounter += 1

      } //while true loop here
    } //def loop here

    iterationCounter -= 1
    //println(s"colored the chunk in $iterationCounter iterations")

    //Return the iteration counter and the maxCounter
    (iterationCounter, currentMaxColor)
  }
}

object testFCKryro extends App {

  import gen.verifyTestSuite

  val conf = new SparkConf().setMaster("local[*]").setAppName("Roaring graph coloring").set("spark.driver.maxResultSize", "0")
  //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //Setting up to use Kryo serializer
  conf.set("spark.kryo.registrator", "com.acme.MyRegistrator")
  conf.set("spark.kryo.registrationRequired", "true")

  conf.set("spark.driver.maxResultSize", "0")
  conf.set("spark.kryo.unsafe", "true") //default false

  conf.set("spark.memory.offHeap.enabled", "true") //default false
  conf.set("spark.memory.offHeap.size", "10g") //default false
  conf.set("spark.broadcast.compress", "false")
  conf.set("spark.checkpoint.compress", "true")


  //.set("spark.checkpoint.compress", "true")
  val sc = new SparkContext(conf)
  sc.setLogLevel("OFF")

  var n = 8
  var t = 7
  var v = 4

  import cmdlineparser.TSPARK.compressRuns
  compressRuns = false
  val tests = distributed_fastcoloring(n, t, v, sc, 20000, "OC") //4000 pour 100 2 2

  println("We have " + tests.size + " tests")
  println("Printing the tests....")
  tests foreach (utils.print_helper(_))

  println("\n\nVerifying test suite ... ")
  println(verifyTestSuite(tests, fastGenCombos(n, t, v, sc), sc))

}

