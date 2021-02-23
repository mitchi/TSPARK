package withoutSpark

import central.gen.filename
import cmdlineparser.TSPARK.compressRuns
import com.acme.BitSet
import enumerator.distributed_enumerator.fastGenCombos
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.roaringbitmap.RoaringBitmap
import progressivecoloring.progressive_coloring.assign_numberClauses
import utils.utils

import java.util.concurrent.{Executors, TimeUnit}
import scala.util.Random

/**
  * Implémentation du Graph Coloring avec OX+Bitsets, et OrderColoring parallélisé
  */

object NoSparkParallelv1 extends Serializable {

  var debug = false
  /**
    *
    */
  def fastcoloring(combos: RDD[Array[Char]],
                   sc: SparkContext,
                   chunkSize: Int,
                   n: Int, v: Int,
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
    val combosNumbered = assign_numberClauses(mycombos, sc).collect().sortBy(_._2)
    println("Shuffling and numbering the clauses is complete")

    //We no longer need the not-numbered combos
    combos.unpersist(false)
    mycombos.unpersist(false)

    //Before everything, we can color the first vertex with the color 1
    var maxColor = 1
    colors(0) = 1
    var i = 1 //the number of vertices we have colored
    var chunkNo = 0

    var tableau = initTableau(n, v)
    var etoiles = initTableauEtoiles(n)

    //On ajoute l'info du premier combo
    val firstCombo = combosNumbered.flatMap(e => {
      if (e._2 == 0) Some(e)
      else None
    })

    utils.print_helper(firstCombo(0)._1)

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
      })

      val sizeOfChunk = someCombos.size
      println(s"Currently working with a chunk of the graph with $sizeOfChunk vertices.")

      //Calculate the biggest ID in the chunk
      val biggestID = i + sizeOfChunk - 1

      println("Adding the entries of the chunk to the tableau...")
      println("Growing the tables right now...")

      //On pourrait mesurer le temps ici

      val startTime = System.nanoTime()
      growTables(tableau, etoiles, biggestID, n, v)
      val afterGrow = System.nanoTime()
      tableau = addToTableau(tableau, someCombos, n, v)
      etoiles = addTableauEtoiles(etoiles, someCombos, n, v)
      val afterAddToTableaux = System.nanoTime()

      val growTablesTime = (afterGrow - startTime).toDouble / 1000000000
      val addTableauxTime = (afterAddToTableaux - afterGrow).toDouble / 1000000000

      println(s"Grow tables : $growTablesTime seconds")
      println(s"Add tableaux time : $addTableauxTime seconds")


      if (debug == true) {
        println("On print les combos du chunk")
        for (c <- someCombos) {
          print(c._2 + " "); utils.print_helper(c._1)
        }
      }

      if (debug == true) {
        println("On imprime le tableau apres remplissage")
        for (i <- 0 until n) { //pour tous les paramètres
          for (j <- 0 until v) { //pour toutes les valeurs
            println(s"p$i=$j " + tableau(i)(j).toString)
          }
        }

        println("On imprime le tableau etoiles")
        var tt = 0
        for (elem <- etoiles) {
          println(s"p$tt=*" + " " + elem.toString)
          tt += 1
        }
      }

      val r1 = generateadjlist_fastcoloringFutures(i, sizeOfChunk, combosNumbered, tableau, etoiles, sc)

      if (debug == true) println("Debug de l'adj list")
      if (debug == true) r1.foreach(e => println(s"${e._1} ${e._2.toString}"))

      val r2 = if (algorithm == "KP") {
        println("Using the Knights & Peasants algorithm to color the graph...")
        //progressiveKP(colors, sc.makeRDD(r1.toArray), r1.size, maxColor, sc)
        (1, 1)
      }
      else {
        println("Using the Order Coloring algorithm to color the graph...")
        val t1 = System.nanoTime()
        val a = ordercoloring(colors, r1.toArray, i, maxColor)
        val t2 = System.nanoTime()
        val time_elapsed = (t2 - t1).toDouble / 1000000000
        println(s"Time elapsed for Order Coloring: $time_elapsed seconds")
        a
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
      i += sizeOfChunk //1 + 10000 = 10001
      println(s"i is now $i")

      loop
    }

    val percent = ((totalIterations.toDouble / count.toDouble) * 100)
    val vertexPerIteration = count / totalIterations
    println(s"We did a total of $totalIterations iterations, which is $percent% of total")
    println(s"We also colored $vertexPerIteration vertices per iteration on average")

    //Create tests now
    val properFormRDD = combosNumbered.map(elem => {
      val id = elem._2
      val color = colors(id.toInt)
      (color, elem._1)
    })

    maxColor
  }


  /**
    * Generate the list of impossible combos by "investing" the list of possible combos,
    * and also using the list of combos with stars
    *
    * On génère la liste des combos qui seront adjacents dans le graphe, en prenant la liste des étoiles, et la liste des
    * possiblement valides.
    *
    * @param id
    * @param list
    * @param etoiles
    * @return
    */
  def generateOtherList(id: Long,
                        list: BitSet,
                        etoiles: BitSet) = {

    // if (debug == true) println(s"L: $list")
    // if (debug == true) println(s"E: $etoiles")
    val possiblyValidGuys = list | etoiles

    //if (debug == true) println(s"|: $possiblyValidGuys")
    possiblyValidGuys.xor1()

    //if (debug == true)println(s"^: $possiblyValidGuys")
    possiblyValidGuys
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
                 tableau: Array[Array[BitSet]],
                 etoiles: Array[BitSet]) = {

    var i = 0 //quel paramètre?
    var certifiedInvalidGuys = new BitSet(id.toInt)

    if (debug == true) println("Combo is " + utils.print_helper(combo) + s" id is $id")

    //On crée le set des validguys a partir de notre tableau rempli
    for (it <- combo) {

      //      println(s"i=$i, value is $it")
      //       if (it == '*') println("*, we skip")

      if (it != '*') {
        val paramVal = it - '0'
        val list = tableau(i)(paramVal) //on prend tous les combos qui ont cette valeur. (Liste complète)
        val listEtoiles = etoiles(i) //on va prendre tous les combos qui ont des etoiles pour ce parametre (Liste complète)
        val invalids = generateOtherList(id, list, listEtoiles)

        //On ajoute dans la grosse liste des invalides
        certifiedInvalidGuys = certifiedInvalidGuys | (invalids)


      }
      //On va chercher la liste des combos qui ont ce paramètre-valeur
      i += 1
    }

    //On retourne cette liste, qui contient au maximum chunkSize éléments
    //Il faut ajuster la valeur des éléments de cette liste pour les id du chunk
    //println(s"F: $certifiedInvalidGuys ")
    certifiedInvalidGuys

  }


  /**
    * Input: a Bitset, output: A Roaring Bitmap
    *
    * @param bitset
    */
  def bitSetToRoaringBitmap(bitset: BitSet) = {
    val r = new RoaringBitmap()
    //r.addN()
    for (elem <- bitset.iterator) {
      r.add(elem)
    }
    r
  }

  /**
    * Input: RDD de combos, tableau, etoiles. Output: un RDD de (id, adjlist) pour colorier le graphew
    * Cette version n'utilise pas Apache Spark
    *
    * @param i
    * @param step
    * @param combos
    * @param tableau
    * @param etoiles
    * @param sc
    * @return
    */
  def generateadjlist_fastcoloringFutures(i: Long, step: Long,
                                          combos: Array[(Array[Char], Long)],
                                          tableau: Array[Array[BitSet]],
                                          etoiles: Array[BitSet],
                                          sc: SparkContext) = {


    println("Generating the adjacency lists using the fast graph construction algorithm...")
    println("Here, we are using Scala Futures")
    println("Run compression : " + compressRuns)

    val t1 = System.nanoTime()

    var totalTimeConvert = 0.0

    val r1 = combos.par.flatMap(combo => {
      val id = combo._2
      if (id != 0 && id < i + step && id >= i) { //Discard first combo, it is already colored. We don't need to compute its adjlist
        //Pour chaque combo du RDD, on va aller chercher la liste de tous les combos dans le chunk qui sont OK
        val adj = comboToADJ(id, combo._1, tableau, etoiles)

        //La liste est de taille minimum, 64 bits
        Some(combo._2, adj)
      }
      else {
        //println("Not calculing adjlist for first combo")
        None
      }
    })

    val t2 = System.nanoTime()
    val time_elapsed = (t2 - t1).toDouble / 1000000000
    val timeToConvertPrint = totalTimeConvert.toDouble / 1000000000
    println(s"Time to create all adj lists: $time_elapsed seconds")
    println(s"Total time to convert: $timeToConvertPrint seconds")

    r1
  }


  /**
    * Input: RDD de combos, tableau, etoiles. Output: un RDD de (id, adjlist) pour colorier le graphew
    * Cette version n'utilise pas Apache Spark
    *
    * @param i
    * @param step
    * @param combos
    * @param tableau
    * @param etoiles
    * @param sc
    * @return
    */
  def generateadjlist_fastcoloringST(i: Long, step: Long,
                                     combos: Array[(Array[Char], Long)],
                                     tableau: Array[Array[BitSet]],
                                     etoiles: Array[BitSet],
                                     sc: SparkContext) = {

    println("Generating the adjacency lists using the fast graph construction algorithm...")

    val t1 = System.nanoTime()

    val r1 = combos.flatMap(combo => {
      val id = combo._2
      if (id != 0 && id < i + step && id >= i) { //Discard first combo, it is already colored. We don't need to compute its adjlist
        //Pour chaque combo du RDD, on va aller chercher la liste de tous les combos dans le chunk qui sont OK
        val adjlist = comboToADJ(id, combo._1, tableau, etoiles)
        //La liste est de taille minimum, 64 bits
        Some(combo._2, adjlist)
      }
      else {
        //println("Not calculing adjlist for first combo")
        None
      }
    })
    val t2 = System.nanoTime()
    val time_elapsed = (t2 - t1).toDouble / 1000000000
    println(s"Time to create all adj lists: $time_elapsed seconds")
    r1
  }

  /**
    * On crée le tableau qu'on va utiliser.
    * On skip les * lorsqu'on remplit ce tableau avec les valeurs
    * Ce tableau se fait remplir avec chaque traitement de chunk.
    *
    * */
  def initTableau(n: Int, v: Int) = {
    var tableau = new Array[Array[BitSet]](n)

    //On met très exactement n paramètres, chacun avec v valeurs. On ne gère pas les *
    for (i <- 0 until n) {
      tableau(i) = new Array[BitSet](v)
      for (v <- 0 until v) {
        tableau(i)(v) = new BitSet(10)
      }
    }
    tableau
  }

  /**
    * On crée un tableau pour gérer seulement les étoiles
    * Roaring Bitmap dans le tableau
    */
  def initTableauEtoiles(n: Int) = {
    var tableauEtoiles = new Array[BitSet](n)

    for (i <- 0 until n) {
      tableauEtoiles(i) = new BitSet(10)
    }

    tableauEtoiles
  }


  /**
    * On recoit un tableau, et on ajoute l'information avec le chunk de combos
    * On ajoute sans cesse dans le tableau
    *
    * @param chunk
    * @param n
    * @param v
    */
  def addToTableau(tableau: Array[Array[BitSet]],
                   chunk: Array[(Array[Char], Long)], n: Int, v: Int) = {

    //On remplit cette structure avec notre chunk
    for (combo <- chunk) { //pour chaque combo
      for (i <- 0 until n) { //pour chaque paramètre
        val cc = combo._1(i)
        if (cc != '*') {
          val vv = combo._1(i) - '0' //on va chercher la valeur
          tableau(i)(vv) set combo._2.toInt //on ajoute dans le ArrayBuffer . On pourrait mettre l'index global aussi.mmm
        }
      }
    }

    //On retourne notre travail
    tableau
  }


  def addTableauEtoiles(etoiles: Array[BitSet],
                        chunk: Array[(Array[Char], Long)], n: Int, v: Int) = {

    //On remplit cette structure avec notre chunk
    for (combo <- chunk) { //pour chaque combo
      for (i <- 0 until n) { //pour chaque paramètre
        val cc = combo._1(i)
        if (cc == '*') {
          etoiles(i) set (combo._2.toInt) //on ajoute dans le ArrayBuffer . On pourrait mettre l'index global aussi.mmm
        }
      }
    }
    etoiles
  }

  /**
    * On fait grossir nos BitSets
    *
    * @param tableau
    * @param etoiles
    * @param biggestId
    * @param n
    * @param v
    */
  def growTables(tableau: Array[Array[BitSet]], etoiles: Array[BitSet],
                 biggestId: Int, n: Int, v: Int) = {


    //println("Growing the tables...")
    //println(s"Biggest id is $biggestId")


    //println("Growing table of stars...")
    //On fait les étoiles
    for (i <- 0 until n) {
      // println(s"Growing parameter=$i")
      // println("B: "+ etoiles(i))
      etoiles(i) = BitSet.expandBitSet(etoiles(i), biggestId)
      //  println("A: "+ etoiles(i))
    }

    // println("Growing table of values...")
    //On fait les valeurs
    for (i <- 0 until n) {
      // tableau(i) = new Array[BitSet](v)
      for (v <- 0 until v) {
        // println("B: "+ tableau(i)(v))
        tableau(i)(v) = BitSet.expandBitSet(tableau(i)(v), biggestId)
        //println("A: "+ tableau(i)(v))
      }
    }
  }

  /**
    * On donne tous les paramètres nécessaires au bon fonctionnement de ce thread
    * @param p
    * @param neighborcolors
    * @param colors
    */
  class Handler(p: Int, neighborcolors : BitSet, colors: Array[Int]) extends Runnable {

    def run(): Unit = {

    }
  }

  /**
    * Color N vertices at a time using the Order Coloring algorithm.
    * The adjvectors are precalculated
    *
    * @return
    */
  def ordercoloring(colors: Array[Int], adjMatrix: Array[(Long, BitSet)], i: Int,
                    maxColor: Int) = {

    val limit = adjMatrix.size //the number of iterations we do
    var j = 0 //start coloring using the first adjvector of the adjmatrix
    var currentMaxColor = maxColor

    println("On essaie le truc parallèle!")

    var list2 = (0 to 3)
    var tp = Executors.newFixedThreadPool(list2.size)

    loop

    def loop(): Unit = {

      //Only color n vertices in this loop
      if (j == limit) return

      val neighborcolors = new BitSet(currentMaxColor + 2)
      neighborcolors.setUntil(currentMaxColor + 2) //On remplit le bitset avec des 1

      list2.foreach( elem => {
        tp.submit( new Handler(elem, neighborcolors, colors) {
        })
      })

      tp.awaitTermination(1000, TimeUnit.MINUTES)

      //Batch iteration through the neighbors of this guy
      val id = adjMatrix(j)._1
      val bitset = adjMatrix(j)._2

//      var list = (0 to 3)
//      list.par.foreach( p => {
//        //Ok, on itere sur tous les bits. Il faut arrêter avant
//        loop2;
//        def loop2(): Unit = {
//            for (elem <- bitset.iterator2(p, list.size)) {
//              if (elem >= id) return
//              val neighbor = elem
//              val neighborColor = colors(neighbor)
//              neighborcolors.unset(neighborColor)
//            }
//        }
//      })

      val foundColor = colorBitSet(neighborcolors)
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
    * On itere sur le bitset des couleurs adjacentes. Dès qu'un bit du bitset est égal a zéro, on peut prendre cette couleur.
    *
    * @param vertices
    * @param vertex
    * @return THE COLOR THAT WE CAN TAKE
    */
  def colorBitSet(neighbor_colors: BitSet) = {
    val res = neighbor_colors.nextSetBit(1)
    res
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
  def progressiveKP(colors: Array[Int], adjMatrix: RDD[(Long, BitSet)], remaining: Int,
                    maxColor: Int, sc: SparkContext) = {


    var iterationCounter = 1
    var currentMaxColor = maxColor
    var remainingToBeColored = remaining
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
            //Ok, on itere sur tous les bits. Il faut arrêter avant
            loop3;
            def loop3(): Unit = {
              while (true) {
                for (elem <- adjlist.iterator) {
                  if (elem >= thisId) return
                  val neighbor = elem
                  val neighborColor = colors(neighbor)
                  neighborcolors(neighborColor) = 1
                  if (neighborColor == 0) {
                    betterPeasant = true
                    return
                  }
                }
              }
            }

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

  /**
    * The distributed graph coloring algorithm with the fast coloring construction algorithm
    *
    * @param n
    * @param t
    * @param v
    * @param sc
    * @return
    */
  def withoutSpark(n: Int, t: Int, v: Int, sc: SparkContext,
                   chunkSize: Int = 4000, algorithm: String = "OC"): Int = {
    val expected = utils.numberTWAYCombos(n, t, v)
    import cmdlineparser.TSPARK.compressRuns

    println("Distributed Graph Coloring with FastColoring algorithm, BitSets for graph construction")
    println(s"Run compression for Roaring Bitmap = $compressRuns")
    println(s"Using a chunk size = $chunkSize vertices and algorithm = $algorithm")
    println(s"Problem : n=$n,t=$t,v=$v")
    println(s"Expected number of combinations is : $expected ")
    println(s"Formula is C($n,$t) * $v^$t")

    import java.io._
    val pw = new PrintWriter(new FileOutputStream(filename, true))

    val t1 = System.nanoTime()

    val maxColor = fastcoloring(fastGenCombos(n, t, v, sc).cache(), sc, chunkSize, n, v, algorithm)

    val t2 = System.nanoTime()
    val time_elapsed = (t2 - t1).toDouble / 1000000000

    pw.append(s"$t;$n;$v;WITHOUTSPARK;algorithm=$algorithm;$time_elapsed;$maxColor\n")
    println(s"$t;$n;$v;WITHOUTSPARK;algorithm=$algorithm;$time_elapsed;$maxColor\n")
    pw.flush()

    //Return the test suite
    maxColor
  }
}

object testNoSparkParallelv1 extends App {

  import NoSparkParallelv1.withoutSpark

  val conf = new SparkConf().setMaster("local[*]").
    setAppName("Without Spark Parallel BitSet Iteration")
    .set("spark.driver.maxResultSize", "10g")
  val sc = new SparkContext(conf)
  sc.setLogLevel("OFF")
  println(s"Printing sc.appname : ${sc.appName}")
  println(s"Printing default partitions : ${sc.defaultMinPartitions}")
  println(s"Printing sc.master : ${sc.master}")
  println(s"Printing sc.sparkUser : ${sc.sparkUser}")
  println(s"Printing sc.resources : ${sc.resources}")
  println(s"Printing sc.deploymode : ${sc.deployMode}")
  println(s"Printing sc.defaultParallelism : ${sc.defaultParallelism}")
  println(s"Printing spark.driver.maxResultSize : ${sc.getConf.getOption("spark.driver.maxResultSize")}")
  println(s"Printing spark.driver.memory : ${sc.getConf.getOption("spark.driver.memory")}")
  println(s"Printing spark.executor.memory : ${sc.getConf.getOption("spark.executor.memory")}")
  println(s"Printing spark.serializer : ${sc.getConf.getOption("spark.serializer")}")
  println(s"Printing sc.conf : ${sc.getConf}")
  println(s"Printing boolean sc.islocal : ${sc.isLocal}")
  println("Without Spark Parallel BitSet Iteration")

  var n = 8
  var t = 7
  var v = 4

  import cmdlineparser.TSPARK.compressRuns

  compressRuns = false
  val maxColor = withoutSpark(n, t, v, sc, 100000, "OC") //4000 pour 100 2 2
  println("We have " + maxColor + " tests")

}