package roaringcoloring

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.roaringbitmap.{RoaringArray, RoaringBitmap}
import utils.utils
import progressivecoloring.progressive_coloring.assign_numberClauses
import progressivecoloring.progressive_coloring.determineStep

import scala.util.Random
import ordercoloring.OrderColoring.isAdjacent
import ordercoloring.OrderColoring.coloringToTests

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Set

/**
  * Bon lien:
  * https://richardstartin.github.io/posts/roaringbitmap-performance-tricks#batch-iteration
  *
  */
object roaring_coloring extends Serializable {

  var debug = false


  /**
     On recoit un tableau, et on ajoute l'information avec le chunk de combos
     On ajoute sans cesse dans le tableau
    * @param chunk
    * @param n
    * @param v
    */
  def addToTableau(tableau: Array[Array[ArrayBuffer[Int]]],
                   chunk: Array[(Array[Char], Long)], n : Int, v : Int) = {
    //On pourrait également faire Array[Array[Int]] et faire un indexage plus compliqué

    println("On print les combos du chunk")
    for (c <- chunk) {
      print(c._2 + " ") ; utils.print_helper(c._1)
    }


    var indexCombo = 0
    //On remplit cette structure avec notre chunk
    for (combo <- chunk) { //pour chaque combo
      for (i <- 0 until n) { //pour chaque paramètre
        val cc = combo._1(i)
        if (cc != '*') {
          val vv = combo._1(i) - '0' //on va chercher la valeur
          tableau(i)(vv) += combo._2.toInt //on ajoute dans le ArrayBuffer . On pourrait mettre l'index global aussi.mmm
          //tableau(i)(vv) += indexCombo
        }
      }
      indexCombo +=1
    }

    //On retourne notre travail
    tableau
  }

  /**
    * On crée le tableau qu'on va utiliser.
    * On skip les * lorsqu'on remplit ce tableau avec les valeurs
    *
    * Ce tableau se fait remplir avec chaque traitement de chunk.
    *
    *
    *  */
  def initTableau(n: Int, v : Int) = {
    var tableau = new Array[Array[ArrayBuffer[Int]]](n)

    //On met très exactement n paramètres, chacun avec v valeurs. On ne gère pas les *
    for (i <- 0 until n ) {
      tableau(i) = new Array[ ArrayBuffer[Int]](v)
      for (v <- 0 until v) {
        tableau(i)(v) = new ArrayBuffer[Int]()
      }
    }
    tableau
  }

  /**
    * On crée un tableau pour gérer seulement les étoiles
   */
  def initTableauEtoiles(n: Int) = {
    var tableauEtoiles = new Array[ArrayBuffer[Int]](n)

    for (i <- 0 until n) {
      tableauEtoiles(i) = new ArrayBuffer[Int]()
    }

    tableauEtoiles
  }

  def addTableauEtoiles(etoiles: Array[ArrayBuffer[Int]],
                        chunk: Array[(Array[Char], Long)], n : Int, v : Int ) = {

    //On remplit cette structure avec notre chunk
    for (combo <- chunk) { //pour chaque combo
      for (i <- 0 until n) { //pour chaque paramètre
        val cc = combo._1(i)
        if (cc == '*') {
          etoiles(i) += combo._2.toInt //on ajoute dans le ArrayBuffer . On pourrait mettre l'index global aussi.mmm
        }
      }
    }
    etoiles
  }

  /**
    * Same function as coloring roaring but with a faster graph construction algorithm
    * Works with a fixed v and n for now
    *
    * @param combos the RDD of combos
    * @param sc     SparkContext
    * @param step   the number of vertices to move at the same time. Memory should increase with each iteration
    * @return
    */
  def fastcoloring_roaring(combos: RDD[Array[Char]],
                           sc: SparkContext,
                           step: Int,
                           n: Int, v : Int,
                           algorithm: String = "OrderColoring") = {
    //First, color 100 000 elements. If the graph is colored, we return. Else we continue here.
    val count = combos.count()
    val colors = new Array[Int](count.toInt) //colors for every vertex, starting at 0. We pass this data structure around and modify it

    //Save the seed so that Spark can regenerate the combos on demand. This would save memory but add CPU time
    //checkpoint to HDFS or on a ssd drive would also work
    //val seed = System.nanoTime()
    val seed = 20

    var totalIterations = 0

    //Shuffle the combos before. Doing this ensures a different result every run.
    var mycombos = combos.mapPartitions(it => {
      Random.setSeed(seed)
      Random.shuffle(it)
    }, true)

    val combosNumbered: RDD[(Array[Char], Long)] = assign_numberClauses(mycombos, sc).cache()
    println("Shuffling and numbering the clauses is complete")

    //We no longer need combos
    combos.unpersist(false)

    //Before everything, we can color the first vertex with the color 1
    colors(0) = 1
    var maxColor = 1
    var i = 1 //the number of vertices we have colored

    //var step = 0 //calculated value
    var chunkNo = 0

    var tableau = initTableau(n,v)
    var etoiles: Array[ArrayBuffer[Int]] = initTableauEtoiles(n)


    loop

    def loop(): Unit = {
      chunkNo += 1
      println(s"Now processing Chunk $chunkNo of the graph...")
      System.gc() //calling garbage collection here

      if (i >= count) return //if we have colored everything, we are good to go

      //Filter the combos we color in the next OrderColoring iteration
      val someCombos: Array[(Array[Char], Long)] = combosNumbered.flatMap(elem => {
        if (elem._2 < i + step && elem._2 >= i) {
          Some(elem)
        }
        else None
      }).collect().sortBy(_._2)

      val sizeOfChunk = someCombos.size
      println(s"Currently working with a chunk of the graph with $sizeOfChunk vertices.")

      //val filteredCombos = filterBig(combosNumbered, i, sizeOfChunk)

      tableau = addToTableau(tableau, someCombos, n, v)
      etoiles = addTableauEtoiles(etoiles, someCombos, n, v)


      println("On imprime le tableau apres remplissage")
      for (i <- 0 until n){ //pour tous les paramètres
        for (j <- 0 until v) { //pour toutes les valeurs
          println(s"$i $j "+ tableau(i)(j).toString)
        }
      }

      val r1 = generatelist_fastcoloring(i, sizeOfChunk, combosNumbered, tableau, etoiles, sc).cache()

      //On debug ce qu'on a
      println("Debug de l'adj list")
      r1.collect().foreach(  e => println(s"${e._1} ${e._2.toString}"))

      //Use KP when the graph is sparse (not dense)
      val r2 =
        if (algorithm == "KP") {
          roaringkp.roaringkp.progressiveKP(colors, r1, r1.count().toInt, maxColor, sc)
        }
        else { //algorithm = "OC". The single threaded needs a sorted matrix of adjlists
          ordercoloring(colors, r1.collect().sortBy(_._1), i, maxColor)
        }

      //Update the max color
      totalIterations += r2._1
      maxColor = r2._2
      r1.unpersist(true)
      i += step

      loop
    }

    val percent = ((totalIterations.toDouble / count.toDouble) * 100)
    val vertexPerIteration = count / totalIterations
    println(s"We did a total of $totalIterations iterations, which is $percent% of total")
    println(s"We also colored $vertexPerIteration vertices per iteration on average")

    //Create tests now
    val bcastcolors = sc.broadcast(colors)
    val properFormRDD: RDD[(Int, Array[Char])] = combosNumbered.map(elem => {
      val id = elem._2
      val color = bcastcolors.value(id.toInt)
      (color, elem._1)
    })

    //Transform into tests
    coloringToTests(properFormRDD)
  }


  /**
    * A partir d'une liste avec les numéros des combos qui possèdent le bon paramètre-valeur,
    * on crée la liste de ceux qui ne l'ont pas
    * La liste est triée
    *
    * On pourrait faire autre algorithme plus optimisé. Je pense que ça c'est correct. Le code est plus simple
    *
    * Attention, il faut utiliser la bonne valeur du chunkSize.
    *
    * @param list
    */
  def generateOtherList( id: Long,
                         list : ArrayBuffer[Int],
                         etoiles: ArrayBuffer[Int]) =
  {
    //On construit une lookup table avec la liste des bons combos
    //La liste est exactement de la taille du id
    var lookuptable = new Array[Byte](id.toInt)
    lookuptable = lookuptable.map( e => 0.toByte)

    //On a notre liste de combos qui sont possiblement valides sur ce parametre seulement
    for (i <- list) {
        if (i < id)
        lookuptable(i) = 1
    }

    //On génère maintenant la liste finale
    //Important: On tient compte des combos qui ont une étoile a ce paramètre, pour ne pas les disqualifier

    var adjlist = mutable.Set[Int]()
    var index = 0
    for (i <- 0 until id.toInt) {
      if (lookuptable(i) == 0) {
       adjlist += i
      }
    }

    //On fait la différence avec le set des étoiles
    etoiles.foreach( e => {
      adjlist.remove(e)
    })

    println("List of combos with valid params " + list.toString)
    println("List of combos with stars " + etoiles.toString)
    println("the certified invalid combos "+ adjlist.toString)

    adjlist
  }


  //def genadjlist_roaring(i: Long, step: Long, combos: RDD[(Array[Char], Long)],
  //combosToColor: Array[(Array[Char], Long)], sc: SparkContext) = {

  /**
    * On passe sur tous les paramètres du combo, et a chaque fois on va prendre la liste des combos du chunk qui possèdent ce paramètre.
    * À l'aide ça, on peut construire la liste des combos qui n'ont pas ce paramètre
    *
    * On peut faire plus optimisé avec plus de travail!
    * Est-ce que Roaring Bitmap est utile ici?
    * @param combo
    * @param tableau
    */
  def comboToADJ(id: Long,
                 combo: Array[Char],
                 tableau: Array[Array[ArrayBuffer[Int]]],
                 etoiles: Array[ArrayBuffer[Int]]) = {

    var i = 0 //quel paramètre?
    var possiblyValidGuys = scala.collection.mutable.Set[Int]()
    var certifiedInvalidGuys = scala.collection.mutable.Set[Int]()

    println("Combo is " + utils.print_helper(combo) + s" id is $id")

    //On crée le set des validguys a partir de notre tableau rempli
    for (it <- combo) {

      println(s"i=$i, value is $it")
      if (it == '*') println("*, we skip")

      if (it != '*') {
        val paramVal = it - '0'
        val list = tableau(i)(paramVal)
        val listEtoiles = etoiles(i) //on va prendre toutes les etoiles pour ce parametre

        val invalids = generateOtherList(id, list, listEtoiles)

        //On ajoute juste dans la liste si l'element est plus petit
        for (element <- list) {
          if (element < id) possiblyValidGuys += element
        }

        certifiedInvalidGuys ++= invalids

      }
      //On va chercher la liste des combos qui ont ce paramètre-valeur
      i+=1
    }

    //On filtre les combos dont le id est plus gros que le notre
    //Car notre liste d'adjacence ne connecte pas a ceux la, de toute façon
    //val list2 = generateOtherList(id, list, chunkSize.toInt )
    //badguys  ++= list2

    //println(s"\n\nid: $id")
   // println(s"combo: "+ utils.print_helper(combo))
    //println(s"list of all possibly valid guys" + possiblyValidGuys.toString)
    println(s"list of all certified invalid guys" + certifiedInvalidGuys.toString)
    println("\n\n")

    //On retourne cette liste, qui contient au maximum chunkSize éléments
    //Il faut ajuster la valeur des éléments de cette liste pour les id du chunk
    certifiedInvalidGuys

  }




  /**
    * Faster graph construction routine
    *
    * For every combo in the RDD, we do the following:
    * 1. We iterate over all its parameter-values. On trouve la liste des combos qui ont ce paramètre-valeur.
    * 2. A partir de cette liste, on génère la liste de tout ceux qui n'ont pas le parameter-value
    * 3. On utilise cette liste pour remplir la table de hachage -> RoaringBitmap.
    * 4. On fusionne nos listes, on retourne le graph partiel pour le chunk qu'on nous a donné
    * 5. Il faut gérer les indices aussi avec i et step également. Ne pas oublier.
    *
    * WIP
    *
    * @param i
    * @param step
    * @param combos
    * @param sc
    */
  def generatelist_fastcoloring(i : Long, step: Long,
                                combos: RDD[(Array[Char], Long)],
                                tableau: Array[Array[ArrayBuffer[Int]]],
                                etoiles: Array[ArrayBuffer[Int]],
                                sc : SparkContext) =
  {

    //First step is to group every combo with every other combo to do a MapReduce.
    //val bcasttableau = sc.broadcast(tableau)
    //val bcastetoiles = sc.broadcast(etoiles)

    //Print the number of partitions we are using
    val partitions = combos.getNumPartitions
    println(s"Currently using $partitions partitions")

    val r1 = combos.flatMap( combo => {
      //le id du combat. On n'a pas besoin de mettre des sommets plus gros que lui dans sa liste d'adjacence
      val id = combo._2
      if (id != 0) {
        //Pour chaque combo du RDD, on va aller chercher la liste de tous les combos dans le chunk qui sont OK
        val adjlist = comboToADJ(id, combo._1, tableau, etoiles)
        //On fait une Roaring Bitmap avec tout ça
        val roaring = new RoaringBitmap()
        adjlist.map( e => roaring.add(e)) //on ajoute l'index ici pour que ça soit correct
        Some(combo._2, roaring)
      }
      else None
      })



    r1
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
    * Used to filter out combos or clauses
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
    * Final version of the progressive coloring algorithm
    * We could add an initial coloring with OrderColoring for 100 000 vertices.
    *
    * @param combos the RDD of combos
    * @param sc     SparkContext
    * @param step   the number of vertices to move at the same time. Memory should increase with each iteration
    * @return
    */
  def coloring_roaring(combos: RDD[Array[Char]], sc: SparkContext, step: Int, algorithm: String = "OrderColoring") = {
    //First, color 100 000 elements. If the graph is colored, we return. Else we continue here.
    val count = combos.count()
    val colors = new Array[Int](count.toInt) //colors for every vertex, starting at 0. We pass this data structure around and modify it

    //Save the seed so that Spark can regenerate the combos on demand. This would save memory but add CPU time
    //checkpoint to HDFS or on a ssd drive would also work
    val seed = System.nanoTime()

    var totalIterations = 0

    //Print the combos 1 time
    if (debug == true) {
      println("Printing combos before shuffle")
      combos.collect().foreach(utils.print_helper(_))
    }
    //Shuffle the combos before. Doing this ensures a different result every run.
    var mycombos = combos.mapPartitions(it => {

      if (debug == true) {
        Random.setSeed(1)
      }
      else Random.setSeed(seed)

      Random.shuffle(it)
    }, true)

    //Print the combos 1 time
    if (debug == true) {
      println("Printing combos after shuffle")
      mycombos.collect().foreach(utils.print_helper(_))
    }

    //TODO : Color 100 000 vertex first before doing the rest.
    //val combosNumbered = mycombos.zipWithIndex().cache()
    val combosNumbered: RDD[(Array[Char], Long)] = assign_numberClauses(mycombos, sc).cache()

    println("Shuffling and numbering the clauses is complete")

    //We no longer need combos
    combos.unpersist(false)

    //Print the combos 1 time
    if (debug == true) {
      println("Printing combos after numbering")

      combosNumbered.mapPartitionsWithIndex((index, iterator) => {
        var stringPartition = ""
        stringPartition += "Partition " + index + "\n"
        iterator.foreach(e => {
          stringPartition += e._2 + " " + utils.print_helper2(e._1) + "\n"
        })
        Iterator(stringPartition)
      }).collect().foreach(println)
      // combosNumbered.collect().foreach(e => print(e._2 + " ") + utils.print_helper(e._1))
    }


    //Before everything, we can color the first vertex with the color 1
    colors(0) = 1
    var maxColor = 1
    var i = 1 //the number of vertices we have colored

    //var step = 0 //calculated value
    var chunkNo = 0

    loop

    def loop(): Unit = {
      chunkNo += 1
      println(s"Now processing Chunk $chunkNo of the graph...")
      System.gc() //calling garbage collection here

      if (i >= count) return //if we have colored everything, we are good to go

      //Calculate the step using our current position and the amount of available memory.
      //step = determineStep(memory, i) //petit bug, grands nombres?
      //if (debug == true) step = 6

      println(s"Currently working with a chunk of the graph with $step vertices.")

      //Filter the combos we color in the next OrderColoring iteration
      val someCombos = combosNumbered.flatMap(elem => {
        if (elem._2 < i + step && elem._2 >= i) {
          Some(elem)
        }
        else None
      }).collect().sortBy(_._2)

      //Print the combos now that they are shuffled (Debug mode)
      if (debug == true) {
        someCombos.foreach(e => {
          print(e._2 + " ")
          utils.print_helper(e._1)
        })
      }

      //Generate the adjlists for every combo in that list

      // val r1 = genadjlist_roaring(i, step, combosNumbered, someCombos, sc).cache()
      val filteredCombos = filterBig(combosNumbered, i, step)
      //filteredCombos.localCheckpoint()

      val r1 = genadjlist_roaring(i, step, filteredCombos, someCombos, sc).cache()


      //Print the types of the roaring bitmaps
      if (debug == true) {
        import org.roaringbitmap.insights.BitmapAnalyser._
        r1.collect().foreach(elem => {
          val rr = analyse(elem._2)
          println(rr.toString)
        })
      }

      if (debug == true) {
        println("\n\n")
        var printed = r1.collect().sortBy(_._1)
        //Return the sorted adjacency list, to be used by the order colouring algorithm.
        println(s"Printing the adjmatrix for $i")
        printed.foreach(e => {
          print(e._1 + " ")

          print(e._2.toString()) //printing the roaring matrix
          // e._2.foreach(b => print(b))
          print("\n")
        })
      }

      //Use KP when the graph is sparse (not dense)
      val r2 =
        if (algorithm == "KP") {
          roaringkp.roaringkp.progressiveKP(colors, r1, r1.count().toInt, maxColor, sc)
        }
        else { //algorithm = "OC". The single threaded needs a sorted matrix of adjlists
          ordercoloring(colors, r1.collect().sortBy(_._1), i, maxColor)
        }

      //Update the max color
      totalIterations += r2._1
      maxColor = r2._2
      r1.unpersist(true)
      i += step

      loop
    }

    val percent = ((totalIterations.toDouble / count.toDouble) * 100)
    val vertexPerIteration = count / totalIterations
    println(s"We did a total of $totalIterations iterations, which is $percent% of total")
    println(s"We also colored $vertexPerIteration vertices per iteration on average")

    //Create tests now
    val bcastcolors = sc.broadcast(colors)
    val properFormRDD: RDD[(Int, Array[Char])] = combosNumbered.map(elem => {
      val id = elem._2
      val color = bcastcolors.value(id.toInt)
      (color, elem._1)
    })

    //Transform into tests
    coloringToTests(properFormRDD)
  }

  /**
    * Alternative implementation using Roaring bitmaps
    *
    */

  def genadjlist_roaring(i: Long, step: Long, combos: RDD[(Array[Char], Long)],
                         combosToColor: Array[(Array[Char], Long)], sc: SparkContext) = {

    //First step is to group every combo with every other combo to do a MapReduce.
    val bcastdata = sc.broadcast(combosToColor)
    val chunkSize = combosToColor.size

    //Print the number of partitions we are using
    val partitions = combos.getNumPartitions
    println(s"Currently using $partitions partitions")

    //For every partition, we build a hash table of COMBO -> List of neighbors (all neighbors are strictly less than combo)
    val r1 = combos.mapPartitions(partition => {
      val hashtable = scala.collection.mutable.HashMap.empty[Long, RoaringBitmap]

      partition.foreach(elem => {
        val thisId = elem._2

        // Si le ID du combo est plus haut que ceux des combos choisis, on fait aucun travail.
        //println(s" haut if $thisId > $i + $step ")
        if (thisId > i + step) {
          val bbb = 2 //dummy statement for Scala
        }
        else {

          val someCombos = bcastdata.value

          var j = if (thisId >= i)
            (thisId - i).toInt //start the counter at 0
          else 0
          loop

          def loop(): Unit = {

            //Basic exit condition
            if (j == someCombos.size) return

            //If id of this general combo is lower than the combo to color, we can work
            if (thisId < i + j) {

              //Check if lookuptable exists. Create it if it does not
              if (!hashtable.contains(i + j)) {
                hashtable(i + j) = new RoaringBitmap()
              }

              val answer = isAdjacent(elem._1, someCombos(j)._1)
              if (answer == true)
                hashtable(i + j).add(thisId.toInt) //add value to roaring bitmap
              // hashtable(i + j)(thisId.toInt) = 1
              //else hashtable(i + j)(thisId.toInt) = 0
            }

            j += 1
            loop
          }
        }
      })

      //New ArrayBufer with results
      // val rr = new ArrayBuffer[(Long, RoaringBitmap)]()
      var rr2 = hashtable.toIterator
      rr2
      //Iterator(hashtable.toArray)
      //Return an iterator here
    })


    // Debug mode. We print the adjacency lists
    if (debug == true) {
      val d1 = r1.mapPartitionsWithIndex((partition, it) => {

        var output = ""
        output += "Partition " + partition + "\n"
        it.foreach(e => {
          output += e._1 + " "
          // e._2.foreach(b => output += b)
          output += e._2.toString

          output += "\n"
        })

        Iterator(output)
      }).collect().foreach(println)
    }

    val r3 = r1.reduceByKey((a, b) => {
      //fuseadjlists(a, b)
      a.or(b)
      a
    })

    //Here we handle whether or not we use run compression or not
    import cmdlineparser.TSPARK.compressRuns

    val r4 = compressRuns match {
      case true =>
        println("Appyling the algorithm to compress into runs when its suitable")
        r3.mapValues(e => {
        e.runOptimize()
        e
      })
      case false =>
        r3
    }

//    val r4 = r3.mapValues(e => {
//      e.runOptimize()
//      e
//    })

    //Destroy the DAG here
    // r4.localCheckpoint()

    if (debug == true) {
      println("Fused adjlists")
      val d2 = r3.mapPartitionsWithIndex((partition, it) => {
        var output = ""
        output += "Partition " + partition + "\n"
        it.foreach(e => {
          output += e._1 + " "
          // e._2.foreach(b => output += b)
          output += e._2.toString
          output += "\n"
        })
        Iterator(output)
      }).collect().foreach(println)
    }
    r4
  }

} //object roaring coloring
