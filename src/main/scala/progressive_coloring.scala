package progressivecoloring

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.Random
import ordercoloring.OrderColoring.isAdjacent
import ordercoloring.OrderColoring.coloringToTests
import utils.utils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object progressive_coloring extends Serializable {

  var debug = true

  /**
    * Fold lookup table b inside lookup table a
    * This function mutates a
    *
    * @param a
    * @param b
    */
  def fuseadjlists(a: Array[Byte], b: Array[Byte]) = {
    for (i <- 0 until b.size) {
      if (b(i) == 1) a(i) = 1 //if b has 1, write it to A
    }
    a
  }

  /** Determines the speed at which graph covering can occur. In the beginning, it can go faster because adjlists are smaller
    * Autre option: Calculer la taille des chunks avec la fameuse formule n(n+1)/2 et retrancher la taille du dernier chunk
    *
    * @param availableMemory in megabytes
    * @param startingVertex  starting vertex
    * @param finalVertex     final vertex
    * @return
    */
  def determineStep(availableMemory: Int, startingVertex: Int): Int = {
    var thousand: BigInt = BigInt(1000)
    var c: BigInt = BigInt(availableMemory)
    c = c * thousand * thousand

    var i = 0
    var b = startingVertex

    while (c > 0) {
      c -= b
      b += 1
      i += 1
    }
    i - 1
  }

  /**
    * Here we send in a number of combos which are yet to be colored
    * We produce the adjacency lists directly on the cluster.
    */
  def genadjlist_lookuptable(i: Long, step: Long, combos: RDD[(Array[Char], Long)],
                             combosToColor: Array[(Array[Char], Long)], sc: SparkContext) = {

    //First step is to group every combo with every other combo to do a MapReduce.
    val bcastdata = sc.broadcast(combosToColor)

    //For every partition, we build a hash table of COMBO -> List of neighbors (all neighbors are strictly less than combo)
    val r1 = combos.mapPartitions(partition => {
      val hashtable = scala.collection.mutable.HashMap.empty[Long, Array[Byte]]

      partition.foreach(elem => {
        val thisId = elem._2
        val someCombos = bcastdata.value

        // Si le ID du combo est plus haut que ceux des combos choisis, on fait aucun travail.
        //println(s" haut if $thisId > $i + $step ")
        if (thisId > i + step) {
          val bbb = 2 //dummy statement for Scala
        }
        else {
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
                hashtable(i + j) = new Array[Byte]((i + j).toInt)
              }

              val answer = isAdjacent(elem._1, someCombos(j)._1)
              if (answer == true)
                hashtable(i + j)(thisId.toInt) = 1
              else hashtable(i + j)(thisId.toInt) = 0
            }

            j += 1
            loop
          }
        }
      })
      Iterator(hashtable)
      //Return an iterator here
    })

    //Fuse the sets
    val r2 = r1.fold(scala.collection.mutable.HashMap.empty[Long, Array[Byte]])(
      (acc, value) => {
        value.foreach(elem => { //for each element of the other hashtable
          val key = elem._1
          val value = elem._2
          if (acc.get(key).isEmpty) acc(key) = elem._2
          else acc(key) = fuseadjlists(acc(key), value)
        })
        acc
      }
    )

    val r3 = r2.toArray.sortBy(_._1)

    //Return the sorted adjacency list, to be used by the order colouring algorithm.
    //    println(s"Printing the adjmatrix for $i")
    //    r3.foreach( e => {
    //      print(e._1 + " ")
    //      e._2.foreach( b => print(b))
    //      print("\n")
    //    })

    r3
  }


  /**
    * Here we send in a number of combos which are yet to be colored
    * We produce the adjacency lists directly on the cluster.
    */
  def genadjlist_hashtableReduceByKey(i: Long, step: Long, combos: RDD[(Array[Char], Long)],
                                      combosToColor: Array[(Array[Char], Long)], sc: SparkContext) = {

    //First step is to group every combo with every other combo to do a MapReduce.
    val bcastdata = sc.broadcast(combosToColor)

    //For every partition, we build a hash table of COMBO -> List of neighbors (all neighbors are strictly less than combo)
    val r1: RDD[Array[(Long, Array[Byte])]] = combos.mapPartitions(partition => {
      val hashtable = scala.collection.mutable.HashMap.empty[Long, Array[Byte]]

      partition.foreach(elem => {
        val thisId = elem._2
        val someCombos = bcastdata.value

        // Si le ID du combo est plus haut que ceux des combos choisis, on fait aucun travail.
        //println(s" haut if $thisId > $i + $step ")
        if (thisId > i + step) {
          val bbb = 2 //dummy statement for Scala
        }
        else {
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
                hashtable(i + j) = new Array[Byte]((i + j).toInt)
              }

              val answer = isAdjacent(elem._1, someCombos(j)._1)
              if (answer == true)
                hashtable(i + j)(thisId.toInt) = 1
              else hashtable(i + j)(thisId.toInt) = 0
            }

            j += 1
            loop
          }
        }
      })

      Iterator(hashtable.toArray)
      //Return an iterator here
    })

    //Fuse the sets
    val r2: RDD[(Long, Array[Byte])] = r1.flatMap(e => e).cache()

    //Debug mode. We print the adjacency lists
    if (debug == true) {
    val d1 = r2.mapPartitionsWithIndex((partition, it) => {

      var output = ""
      output += "Partition " + partition + "\n"
      it.foreach(e => {
        output += e._1 + " "
        e._2.foreach(b => output += b)
        output += "\n"
      })

      Iterator(output)
    }).collect().foreach(println)
    }

    val r3 = r2.reduceByKey((a, b) => {
      fuseadjlists(a, b)
    })

    if (debug == true) {
      println("Fused adjlists")
      val d2 = r3.mapPartitionsWithIndex((partition, it) => {
        var output = ""
        output += "Partition " + partition + "\n"
        it.foreach(e => {
          output += e._1 + " "
          e._2.foreach(b => output += b)
          output += "\n"
        })
        Iterator(output)
      }).collect().foreach(println)
    }

    r3
  }


  /** We assign  numbers to the clauses. We make sure that the numbers are spread out over the partitions.
    *
    * @param combos
    */
  def assign_numberClauses(combos: RDD[Array[Char]], sc: SparkContext) = {

    //Grab the number of combos per partition. Make it a map
    val sizes: Array[Int] = combos.mapPartitionsWithIndex((partition, iterator) => {
      Iterator(Tuple2(partition, iterator.size))
    }).collect().sortBy(_._1).map(e => e._2)

    val numberPartitions = sizes.length

    //We define this helper function right away
    def getNextPartition(current: Int): Int = {
      if (current == numberPartitions - 1) return 0
      else return current + 1
    }

    //Array that contains the results. Left is partition, Right is number
    var results = new ArrayBuffer[Tuple2[Int, Long]]()

    //Alternate the numbering for the partitions, and take in consideration the partition sizes
    val count = combos.count()

    var i = 0 //numbering starts at 0
    var j = 0 //first partition is 0 too
    //Alternate the placement of numbers
    //Algorithm : We place the number in the partition if there is still enough space in the partition. If there is not,
    //we move to the next partition
    loop

    def loop(): Unit = {

      while (true) {

        if (i == count) return //biggest number reached

        //Check if the partition is okay too
        if (sizes(j) == 0) {
          j = getNextPartition(j)
        }

        //There are numbers remaining in this partition. Add the result and continue to to next number, and the next partition
        else {
          sizes(j) -= 1 //decrease the quantity remaining in the partition
          results += Tuple2(j, i.toLong)
          i += 1
          j = getNextPartition(j)
        }

      }

    }

    //Bcast variable
    val bcast = sc.broadcast(results)

    val numberedCombos: RDD[(Array[Char], Long)] = combos.mapPartitionsWithIndex((partitionNo, it) => {

      //Just grab the numbers here
      val numbers: Seq[Long] = bcast.value.flatMap(elem => {
        if (partitionNo == elem._1) Some(elem._2)
        else None
      })

      var i = 0
      val ress = it.map(elem => {
        val result = (elem, numbers(i))
        i += 1
        result
      })
      ress
    })

    //Return ze result
    numberedCombos
  }


  /**
    * Final version of the progressive coloring algorithm
    * We could add an initial coloring with OrderColoring for 100 000 vertices.
    *
    * @param combos the RDD of combos
    * @param sc     SparkContext
    * @param memory the number of megabytes of memory to store adjlists
    * @return
    */
  def progressivecoloring_final(combos: RDD[Array[Char]], sc: SparkContext, memory: Int, algorithm: String = "OrderColoring") = {
    //First, color 100 000 elements. If the graph is colored, we return. Else we continue here.
    val count = combos.count()
    val colors = new Array[Int](count.toInt) //colors for every vertex, starting at 0. We pass this data structure around and modify it

    //Save the seed so that Spark can regenerate the combos on demand. This would save memory but add CPU time
    //checkpoint to HDFS or on a ssd drive would also work
    val seed = System.nanoTime()


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
    val combosNumbered = assign_numberClauses(mycombos, sc).cache()
    //needs cache or localcheckpoint here. We cannot regenerate this!


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

    var step = 0 //calculated value
    var chunkNo = 0

    loop

    def loop(): Unit = {
      chunkNo += 1
      println(s"Now processing Chunk $chunkNo of the graph...")
      System.gc() //calling garbage collection here

      if (i >= count) return //if we have colored everything, we are good to go

      //Calculate the step using our current position and the amount of available memory.
      step = determineStep(memory, i) //petit bug, grands nombres?
      if (debug == true) step = 6

      println(s"Currently working with a chunk of the graph with $step vertices. The chunk weights $memory megabytes")

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
      val r1: RDD[(Long, Array[Byte])] = genadjlist_hashtableReduceByKey(i, step, combosNumbered, someCombos, sc).cache()

      if (debug == true) {
        println("\n\n")
        var printed = r1.collect().sortBy(_._1)
        //Return the sorted adjacency list, to be used by the order colouring algorithm.
        println(s"Printing the adjmatrix for $i")
        printed.foreach(e => {
          print(e._1 + " ")
          e._2.foreach(b => print(b))
          print("\n")
        })
      }

      //Color the graph using these combos
      import newalgo._

      //Use KP when the graph is sparse (not dense)
      val r2 =
        if (algorithm == "KP") {
          progressive_kp.progressiveKP(colors, r1, r1.count().toInt, maxColor, sc)
        }
        else { //algorithm = "OC". The single threaded needs a sorted matrix of adjlists
          ordercoloring_progress(colors, r1.collect().sortBy(_._1), i, maxColor)
        }

      //Update the max color
      maxColor = r2._2
      r1.unpersist(true)
      i += step


      loop
    }

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
    * Color N vertices at a time using the Order Coloring algorithm.
    * The adjvectors are precalculated
    *
    * @return
    */
  def ordercoloring_progress(colors: Array[Int], adjMatrix: Array[(Long, Array[Byte])], i: Int,
                             maxColor: Int) = {

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
      for (k <- 0 until adjMatrix(j)._2.size) {
        if (adjMatrix(j)._2(k) == 1) {
          val thisColor = colors(k)
          neighborcolors(thisColor) = 1
        }
      }
      val foundColor = color(neighborcolors)
      colors(i + j) = foundColor

      if (foundColor > currentMaxColor) currentMaxColor = foundColor

      j += 1 //we color the next vertex
      loop
    }

    //Max color is used when the graph is small
    (colors, currentMaxColor)
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


  /** Our first function colors the initial 100 000 vertices with a single thread.
    * Use this when everything else works
    *
    * @param sequence
    * @param vertices
    * @return the list of colors for these vertices
    */
  def orderColoring_initial(vertices: Array[Array[Char]]) = {
    var maxColor = 0
    var colors = new Array[Int](vertices.size) //lookup table for a vertex and its color

    //Color each vertex in the order. i is also the number of vertices colored, and the current vertex.
    //At any time, i has i-1 neighbors.
    for (i <- 0 until vertices.size) {
      //Build the list of adjacent colors.
      val neighbor_colors = new Array[Int](maxColor + 2) //we could optimize this

      var j = 0
      loop

      def loop(): Unit = {
        if (j == i) return //our exit condition
        val answer = isAdjacent(vertices(i), vertices(j))
        if (answer == true) {
          neighbor_colors(colors(j)) = 1 //set the color using this neighbor
        }
        j += 1
        loop
      }

      //Choose from this list the best color
      val c = color(neighbor_colors)
      if (c > maxColor) maxColor = c
      colors(i) = c
    }

    //Max color is used when the graph is small
    (colors, maxColor)
  }


} //fin object progressive coloring


