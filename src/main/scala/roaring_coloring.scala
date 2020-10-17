package roaringcoloring

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.roaringbitmap.RoaringBitmap
import utils.utils

import progressivecoloring.progressive_coloring.assign_numberClauses
import progressivecoloring.progressive_coloring.determineStep
import scala.util.Random

import ordercoloring.OrderColoring.isAdjacent
import ordercoloring.OrderColoring.coloringToTests

/**
  * Bon lien:
  * https://richardstartin.github.io/posts/roaringbitmap-performance-tricks#batch-iteration
  *
  */
object roaring_coloring extends Serializable {

  var debug = false

  /**
    * Roaring bitmap iterator wrapper
    */


  /**
    * Color N vertices at a time using the Order Coloring algorithm.
    * The adjvectors are precalculated
    *
    * @return
    */
  def ordercoloring(colors: Array[Int], adjMatrix: Array[(Long, RoaringBitmap)], i: Int,
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


  /**
    * Final version of the progressive coloring algorithm
    * We could add an initial coloring with OrderColoring for 100 000 vertices.
    *
    * @param combos the RDD of combos
    * @param sc     SparkContext
    * @param memory the number of megabytes of memory to store adjlists
    * @return
    */
  def coloring_roaring(combos: RDD[Array[Char]], sc: SparkContext, memory: Int, algorithm: String = "OrderColoring") = {
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
      val r1 = genadjlist_roaring(i, step, combosNumbered, someCombos, sc).cache()

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

      //Color the graph using these combos
      import newalgo._

      //Use KP when the graph is sparse (not dense)
      val r2 =
        if (algorithm == "KP") {
          roaringkp.roaringkp.progressiveKP(colors, r1, r1.count().toInt, maxColor, sc)
        }
        else { //algorithm = "OC". The single threaded needs a sorted matrix of adjlists
          ordercoloring(colors, r1.collect().sortBy(_._1), i, maxColor)
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
    * Alternative implementation using Roaring bitmaps
    *
    */

  def genadjlist_roaring(i: Long, step: Long, combos: RDD[(Array[Char], Long)],
                         combosToColor: Array[(Array[Char], Long)], sc: SparkContext) = {

    //First step is to group every combo with every other combo to do a MapReduce.
    val bcastdata = sc.broadcast(combosToColor)

    //For every partition, we build a hash table of COMBO -> List of neighbors (all neighbors are strictly less than combo)
    val r1 = combos.mapPartitions(partition => {
      val hashtable = scala.collection.mutable.HashMap.empty[Long, RoaringBitmap]

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

      Iterator(hashtable.toArray)
      //Return an iterator here
    })

    //Fuse the sets
    val r2 = r1.flatMap(e => e).cache()

    // Debug mode. We print the adjacency lists
    if (debug == true) {
      val d1 = r2.mapPartitionsWithIndex((partition, it) => {

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

    val r3 = r2.reduceByKey((a, b) => {
      //fuseadjlists(a, b)
      a.or(b)
      a
    })

    val r4 = r3.mapValues(e => {
      e.runOptimize()
      e
    })

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


}
