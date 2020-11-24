import enumerator.distributed_enumerator.fastGenCombos
import ordercoloring.OrderColoring.coloringToTests
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import roaringcoloring.roaring_coloring._
import utils.utils.saveTestSuite
import utils.utils
import phiway.phiway.{clause, readPhiWayClauses}
import progressivecoloring.progressive_coloring.assign_numberClauses

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object phiway_coloring extends Serializable {

  var debug = true
  var save = false
  var filename = "results.txt"


  /** We assign  numbers to the clauses. We make sure that the numbers are spread out over the partitions.
    * Phiway coloring
    *
    * @param combos
    */
  def phiway_numberclauses(combos: RDD[clause], sc: SparkContext) = {

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

    val numberedCombos: RDD[(clause, Long)] = combos.mapPartitionsWithIndex((partitionNo, it) => {

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
    * Phiway coloring
    *
    * @param combos the RDD of combos
    * @param sc     SparkContext
    * @param step   the number of vertices to move at the same time. Memory should increase with each iteration
    * @return
    */
  def phiway_coloring(clauses: RDD[clause], sc: SparkContext, step: Int, algorithm: String = "OrderColoring") = {
    //First, color 100 000 elements. If the graph is colored, we return. Else we continue here.
    val count = clauses.count()
    val colors = new Array[Int](count.toInt) //colors for every vertex, starting at 0. We pass this data structure around and modify it

    //Save the seed so that Spark can regenerate the combos on demand. This would save memory but add CPU time
    //checkpoint to HDFS or on a ssd drive would also work
    val seed = System.nanoTime()

    var totalIterations = 0

    //Print the combos 1 time
    if (debug == true) {
      println("Printing combos before shuffle")
      clauses.collect().foreach(utils.print_helper(_))
    }

    //Shuffle the combos before. Doing this ensures a different result every run.
    var myclauses = clauses.mapPartitions(it => {

      if (debug == true) {
        Random.setSeed(1)
      }
      else Random.setSeed(seed)

      Random.shuffle(it)
    }, true)

    //Print the combos 1 time
    if (debug == true) {
      println("Printing combos after shuffle")
      myclauses.collect().foreach(println)
    }

    //TODO : Color 100 000 vertex first before doing the rest.
    //val combosNumbered = mycombos.zipWithIndex().cache()
    val clausesNumbered = phiway_numberclauses(myclauses, sc).cache()

    //We no longer need combos
    clauses.unpersist(false)

    //Print the combos 1 time
    if (debug == true) {
      println("Printing combos after numbering")

      clausesNumbered.mapPartitionsWithIndex((index, iterator) => {
        var stringPartition = ""
        stringPartition += "Partition " + index + "\n"
        iterator.foreach(e => {
          stringPartition += e._2 + " " + print(e._1) + "\n"
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
      val someCombos = clausesNumbered.flatMap(elem => {
        if (elem._2 < i + step && elem._2 >= i) {
          Some(elem)
        }
        else None
      }).collect().sortBy(_._2)

      //Print the combos now that they are shuffled (Debug mode)
      if (debug == true) {
        someCombos.foreach(e => {
          print(e._2 + " ")
          print(e._1)
        })
      }

      //Generate the adjlists for every combo in that list
      // val r1 = genadjlist_roaring(i, step, combosNumbered, someCombos, sc).cache()
      val filteredCombos = filterBig(clausesNumbered, i, step)
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
    val properFormRDD: RDD[(Int, Array[Char])] = clausesNumbered.map(elem => {
      val id = elem._2
      val color = bcastcolors.value(id.toInt)
      (color, elem._1)
    })

    //Transform into tests
    coloringToTests(properFormRDD)
  }


  /**
    * The distributed graph coloring algorithm with roaring bitmaps and phiway coverage
    * This function starts the whole process. It is called from the cmdline function
    *
    * @param n
    * @param t
    * @param v
    * @param sc
    * @return
    */
  def start_graphcoloring_phiway(n: Int, v: Int, clausesFile: String, sc: SparkContext,
                                 chunkSize: Int = 4000, algorithm: String = "OrderColoring"):
  Array[Array[Char]] = {

    //Read the clauses
    val clauses = readPhiWayClauses(clausesFile)
    val number = clauses.size

    println("Distributed Graph Coloring with Roaring bitmaps")
    println(s"Using a chunk size = $chunkSize vertices and algorithm = $algorithm")
    println(s"Using a set of phiway clauses instead of interaction strength")
    println(s"Problem : n=$n,v=$v and $number clauses")

    import java.io._
    val pw = new PrintWriter(new FileOutputStream(filename, true))

    var t1 = System.nanoTime()

    val tests = phiway_coloring(sc.makeRDD(clauses), sc, chunkSize, algorithm)

    var t2 = System.nanoTime()
    var time_elapsed = (t2 - t1).toDouble / 1000000000

    pw.append(s"$number;$n;$v;GRAPHCOLORING_PHIWAY;$time_elapsed;${tests.size}\n")
    println(s"$number;$n;$v;GRAPHCOLORING_PHIWAY;$time_elapsed;${tests.size}\n")
    pw.flush()

    //If the option to save to a text file is activated
    if (save == true) {
      println(s"Saving the test suite to a file named $number;$n;$v.txt")
      //Save the test suite to file
      saveTestSuite(s"$number;$n;$v.txt", tests)
    }

    //Return the test suite
    tests
  }


} //fin object phiway_coloring
