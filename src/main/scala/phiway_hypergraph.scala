import java.util

import central.gen.{filename, save}
import enumerator.distributed_enumerator._
import hypergraph_cover.Algorithm2.progressive_filter_combo_string
import hypergraph_cover.greedypicker.greedyPicker
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.roaringbitmap.RoaringBitmap
import utils.utils.{arrayToString, saveTestSuite, stringToArray}
import utils.utils
import phiway.phiway._

import scala.collection.mutable.ArrayBuffer

object phiway_hypergraph extends Serializable {


  case class it(bitmap: RoaringBitmap) {
    var iteratorr = bitmap.iterator()
  }

  /**
    * Get the next value, in order to create a test
    *
    * @param i
    */
  def getNext(set: RoaringBitmap, i: Int): Unit = {
    val a: util.Iterator[Integer] = set.iterator()
    val b = a.next() //envoie une exception si yen a plus

  }


  /**
    * From a clause, we generate every possible test.
    * Every time, we use the boolean conds to generate the appropriate domain of values.
    * We also use the list of domain sizes for each parameter.
    * This is a global variable.
    *
    */
  def clauseToTests(clause: clause): Array[String] = {

    var tests = new ArrayBuffer[String]() //Generated tests
    var sets = new ArrayBuffer[it]()

    //On fait les bitmap compressées a partir des conditions de la clause
    var ii = 0
    val bitmaps = clause.conds.map(e => {
      val ret = condToSET(e, ii)
      ii += 1
      ret
    })

    //Loop until we have generated all possible tests
    loop;
    def loop(): Unit = {
      while (true) {

      }
    }


  }


  /**
    * Greedy algorithm for hypergraph covering
    * Every hyperedge is a phi-way clause.
    * Every phiway clause votes for tests, every iteration
    * The best tests are picked using an algorithm called greedypicker.
    * The greedypicker algorithm picks a diverse set of tests.
    *
    * @param sc
    * @param v
    * @param rdd
    * @return
    */
  def greedyalgorithm(sc: SparkContext,
                      rdd: RDD[clause]): Array[String] = {
    //sc.setCheckpointDir(".") //not used when using local checkpoint
    val randomGen = scala.util.Random
    //.cache()
    var logEdgesChosen = ArrayBuffer[String]()
    var counter = 1
    var currentRDD = rdd
    val sizeRDD = rdd.count()

    //Get the number of partitions
    val num_partitions = rdd.getNumPartitions
    println("Number of partitions : " + num_partitions)

    //Max picks = 1 percent of the size
    var maxPicks = sizeRDD / 100
    // if (maxPicks < 500) maxPicks = 500
    if (maxPicks < 5) maxPicks = 2

    println("Repartitioning...")
    currentRDD = currentRDD.repartition(num_partitions * 2)
    println("Done")

    def loop(): Unit = {
      while (true) {

        counter += 1
        println("Iteration : " + counter)

        //Condition de fin, le RDD est vide
        if (currentRDD.isEmpty()) return

        //Trouver le sommet S qui est présent dans le plus de tTests (Transformation)
        val s1 = currentRDD.mapPartitions(partition => {
          var hashmappp = scala.collection.mutable.HashMap.empty[String, Int]
          partition.foreach(combo => {
            val list = clauseToTests(stringToArray(combo), v)
            list.foreach(elem => {
              val key = arrayToString(elem)
              if (hashmappp.get(key).isEmpty) //if entry is empty
                hashmappp.put(key, 1)
              else
                hashmappp(key) += 1
            })
          })
          Iterator(hashmappp)
        }
        )

        var res = s1.flatMap(hash => hash.toSeq).reduceByKey((a, b) => a + b).collect()

        case class bt(var test: String = "", var count: Int = 0)
        var bestCount = 0
        var MTests = new ArrayBuffer[Array[Char]]()
        var Mpicks = 0

        //First we find the best count
        //https://docs.scala-lang.org/overviews/parallel-collections/overview.html
        res.foreach(elem => {
          if (elem._2 > bestCount) {
            bestCount = elem._2
          }
        })

        //   Then we can pick as much as M such counts
        res.foreach(elem => {
          if (elem._2 == bestCount && Mpicks != maxPicks) {
            //println(s"We pick a test here. The best count is $bestCount")
            val toKeep = stringToArray(elem._1)
            MTests += toKeep
            Mpicks += 1
          }
        })

        println("Picking the tests")
        val chosenTests = greedyPicker(MTests)
        println(s"We have chosen " + chosenTests.size + " tests in this iteration using the greedy picker algorithm")

        //Quand on enleve 1000 tests a la fois, c'est trop long...
        //Il faut en enlever moins a la fois.
        currentRDD = progressive_filter_combo_string(chosenTests.toArray, currentRDD, sc)

        //Add the M tests to logEdgesChosen
        chosenTests.foreach(test => {
          logEdgesChosen += test
        })

        //Checkpoint every 3 iterations
        currentRDD = currentRDD.localCheckpoint()

      } //fin du while
    } //fin fct loop

    loop()
    // CustomLogger.logger.info(s"ITERATION NUMBER : $counter")
    logEdgesChosen.toArray
  }


  /**
    * Here we execute a simple setcover algorithm
    */
  def phiway_hypergraphcover(clausesFile: String, sc: SparkContext): Array[String] = {

    println("Hypergraph Vertex Cover for Phi-way testing")
    val clauses = readPhiWayClauses(clausesFile)
    val clausesRDD = sc.makeRDD(clauses)
    //println(s"Working with a Phi-way set of $number clauses")

    //Write to results.txt
    import java.io._
    val pw = new PrintWriter(new FileOutputStream(filename, true))

    var t1 = System.nanoTime()
    //Now we have the combos, we ship them directly to the setcover algorithm

    val result = greedyalgorithm(sc, clausesRDD)

    var t2 = System.nanoTime()
    var time_elapsed = (t2 - t1).toDouble / 1000000000
    println(s"Hypergraph vertex cover time: $time_elapsed seconds")
    println("We have found " + result.size + " tests")

    pw.append(s"$clausesFile;PHIWAY_HYPERGRAPH;$time_elapsed;${result.size}\n")
    println(s"$clausesFile;PHIWAY_HYPERGRAPH;$time_elapsed;${result.size}\n")
    pw.flush()

//    //If the option to save to a text file is activated
//    if (save == true) {
//      println(s"Saving the test suite to a file named $t;$n;$v.txt")
//      //Save the test suite to file
//      saveTestSuite(s"$t;$n;$v.txt", result)
//    }

    //Return the results
    result
  }

}
