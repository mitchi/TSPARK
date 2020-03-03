package setcover

import generator.gen
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import enumerator.distributed_enumerator._
import utils.utils._
import greedypicker._
import gen._


object Algorithm2 {
  var debug = false
  //Returns an array that contains the chosen sets
  /**
    * Some considerations :
    * https://stackoverflow.com/questions/1463284/hashset-vs-treeset
    * https://github.com/JerryLead/SparkInternals/blob/master/markdown/english/6-CacheAndCheckpoint.md
    * Repartition problems
    * https://stackoverflow.com/questions/36414123/does-a-flatmap-in-spark-cause-a-shuffle
    */

  /*

  This is a Set Cover solver.
  We get a hypergraph contained in a RDD.
  We output a list of sets that covers all the hypergraph.

  We checkpoint at each iteration for performance.
  We generate and send a random tiebreaker. We do this in order to get better results with algorithm reruns.

  The algorithm works like this :
  Every hyperedge is a set of vertices (many vertex). A vertex is a test. It contains every variable, and their value.
  We count how often a vertex appears. This is a big distributed map/reduce operation.
  We select the vertex that appears the most often. Now, all hyperedges that contain this vertex, we can delete.
  Why do we delete them? Well, since they contain this vertex, it means that they are already tested.
  */


  /*
   This is the Array[Char] version with on the fly decompression.
    Input is an array of combos.
    Output is an array of chosen tests

    We keep the combos. And we transform them on the fly into tests. This allows us to save space much better than compression.
    Exception in thread "main" org.apache.spark.SparkException: Cannot use map-side combining with array keys.

   */

  def greedy_algorithm(sc: SparkContext, v: Int, rdd: RDD[Array[Char]]): Array[Array[Char]] = {
    //sc.setCheckpointDir(".") //not used when using local checkpoint
    val randomGen = scala.util.Random
    var workingRDD = rdd
    //.cache()
    val logEdgesChosen = ArrayBuffer[Array[Char]]()
    var counter = 1

    //Unroll the RDD into a RDD of tests in String form
    var currentRDD = workingRDD.map(combo => {
      val tt = combo_to_tests(combo, v)
      tt.map(e => arrayToString(e))
    })

    currentRDD.cache()

    def loop(): Unit = {
      while (true) {

        currentRDD = currentRDD.localCheckpoint()

        //CustomLogger.logger.info(s"ITERATION NUMBER : $counter")
        counter += 1

        //println("Iteration : "+ counter)
        val cc = currentRDD.count()

        //Condition de fin, le RDD est vide
        if (currentRDD.isEmpty()) return

        //Trouver le sommet S qui est présent dans le plus de tTests (Transformation)
        val rdd_sommetsCount = currentRDD.flatMap(tests => {
          for (i <- tests)
            yield (i, 1)
        })

        // if (debug)  println("DEBUG sommetsCounts :" + rdd_sommetsCount.count())
        //if (debug)  rdd_sommetsCount.collect().foreach(  println(_))

        //Calculate the counts for each vertex (Transformation)
        val counts = rdd_sommetsCount.reduceByKey((a, b) => a + b)

        // if (debug)   println("DEBUG apres reducebykey")
        // if (debug)  counts.collect().foreach(  println(_))

        //Send random tiebreaker. Closest Long gets chosen in case of a tie.
        val tiebreaker = randomGen.nextLong() % 10

        //Find best vertex to cover the set (Action)
        val best = counts.reduce((a, b) => {
          var retValue = a
          //If same strength, we have to compare to the tiebreaker.
          if (a._2 == b._2) {
            val distanceA = Math.abs(tiebreaker - (a._1.hashCode() % 10))
            val distanceB = Math.abs(tiebreaker - (b._1.hashCode() % 10))
            if (distanceA > distanceB)
              retValue = b
            else retValue = a
          }
          else if (a._2 > b._2) retValue = a else retValue = b
          retValue
        })

        //Keep our chosen Set in the log
        val toKeep = stringToArray(best._1)
        logEdgesChosen.append(toKeep)

        //Remove dead T-tests
        //best_1 will be shipped to each task but it is rather small.
        //No need to use a BC variable here

        currentRDD = currentRDD.flatMap(tests => {
          if (tests.contains(best._1))
            None
          else Some(tests)
        })
      }
    }

    loop()
    // CustomLogger.logger.info(s"ITERATION NUMBER : $counter")
    logEdgesChosen.toArray
  }


  /**
    * This version uses Array[Char] tests, not integer tests.
    * We do on-the-fly generation for possible tests, and we put them right away inside the hash table.
    * Then we fold the hash tables, this is our mapreduce. And we choose the best test at every iteration
    * This algorithm will save all the memory it can, while being more cpu intensive. This is a good thing.
    *
    * @param sc
    * @param v
    * @param rdd
    * @return
    */
  def greedy_algorithm2(sc: SparkContext, v: Int, rdd: RDD[Array[Char]]): Array[Array[Char]] = {
    println("Using the Set Cover algorithm 1 step at a time")

    //sc.setCheckpointDir(".") //not used when using local checkpoint
    val randomGen = scala.util.Random
    var workingRDD = rdd
    //.cache()
    val logEdgesChosen = ArrayBuffer[Array[Char]]()
    var counter = 1

    //  Unroll the RDD into a RDD of tests in String form
    var currentRDD = workingRDD.map(combo => {
      arrayToString(combo)
    })

    //Keep this thing in cache
    currentRDD.cache()

    def loop(): Unit = {
      while (true) {

        //CustomLogger.logger.info(s"ITERATION NUMBER : $counter")
        counter += 1

        if (counter % 10 == 0)
          println("Iteration : " + counter)
        //        val cc = currentRDD.count()
        //        println(s"We still have to cover : $cc combos")

        //Condition de fin, le RDD est vide
        if (currentRDD.isEmpty()) return

        //Trouver le sommet S qui est présent dans le plus de tTests (Transformation)
        val s1 = currentRDD.mapPartitions(partition => {
          var hashmappp = scala.collection.mutable.HashMap.empty[String, Int]
          partition.foreach(combo => {
            val list = combo_to_tests(stringToArray(combo), v)
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

        //Fold would be better here
        //Voir le StackOverflow pour merge des maps https://stackoverflow.com/questions/7076128/best-way-to-merge-two-maps-and-sum-the-values-of-same-key
        var res = s1.fold(mutable.HashMap.empty[String, Int])(
          (acc, values) => {
            values.foreach(elem => { //for each element of a
              val key = elem._1
              val value = elem._2
              if (acc.get(key).isEmpty) acc(key) = elem._2
              else acc(key) += value
            })
            acc
          })

        case class bt(var test: String, var count: Int)
        var bestTest = bt("", 0)

        //Could be nice to use a Scala parallel Reduce here.
        //Find the best test inside the hash table
        //https://docs.scala-lang.org/overviews/parallel-collections/overview.html
        res.foreach(elem => {
          if (elem._2 > bestTest.count) {
            bestTest.count = elem._2
            bestTest.test = elem._1
          }
        })

        //Keep our chosen Set in the log
        val toKeep = stringToArray(bestTest.test)
        logEdgesChosen.append(toKeep)

        //Remove the tests
        currentRDD = currentRDD.flatMap(combo => {
          val list = combo_to_tests(stringToArray(combo), v)
          val result: Boolean = findTestinTests(toKeep, list)
          if (result == true) None
          else Some(combo)
        }).cache()

        //Checkpoint every 3 iterations
        // if (counter % 3 == 0)
        currentRDD = currentRDD.localCheckpoint()

      } //fin du while
    } //fin fct loop

    loop()
    // CustomLogger.logger.info(s"ITERATION NUMBER : $counter")
    logEdgesChosen.toArray
  }


  /**
    * This version uses Array[Char] tests, not integer tests.
    * We do on-the-fly generation for possible tests, and we put them right away inside the hash table.
    * Then we fold the hash tables, this is our mapreduce. And we choose the best test at every iteration
    * This algorithm will save all the memory it can, while being more cpu intensive. This is a good thing.
    *
    * This version covers M tests at the same time. The M value can be increased during the algorithm.
    *
    * @param sc
    * @param v
    * @param rdd
    * @return
    */
  def greedy_algorithm_list(sc: SparkContext, v: Int, rdd: RDD[Array[Char]], vstep: Int = -1): Array[Array[Char]] = {
    //sc.setCheckpointDir(".") //not used when using local checkpoint
    val randomGen = scala.util.Random
    var workingRDD = rdd
    //.cache()
    var logEdgesChosen = ArrayBuffer[Array[Char]]()
    var counter = 1

    //  Unroll the RDD into a RDD of tests in String form
    var currentRDD = workingRDD.map(combo => {
      arrayToString(combo)
    })

    //Let's calculate a speed for Set Cover
    //Speed will be : 1% of the logarithm of the combos
    val sizeRDD = rdd.count()
    var M = scala.math.log10(sizeRDD).toInt
    if (M < 1) M = 1

    //If we get the value from a parameter, we change it here.
    if (vstep != -1) M = vstep

    println(s"Our value of vstep is $M")
    //Keep this thing in cache
    currentRDD.cache()

    def loop(): Unit = {
      while (true) {

        //CustomLogger.logger.info(s"ITERATION NUMBER : $counter")
        counter += 1

        if (counter % 10 == 0)
          println("Iteration : " + counter)
        //        val cc = currentRDD.count()
        //        println(s"We still have to cover : $cc combos")

        //Condition de fin, le RDD est vide
        if (currentRDD.isEmpty()) return

        //Trouver le sommet S qui est présent dans le plus de tTests (Transformation)
        val s1 = currentRDD.mapPartitions(partition => {
          var hashmappp = scala.collection.mutable.HashMap.empty[String, Int]
          partition.foreach(combo => {
            val list = combo_to_tests(stringToArray(combo), v)
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

        //Fold would be better here
        //Voir le StackOverflow pour merge des maps https://stackoverflow.com/questions/7076128/best-way-to-merge-two-maps-and-sum-the-values-of-same-key
        var res = s1.fold(mutable.HashMap.empty[String, Int])(
          (acc, values) => {
            values.foreach(elem => { //for each element of a
              val key = elem._1
              val value = elem._2
              if (acc.get(key).isEmpty) acc(key) = elem._2
              else acc(key) += value
            })
            acc
          })

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
          if (elem._2 == bestCount && Mpicks != M) {
            //println(s"We pick a test here. The best count is $bestCount")
            val toKeep = stringToArray(elem._1)
            MTests += toKeep
            Mpicks += 1
          }
        })

        //Remove the tests
        currentRDD = currentRDD.flatMap(combo => {
          val list = combo_to_tests(stringToArray(combo), v)
          var delete = false

          //For every element
          for (elem <- list) {

            var k = 0
            loop

            def loop(): Unit = {
              if (k == MTests.size) return
              val answer = comparetwoarrays(elem, MTests(k))
              if (answer == true) {
                delete = true
                return
              }
              k += 1
              loop
            }
          }

          if (delete == true) None
          else Some(combo)
        }).cache()

        //Add the M tests to logEdgesChosen
        MTests.foreach(test => {
          logEdgesChosen += test
        })

        //Checkpoint every 3 iterations
        // if (counter % 3 == 0)
        currentRDD = currentRDD.localCheckpoint()

      } //fin du while
    } //fin fct loop

    loop()
    // CustomLogger.logger.info(s"ITERATION NUMBER : $counter")
    logEdgesChosen.toArray
  }


  /**
    * This version uses Array[Char] tests, not integer tests.
    * We do on-the-fly generation for possible tests, and we put them right away inside the hash table.
    * Then we fold the hash tables, this is our mapreduce. And we choose the best test at every iteration
    * This algorithm will save all the memory it can, while being more cpu intensive. This is a good thing.
    *
    * This version covers M tests at the same time.
    * This value of M is calculated using an algorithm. A test is picked if it is at least 50% different from all the
    * previously picked K tests  (K < M)
    *
    * @param sc
    * @param v
    * @param rdd
    * @return
    */
  def greedy_setcover_buffet(sc: SparkContext, v: Int, rdd: RDD[Array[Char]], vstep: Int = -1): Array[Array[Char]] = {
    //sc.setCheckpointDir(".") //not used when using local checkpoint
    val randomGen = scala.util.Random
    //.cache()
    var logEdgesChosen = ArrayBuffer[Array[Char]]()
    var counter = 1

    //Get the number of partitions
    val num_partitions = rdd.getNumPartitions
    println("Number of partitions : " + num_partitions)

    //  Unroll the RDD into a RDD of tests in String form
    var currentRDD = rdd.map(combo => {
      arrayToString(combo)
    }).cache()

    val sizeRDD = rdd.count()

    //Max picks = 1 percent of the size
    var maxPicks = sizeRDD / 100
    // if (maxPicks < 500) maxPicks = 500
    if (maxPicks < 5) maxPicks = 2

    println("Repartitionning...")
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
            val list = combo_to_tests(stringToArray(combo), v)
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
    * The problem with the filter combo technique is that it can be very slow when the test suite becomes very large.
    * It is better to filter using a fixed number of tests at a time. Otherwise we will be testing the same combos multiple times.
    *
    * @param testSuite
    * @param combos
    */
  def progressive_filter_combo_string[A](testSuite: Array[Array[Char]], combos: RDD[String],
                                         sc: SparkContext, speed: Int = 50) = {
    //Pick 1000 tests and filter the combos with them
    //Find out what the t value is
    var t = findTFromCombo(combos.first())
    var i = 0

    val testSuiteSize = testSuite.length
    var filtered = combos

    //filtered.cache()

    def loop(): Unit = {

      val j = if (i + speed > testSuiteSize) testSuiteSize else i + speed
      val broadcasted_tests = sc.broadcast(testSuite.slice(i, j))
      //Broadcast and filter using these new tests
      println("Broadcasting the tests, and filtering them")
      filtered = filtered.filter(combo => filter_combo(combo, broadcasted_tests, t))
      //println("Number of combos after filter " + filtered.count())

      if ((i + speed > testSuiteSize)) return //if this was the last of the tests

      broadcasted_tests.unpersist(false)

      i = j //update our i

      filtered.localCheckpoint()

      loop
    }

    loop

    //Return the RDD of combos
    filtered
  }

} //fin object algorithm2

