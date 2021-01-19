package ipog

import cmdline.resume_info
import enumerator.distributed_enumerator.{fastGenCombos, genPartialCombos, genPartialCombos2}
import central.gen._
import com.sun.org.apache.xml.internal.security.algorithms.JCEMapper.Algorithm
import ordercoloring.OrderColoring.orderColoring
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import utils.utils.{containsStars, saveTestSuite}
import utils.utils
import roaringcoloring.roaring_coloring.coloring_roaring

import cmdlineparser.TSPARK.save


object d_ipog_roaring {

  var debug = false //debug global variable
  import cmdlineparser.TSPARK.resume

  /**
    * Fonction utilisée par D-IpogColoring-Roaring
    *
    * @param tests
    * @return
    */
  def entergraphcoloring(tests: Array[Array[Char]], combos: RDD[Array[Char]],
                         sc: SparkContext, chunksize: Int = 20000,
                         algorithm: String = "OC"): Array[Array[Char]] = {

    //Union the incomplete tests and the combos. And remove the incomplete tests from the test set
    val incompleteTests = tests.flatMap(arr => {
      if (containsStars(arr) == true) {
        Some(arr)
      }
      else None
    })

    //Do the contrary here. Remove those we have just added from the original test set. This could be more optimal but would need custom code I guess.
    var testsWithoutStars = tests.flatMap(arr => {
      if (containsStars(arr) == true) {
        None
      }
      else Some(arr)
    })

    var input = combos.collect union incompleteTests

    var coloredTests = coloring_roaring(sc.makeRDD(input), sc, chunksize, algorithm)

    coloredTests ++ testsWithoutStars
  }

  /**
    * A hybrid IPOG (Distributed Horizontal Growth + Distributed Graph Coloring) that covers M tests at a time during set cover. M value is determined at runtime.
    *
    * @param n
    * @param t
    * @param v
    * @param sc
    * @return
    */
  def distributed_ipog_coloring_roaring(n: Int, t: Int, v: Int, sc: SparkContext,
                                        hstep: Int = -1,
                                        chunksize: Int = 20000, algorithm: String = "OC"): Array[Array[Char]] = {
    val expected = utils.numberTWAYCombos(n, t, v)
    println("Parallel IPOG ROARING with M tests")
    println(s"Chunk size: $chunksize vertices")
    println(s"Algorithm : $algorithm")
    println(s"Problem : n=$n,t=$t,v=$v")
    println(s"Expected number of combinations is : $expected ")
    println(s"Formula is C($n,$t) * $v^$t")

    var time_elapsed = 0

    import java.io._
    val pw = new PrintWriter(new FileOutputStream(filename, true))

    //Horizontal extend all of them
    var tests =
      if (resume.isEmpty) {
        fastGenCombos(t, t, v, sc).collect()
      }
      else {
        resume.get.tests
      }

    //We have started with t covered parameters
    var i = 0

    if (!resume.isEmpty) {
      println(s"Resuming the test suite extension at parameter ${resume.get.param}")
      i = resume.get.param - t - 1
    }

    var t1 = System.nanoTime()

    if (debug == true) {
      println("Printing the initial test suite...")
      tests.foreach(utils.print_helper(_))
    }

    loop

    //Cover all the remaining parameters
    def loop(): Unit = {
      //Exit condition
      if (i + t == n) return

      println("Currently covering parameter : " + (i + t + 1))
      var newCombos = genPartialCombos(i + t, t - 1, v, sc).persist()

      println(s" ${newCombos.count()} combos to cover")
      println(s" we currently have ${tests.size} tests")

      if (debug == true) {
        println("Printing the partial combos...")
        newCombos.collect().foreach(utils.print_helper(_))
      }

      //Apply horizontal growth
      // val r1 = setcover_m_progressive(tests, newCombos, v, t, sc)
      val r1 = horizontalgrowthfinal(tests, newCombos, v, t, sc, hstep)

      newCombos = r1._2 //Retrieve the combos that are not covered
      tests = r1._1 //Replace the tests

      if (debug == true) {
        println("Printing the tests after horizontal growth...")
        tests.foreach(utils.print_helper(_))
      }

      println(s" ${newCombos.count()} combos remaining , sending to graph coloring")

      //If there are still combos left to cover, apply a vertical growth algorithm
      if (newCombos.isEmpty() == false) {

        if (debug == true) {
          println("Printing the remaining combos...")
          newCombos.collect().foreach(utils.print_helper(_))
        }

        tests = entergraphcoloring(tests, newCombos, sc, chunksize, algorithm)
      }

      i += 1 //move to another parameter

      println(s"ts size : ${tests.size}")

      var t2 = System.nanoTime()
      var time_elapsed = (t2 - t1).toDouble / 1000000000

      pw.append(s"$t;${i + t};$v;PARALLEL_IPOG_M_COLORING_ROARING;$time_elapsed;${tests.size}\n")
      println(s"$t;${i + t};$v;PARALLEL_IPOG_M_COLORING_ROARING;$time_elapsed;${tests.size}\n")
      pw.flush()

      //If the option to save to a text file is activated
      if (save == true) {
        println(s"Saving the test suite to a file named $t;${i + t};$v.txt")
        //Save the test suite to file
        saveTestSuite(s"$t;${i + t};$v.txt", tests)
      }

      System.gc()
      loop
    }

    //Return the test suite
    tests
  }


} //end of the singleton object
