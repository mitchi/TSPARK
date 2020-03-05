package ipog

import cmdline.resume_info
import enumerator.distributed_enumerator.{fastGenCombos, genPartialCombos}
import generator.gen._
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import utils.utils.saveTestSuite
import utils.utils

object d_ipog_coloring {


  /**
    * A hybrid IPOG (Distributed Horizontal Growth + Distributed Graph Coloring) that covers M tests at a time during set cover. M value is determined at runtime.
    *
    * @param n
    * @param t
    * @param v
    * @param sc
    * @return
    */
  def distributed_ipog_coloring(n: Int, t: Int, v: Int, sc: SparkContext,
                                numberProcessors: Int = 6, hstep: Int = -1,
                                resume: Option[resume_info] = None): Array[Array[Char]] = {
    val expected = utils.numberTWAYCombos(n, t, v)
    println("Parallel IPOG with M tests")
    println(s"Number of parallel graph colorings : $numberProcessors")
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

    loop

    //Cover all the remaining parameters
    def loop(): Unit = {
      //Exit condition
      if (i + t == n) return

      println("Currently covering parameter : " + (i + t + 1))
      var newCombos = genPartialCombos(i + t, t - 1, v, sc).persist(StorageLevel.MEMORY_AND_DISK)

      println(s" ${newCombos.count()} combos to cover by setcover_m_progressive")
      println(s" we currently have ${tests.size} tests")

      //Apply horizontal growth
      // val r1 = setcover_m_progressive(tests, newCombos, v, t, sc)
      val r1 = horizontalgrowth_1percent(tests, newCombos, v, t, sc, hstep)

      newCombos = r1._2 //Retrieve the combos that are not covered
      tests = r1._1 //Replace the tests

      println(s" ${newCombos.count()} combos remaining , sending to graph coloring")

      //If there are still combos left to cover, apply a vertical growth algorithm
      if (newCombos.isEmpty() == false) {
        tests = updateTestColoring(tests, newCombos, sc, v)
      }

      i += 1 //move to another parameter

      println(s"ts size : ${tests.size}")

      var t2 = System.nanoTime()
      var time_elapsed = (t2 - t1).toDouble / 1000000000

      pw.append(s"$t;${i + t};$v;PARALLEL_IPOG_M_COLORING;$time_elapsed;${tests.size}\n")
      println(s"$t;${i + t};$v;PARALLEL_IPOG_M_COLORING;$time_elapsed;${tests.size}\n")
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


  /**
    * This is a version that uses the set cover algorithm instead of graph coloring. It should go more slowly, but offer better results.
    * Supports resuming a test suite as well.
    *
    * @param n
    * @param t
    * @param v
    * @param sc
    * @return
    */
  def distributed_ipog_hypergraph(n: Int, t: Int, v: Int, sc: SparkContext, hstep: Int = -1, vstep: Int = -1,
                                  resume: Option[resume_info] = None): Array[Array[Char]] = {
    val expected = utils.numberTWAYCombos(n, t, v)
    println("Parallel IPOG with M tests")
    println(s"Using Hypergraph Set Cover for Vertical Growth")
    println(s"Using hstep=$hstep and vstep=$vstep")
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

    loop

    //Cover all the remaining parameters
    def loop(): Unit = {
      //Exit condition
      if (i + t == n) return

      println("Currently covering parameter : " + (i + t + 1))
      var newCombos = genPartialCombos(i + t, t - 1, v, sc).persist(StorageLevel.MEMORY_AND_DISK)

      println(s" ${newCombos.count()} combos to cover by setcover_m_progressive")
      println(s" we currently have ${tests.size} tests")

      //Apply horizontal growth
      // val r1 = setcover_m_progressive(tests, newCombos, v, t, sc)
      //val r1 = horizontalgrowth_1percent(tests, newCombos, v, t, sc, hstep)
      val r1 = horizontalgrowthfinal(tests, newCombos, v, t, sc, hstep)

      newCombos = r1._2 //Retrieve the combos that are not covered
      tests = r1._1 //Replace the tests

      println(s" ${newCombos.count()} combos remaining , sending to set cover algorithm")

      //If there are still combos left to cover, apply a vertical growth algorithm
      if (newCombos.isEmpty() == false) {
        tests = updateTestsSetCover(tests, newCombos, sc, v, vstep)
      }

      i += 1 //move to another parameter

      println(s"ts size : ${tests.size}")

      var t2 = System.nanoTime()
      var time_elapsed = (t2 - t1).toDouble / 1000000000

      pw.append(s"$t;${i + t};$v;PARALLEL_IPOG_M_SETCOVER;$time_elapsed;${tests.size}\n")
      println(s"$t;${i + t};$v;PARALLEL_IPOG_M_SETCOVER;$time_elapsed;${tests.size}\n")
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


}
