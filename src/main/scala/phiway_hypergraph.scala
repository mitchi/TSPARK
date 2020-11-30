package phiway_hypergraph

import central.gen.{filename, filter_combo, isComboHere}
import hypergraph_cover.Algorithm2.progressive_filter_combo_string
import hypergraph_cover.greedypicker.greedyPicker
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import utils.utils.{arrayToString, findTFromCombo, stringToArray}
import phiway.phiway._
import phiway.phiway.compatible
import phiwaycoloring.phiway_coloring.saveTestSuite

import scala.collection.mutable.ArrayBuffer

object phiway_hypergraph extends Serializable {


  /**
    * Compare two tests and return a score of difference.
    * The idea here is that this function will help in SetCover functions to select tests that have more differences.
    * The bigger the difference between two tests, the better it is.
    *
    * @param test1
    * @param test2
    * @return
    */
  def differenceScore(test1: String, test2: String): Int = {
    var difference = 0
    for (i <- 0 until test1.size) {
      if (test1(i) != test2(i)) difference += 1
    }
    difference
  }

  /**
    * This is a greedy algorithm for selecting good tests during the set cover algorithm
    * We will select a number of tests that are equally good at covering combos.
    * Then, we will compare the second test to the first test
    * Then, the third test will be compared to the second and first test and so on.
    *
    * We will add a test if its difference with every other test is at least 50%
    *
    * Tester dans le futur : Ajouter le check de maxPicks dans cette fonction, au lieu d'envoyer un plus petit tableau
    *
    */
  def greedyPicker(tests: ArrayBuffer[String]): ArrayBuffer[String] = {
    var chosenTests = new ArrayBuffer[String]()
    chosenTests += tests(0) //choose the first test right away
    val sizeOfTests = tests(0).size

    //If there's only one test, return right away
    if (tests.size < 2) return chosenTests
    // var j = 1

    for (i <- 1 until tests.size) {
      var thisTest = tests(i)
      var j = 0

      var found = true

      //Compare this test to all the others. Pick it if it's always at least 50% different
      loop

      def loop(): Unit = {
        if (j == chosenTests.size) return
        val oneOfTheChosen = chosenTests(j)

        val difference = differenceScore(thisTest, oneOfTheChosen)
        val diff = (difference.toDouble / sizeOfTests.toDouble) * 100

        //If the test has only a little difference, we don't pick it
        if (diff < 40.0) {
          found = false
          return
        }
        j += 1
        loop
      }

      //If the test was good, we pick it
      if (found == true) {
        chosenTests += thisTest
      }

    } //end for loop
    chosenTests
  }


  /**
    * Compare a clause with a test.
    * If the test covers the clause, we can delete this clause.
    *
    * We compare all the conditions.
    * When one condition is not compatible, the whole thing is not compatible.
    * If every condition is compatible, then we can filter out this clause
    *
    * @param combo
    * @param test
    * @param t
    * @tparam A
    * @return
    */
  def isClauseHere(theClause: clause, test: String): Boolean = {

    var i = 0
    var iscompatible = true

    //Decompose the test in several values
    val values = test.split(";").map(s => s.toShort)

    loop;
    def loop(): Unit = {
      for (cond <- theClause.conds) {
        val cc = booleanCond('=', values(i))
        val result = compatible(cond, cc)
        if (result == false) {
          iscompatible = false;
          return
        }
        i += 1
      }
    }
    //Return found true or false
    iscompatible
  }


  /**
    * We filter out a clause using an array of tests
    */
  def filter_clause(myClause: clause,
                    tests: ArrayBuffer[String]): Boolean = {

    var i = 0
    var end = tests.length
    var returnValue = false

    loop;
    def loop(): Unit = {
      if (i == end) return
      if (isClauseHere(myClause, tests(i))) {
        returnValue = true
        return
      }
      i += 1
      loop
    }

    !returnValue
  }

  /**
    * The problem with the filter combo technique is that it can be very slow when
    * the test suite becomes very large.
    * It is better to filter using a fixed number of tests at a time.
    * Otherwise we will be testing the same combos multiple times.
    *
    * @param testSuite
    * @param combos
    */
  def progressive_filter(testSuite: ArrayBuffer[String],
                         clauses: RDD[clause],
                         sc: SparkContext,
                         speed: Int = 50) = {
    //Find out what the t value is
    var i = 0
    val testSuiteSize = testSuite.length
    var filtered = clauses

    loop;
    def loop(): Unit = {

      val j = if (i + speed > testSuiteSize) testSuiteSize else i + speed
      val broadcasted_tests = testSuite.slice(i, j)
      //Broadcast and filter using these new tests
      println("Broadcasting the tests, and filtering them")
      filtered = filtered.filter(combo => filter_clause(combo, broadcasted_tests))

      if ((i + speed > testSuiteSize)) return //if this was the last of the tests

      i = j //update our i
      filtered.localCheckpoint()
      loop
    }
    //Return the RDD of combos
    filtered
  }

  /**
    * Manage the available domain for a parameter
    *
    * @param domain
    * @param iterator
    */
  case class domainParam(domain: ArrayBuffer[Int], var iterator: Int = 0) {

    def printDomain: String = {
      var output = ""
      for (i <- domain) {
        output += i
      }
      output
    }

    //Return the current value
    override def toString: String = {
      return domain(iterator).toString
    }


    /**
      * Reset the domain size
      */
    def reset(): Unit = {
      iterator = 0
    }

    /**
      * Increment the domain of the parameter using the iterator.
      * Always use this function before the increment function
      */
    def tryIncrement(): Int = {
      //Case 1: If we increment, we go over the domain size
      if (iterator + 1 == domain.length) {
        return 1
      }

      //Case 2: We can increment without any problem
      // else (iterator +1 < domain.length)

      return 2

    }

    /**
      * Go to the next element in the domain
      * Or loop back to the first element
      */
    def increment(): Unit = {
      iterator += 1
    }

  }

  /**
    * Manage the iteration over all domains
    */
  case class combinator(var domains: Array[domainParam]) {

    override def toString: String = {
      var output = ""
      var lastI = domains.length - 1
      var i = 0

      for (it <- domains) {
        if (i == lastI) output += it //Remove last ;
        else output += it + ";"
        i += 1
      }
      //output.substring(0, output.length - 1)
      output
    }


    /**
      * Generate the combinations
      */
    def generate() = {
      val tests = new ArrayBuffer[String]()
      var end = false

      while (!end) {

        val test = toString()
        tests += test
        end = increment_left()
      }

      tests
    }

    /**
      * We try to increment the domains data structure to produce a new test.
      * If we cannot increment any of the domains, we return that the end has been reached.
      *
      * @return
      */
    def increment_left(): Boolean = {

      var i = 0 //this iterator allows us to change to another parameter domain
      var theEnd = false //0 the end, 1 same parameter, 2 next parameter

      loop;
      def loop(): Unit = {

        //We have nowhere else to increment. Its over!
        if (i == domains.length) {
          theEnd = true
          return
        }

        //We can increment. Return and reset the loop
        //Todo: use enums
        else if (domains(i).tryIncrement() == 2) {
          domains(i).increment()
          return
        }

        //If we cannot increment this domain, but we can increment the next parameter
        else if (domains(i).tryIncrement() == 1) {
          domains(i).reset()
          i += 1
        }

        loop
      }

      theEnd
    }

  } //fin class combinator

  /**
    * Always tranform the boolean condition to a set.
    * We use this with hypergraph vertex covering
    *
    * @param aa
    * @param ith
    * @return
    */
  def condToSET(aa: booleanCondition, ith: Int): ArrayBuffer[Int] = {
    var bitmap = new ArrayBuffer[Int]()

    //Si on a Rien, alors on retourne la totalité de l'ensemble
    if (aa.isInstanceOf[EmptyParam]) {
      for (i <- 0 until domainSizes(ith)) {
        bitmap += i
      }

      return bitmap
    }
    //Sinon, on y va avec les opérateurs

    val a = aa.asInstanceOf[booleanCond]

    if (a.operator == '=') {
      bitmap += (a.value)
    }

    else if (a.operator == '!') {
      for (i <- 0 until domainSizes(ith)) {
        if (i != a.value) bitmap += (i)
      }
    }

    else if (a.operator == '<') {
      for (i <- 0 until a.value)
        bitmap += (i)
    }

    //Else '>'
    else {
      for (i <- a.value + 1 until domainSizes(ith))
        bitmap += (i)
    }

    bitmap
  }

  /**
    * From a clause, we generate every possible test.
    * Every time, we use the boolean conds to generate the appropriate domain of values.
    * We also use the list of domain sizes for each parameter.
    * This is a global variable.
    *
    */
  def clauseToTests(clause: clause) = {

    //On fait les bitmap compressées a partir des conditions de la clause
    var ii = 0
    val domains = clause.conds.map(e => {
      val ret = condToSET(e, ii)
      ii += 1
      ret
    }).map(e => {
      domainParam(e,0)
    })

    val dd = combinator(domains)
    val tests = dd.generate()
    tests
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
          partition.foreach(clause => {
            val list = clauseToTests(clause)
            list.foreach(elem => {
              val key = elem
              if (hashmappp.get(key).isEmpty) //if entry is empty
                hashmappp.put(key, 1)
              else
                hashmappp(key) += 1
            })
          })
          hashmappp.toIterator
        }
        )

        var res = s1.reduceByKey((a, b) => a + b).collect()

        case class bt(var test: String = "", var count: Int = 0)
        var bestCount = 0
        var MTests = new ArrayBuffer[String]()
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
            val toKeep = (elem._1)
            MTests += toKeep
            Mpicks += 1
          }
        })

        println("Picking the tests")
        val chosenTests = greedyPicker(MTests)
        println(s"We have chosen " + chosenTests.size + " tests in this iteration using the greedy picker algorithm")

        //Quand on enleve 1000 tests a la fois, c'est trop long...
        //Il faut en enlever moins a la fois.
        currentRDD = progressive_filter(chosenTests, currentRDD, sc)

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

    val clauses = readPhiWayClauses(clausesFile)
    val number = clauses.length
    val clausesRDD = sc.makeRDD(clauses)

    println("Hypergraph Vertex Cover for Phi-way testing")
    println(s"Using a set of phiway clauses instead of interaction strength")
    println(s"Problem: $clausesFile with $number clauses")

    //println(s"Working with a Phi-way set of $number clauses")

    //Write to results.txt
    import java.io._
    val pw = new PrintWriter(new FileOutputStream(filename, true))

    var t1 = System.nanoTime()
    //Now we have the combos, we ship them directly to the setcover algorithm
    val tests = greedyalgorithm(sc, clausesRDD)
    var t2 = System.nanoTime()
    var time_elapsed = (t2 - t1).toDouble / 1000000000
    println(s"Hypergraph vertex cover time: $time_elapsed seconds")
    println("We have found " + tests.size + " tests")

    pw.append(s"$clausesFile;PHIWAY_HYPERGRAPH;$time_elapsed;${tests.size}\n")
    println(s"$clausesFile;PHIWAY_HYPERGRAPH;$time_elapsed;${tests.size}\n")
    pw.flush()

    //If the option to save to a text file is activated
    import cmdlineparser.TSPARK.save
    if (save == true) {
      println(s"Saving the test suite to a file named ${clausesFile}results.txt")
      //Save the test suite to file
      saveTestSuite(s"${clausesFile}results.txt", tests)
    }

    //Return the results
    tests
  }

  /**
    * Petit test rapide de la génération des tests a partir des clauses
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[1]")
      .setAppName("Phi-way hypergraph vertex covering")
      .set("spark.driver.maxResultSize", "0")
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")

    val tt = phiway_hypergraphcover("clauses1.txt", sc)
    tt.foreach(println)


  }

}
