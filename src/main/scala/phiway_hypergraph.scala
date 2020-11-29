import central.gen.filename
import hypergraph_cover.Algorithm2.progressive_filter_combo_string
import hypergraph_cover.greedypicker.greedyPicker
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import utils.utils.{arrayToString, stringToArray}
import phiway.phiway._

import scala.collection.mutable.ArrayBuffer

object phiway_hypergraph extends Serializable {

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
      for (i <- domains) {
        output += i
      }
      output
    }

    /**
      * Incrémenter la structure
      * L'algorithme termine quand tous les domaines ont leur valeur max.
      *
      */


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
        //currentRDD = progressive_filter_combo_string(chosenTests.toArray, currentRDD, sc)

        //Add the M tests to logEdgesChosen
        //  chosenTests.foreach(test => {
        //     logEdgesChosen += test
        //   })

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

    val tt = phiway_hypergraphcover("clauses4.txt", sc)
    tt.foreach(println)


  }

}
