/** This is a new combination generator for a t-way problem based on the IPOG algorithm
  * The variant we add however is that we parallelelize the combination generator
  * The parameter vector generation is distributed, and the value combination generation is distributed as well
  * Edmond La Chance UQAC 2019
  * *
  * Algorithms for solving are now included as well.
  * *
  * Cluster mode : Use spark-submit with Fat JAR, and use basic SparkContext()
  * *
  *
  * IPOG approach to store combos :
  * Assuming no object overhead, and no alignment.
  * *
  *1. A number of pointers to memory. One pointer per parameter vector combination
  *2. An array of bits for the value combinations.
  * *
  * Assuming a 32 bit system, so 32 bit pointers (4 bytes)
  * If it's 64 bit, the combos are twice as heavy.
  * (It would be better to have a full static table, instead of pointers). We would save pointer space.
  * But the problem is, if we have a variable number of parameters, the structures wouldn't have the same size.
  * A fix for this would be to make them all "maximum size".
  * *
  * If we have the problem : n=200, t=4, v=2
  * There are 200 C4 parameter vectors
  * *
  * -> 200 C4 = 64684950  * 4 = 258.7398 megs of pointers
  * -> 1034959200 value combinations. 1 bit per value combination -> 129 megs of memory.
  * *
  *
  * IPOG COLORING APPROACH to store combos :
  * *
  * We generate all the combos for the next parameter to cover.
  * Therefore, we only need to store a small part of the combos.
  * And these combos are generated and filtered directly in place, on the cluster.
  * 200 character per combo. Each character is 16 bit.
  * *
  * Biggest :
  * 200 C 3 * 2^4   = 21014400  * 200 characters * 2bytes per character =
  *8.4 gigs of distributed memory.
  * -> We can compress this.
  * *
  * TODO : Progressive graph coloring
  * TODO : un algorithme (one test at a time style AETG)
  * *
  * 1. Generate 1000 combos.
  * 2. Color graph
  * 3. Add 1000 combos
  * 4. Color graph
  * Etc, etc.
  * *
  * OR
  * *
  * Generate 10 000 combos
  * *
  * Color 1000 nodes
  * merge them
  * *
  * Color 1000 nodes
  * merge them
  * *
  * Take all the merge nodes, and color them. Return the graph.
  * *
  * TODO : Remplacer le système avec l'étoile pour les combos.
  * A la place, utiliser le système suivant :
  * Le caractère 0
  * *
  * TODO fix set cover... utiliser un set plus restreint de tests possibles pour chaque combos.
  * On crée une suite de tests... et on l'associe aux t-way combos. Ensuite on fait le Count.
  *
  *
  * * Here we implement an algorithm similar to the AETG algorithm. This algorithm covers the combos by adding one test at a time.
  * * https://ieeexplore.ieee.org/document/605761 for the algorithm description
  *
  *
  *
  */

package central

import ordercoloring.OrderColoring.orderColoring
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
import setcover.Algorithm2._
import scala.io.Source
//Import all enumerator functions
import enumerator.distributed_enumerator._
import progressivecoloring.progressive_coloring._
import utils._

//TODO supporter mixed strength, contraintes etc.
//Todo changer CHAR pour byte. Char = 16 bits, donc il y a du waste ici
//Todo ajouter support pour parameters avec un nombre variable de valeurs différentes. Faire ça avec un fichier
//Todo tester avec Kryro serialization, et tester un run avec JProfiler??
//Todo : expérimenter avec la compression de combos peut-être??

/*
Todo : une fonction pour accélérer la recherche. Une première binary search, pour trouver la première occurence du combo
Et ensuite recherche normale exhaustive sur le reste des combos
S'assurer que la recherche combo-combo est la plus rapide possible!
 */

//todo : faire une version SPARK de l'algorithme singlethreadcoloring.
//Il suffit après tout de broadcast la structure de voisinage, et de garder la couleur de chaque vertex localement.


//If lastPV is set to true, this means that this is the last step
case class _step(stepNo: Int, startingpv: Array[Char], nextpv: Array[Char],
                 lastPV: Boolean = false) {

  override def toString: String = {
    var chars = ""
    startingpv.foreach(f => chars += f)

    var chars2 = ""
    nextpv.foreach(f => chars2 += f)

    s"Step : $stepNo, Initial: $chars Next:$chars2  last=$lastPV\n"
  }
}

object gen extends Serializable {

  import utils._

  //Helpful global vars
  var save = false
  var debug = false
  var writeToFile = true //Write the results to a file
  var filename = "results.txt"

  /**
    * Compare two arrays. One array is a combo, and the other is a test
    * Both arrays should be the same size
    * Algorithm : Every element of the combo should be present in the other array
    * If this is the case, we return true. Else, we return false
    * *
    * If used with RDD.filter, invert the answer of this function in order to make remove the tested combos.
    * TODO : Optimiser pour un plus petit nombre de comparaisons -> t. Ici on compare tout le tableau quand même
    */
  def isComboHere[A](combo: IndexedSeq[A], test: Array[Char], t: Int): Boolean = {

    var i = 0
    var matched = 0
    var found = false

    def loop: Unit = {

      //Our exit condition
      if (matched == t) {
        found = true
        return
      }

      //Exit condition for end of array
      if (i == test.length) return

      if (combo(i) == '*') {
      }

      //Just added this condition, should go a little faster.
      else if (combo(i) != test(i)) return

      else {
        if (combo(i) == test(i)) {
          matched += 1
        }
      }
      i += 1
      loop
    }

    loop

    //Return found true or false
    found
  }


  /**
    * This function is used by setcover_m_progressive and other variants.
    *
    * @param combo
    * @param test
    * @return
    */
  def isComboHere2(combo: Array[Char], test: Array[Char], t: Int): Boolean = {

    var i = 1 //iterator for combo
    var j = 0 //iterator for test
    var matched = 1
    var found = false

    def loop: Unit = {

      //If everything has been matched, we can return
      if (matched == t) {
        found = true
        return
      }

      //Exit condition for end of array
      if (j == test.length) return

      if (combo(i) == '*') {
      }

      //Just added this condition, should go a little faster.
      else if (combo(i) != test(j)) return

      else {
        if (combo(i) == test(j)) {
          matched += 1
        }
      }

      i += 1 //increase counter for combo
      j += 1 //increase counter for test
      loop
    }

    loop

    //Return found true or false
    found
  }

  /**
    * We use this function with RDD.filter
    * Algorithm : a given combo is compared to all other tests in the broadcasted array of tests
    * TODO : make it faster
    */
  def filter_combo[A](combo: IndexedSeq[A], bv: Broadcast[Array[Array[Char]]], t: Int): Boolean = {

    //Search all the broadcasted tests to see if the combo is there
    var i = 0
    var end = bv.value.length
    var returnValue = false
    val tests = bv.value

    def loop(): Unit = {
      if (i == end) return
      if (isComboHere(combo, tests(i), t)) {
        returnValue = true
        return
      }
      i += 1
      loop
    }

    loop

    !returnValue
  }


  /**
    * Here we execute a simple setcover algorithm
    */
  def simple_setcover(n: Int, t: Int, v: Int, sc: SparkContext, vstep: Int = -1): Array[Array[Char]] = {

    val expected = utils.numberTWAYCombos(n, t, v)
    println("Simple Set Cover algorithm (Greedy Random algorithm)")
    println(s"vstep is $vstep")
    println(s"Problem : n=$n,t=$t,v=$v")
    println(s"Expected number of combinations is : $expected ")
    println(s"Formula is C($n,$t) * $v^$t")

    var t1 = System.nanoTime()
    val steps = generate_all_steps(n, t)
    val r1 = sc.makeRDD(steps) //Parallelize the steps
    val r2 = r1.flatMap(step => generate_from_step(step, t)) //Generate all the parameter vectors
    var testsRDD = r2.flatMap(pv => generate_vc(pv, t, v)) //Generate the tway combos

    //Write to results.txt
    import java.io._
    val pw = new PrintWriter(new FileOutputStream(filename, true))

    println("There are " + testsRDD.count() + " combos")

    var t2 = System.nanoTime()
    var time_elapsed = (t2 - t1).toDouble / 1000000000
    println(s"Generation time : $time_elapsed seconds")

    t1 = System.nanoTime()
    //Now we have the combos, we ship them directly to the setcover algorithm

    val result = if (vstep == 1) setcover.Algorithm2.greedy_algorithm2(sc, v, testsRDD)
    else setcover.Algorithm2.greedy_setcover_buffet(sc, v, testsRDD, vstep)

    t2 = System.nanoTime()
    time_elapsed = (t2 - t1).toDouble / 1000000000
    println(s"Set cover time : $time_elapsed seconds")
    println("We have found " + result.size + " tests")


    pw.append(s"$t;$n;$v;HYPERGRAPH_SETCOVER;$time_elapsed;${result.size}\n")
    println(s"$t;$n;$v;HYPERGRAPH_SETCOVER;$time_elapsed;${result.size}\n")
    pw.flush()

    //If the option to save to a text file is activated
    if (save == true) {
      println(s"Saving the test suite to a file named $t;$n;$v.txt")
      //Save the test suite to file
      saveTestSuite(s"$t;$n;$v.txt", result)
    }

    //Return the results
    result
  }

  /**
    * We have a specialized merge function here
    * A is a test, B is a combo that we want to merge inside A.
    * B IS MERGED INSIDE A
    * In order for the merge to be successful, we need to move every character from b inside A.
    * We can move inside a star, or the same character
    *
    * B and A have the same length.
    *
    * Returns a new test object (clone). Or None if we cannot merge.
    *
    * @param a TEST with stars
    * @param b COMBO
    * @return TEST with less stars
    */
  def mergeForSetCover(a: Array[Char], b: Array[Char], t: Int = Int.MaxValue): Option[String] = {

    var matchedCharacters = 0
    var i = 0
    var matched = true //we put to false when we encounter a problem
    var c = a.clone()

    loop

    def loop(): Unit = {

      if (matchedCharacters == t) return //we have matched enough characters
      if (i == a.length) return //check for endstring

      val comboChar = b(i)
      val testChar = a(i)

      if (comboChar != '*') {

        if (testChar != '*' && testChar != comboChar) { //little exit condition here.
          matched = false
          return
        }

        if (comboChar == testChar || testChar == '*') {
          c(i) = b(i) //write to result test
          matchedCharacters += 1
        }

      }

      i += 1 //move to next character
      loop
    }

    if (matched == true)
      Some(utils.arrayToString(c))
    else None
  }

  /**
    * Here we verify a test suite to see if it covers a given problem correctly.
    *
    * @param testSuite
    * @param combos
    * @return
    */
  def verifyTestSuite(testSuite: Array[Array[Char]], combos: RDD[Array[Char]], sc: SparkContext): Boolean = {

    val broadcasted_tests = sc.broadcast(testSuite)
    var filtered = combos

    //Find out what the t value is
    var t = findTFromCombo(combos.first())

    //Broadcast and filter using these new tests
    //time {
    println("Broadcasting the tests, and filtering them")
    val bv = sc.broadcast(broadcasted_tests)
    filtered = filtered.filter(combo => filter_combo(combo, broadcasted_tests, t)).cache()
    //println("Number of combos after filter " + unmerged.count())
    // }

    if (filtered.isEmpty()) true
    else false
  }

  /**
    * The problem with the filter combo technique is that it can be very slow when the test suite becomes very large.
    * It is better to filter using a fixed number of tests at a time. Otherwise we will be testing the same combos multiple times.
    *
    * @param testSuite
    * @param combos
    */
  def progressive_filter_combo(testSuite: Array[Array[Char]], combos: RDD[Array[Char]],
                               sc: SparkContext, speed: Int = 500): RDD[Array[Char]] = {
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

      println("Number of combos after filter " + filtered.count())

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


  /**
    * Generate every version of a test according to the v parameter
    * This is used in the horizontal extension optimization
    * This does not work on
    */
  def testVersions(combo: Array[Char], v: Int): ArrayBuffer[Array[Char]] = {
    var results = new ArrayBuffer[Array[Char]]()
    for (i <- 0 to v) {
      val valuee = i + '0'
      val res = growby1(combo, valuee.toChar)
      results += res
    }
    results
  }

  /**
    * This is a second version of the function. It fills in values for all the missing stars.
    * Basically, it does even more work.
    * Call this function when there are more stars in the test
    *
    * @param combo
    * @param v
    * @return
    */
  def testVersions2(combo: Array[Char], v: Int): ArrayBuffer[Array[Char]] = {
    var results = new ArrayBuffer[Array[Char]]() //all the test versions are here.

    //For each star, we generate all versions.
    //Go over the test, and note where all the stars are...
    var starsPositions = ArrayBuffer[Int]()

    var counter = 0
    for (i <- combo) {
      if (i == '*') starsPositions += counter
      counter += 1
    }

    if (starsPositions.size == 0) return results

    var test = combo.clone()
    var j = 0 //index of a star position
    def loop(j: Int): Unit = {
      //If we have reached the last position, we can emit
      if (j == starsPositions.size) {
        results += test.clone()
        return
      }

      //Assign the value here.
      for (i <- 0 until v) {
        test(starsPositions(j)) = (i + '0').toChar
        loop(j + 1)
      }
    }

    loop(0)

    results
  }


  /**
    * Takes a test suite that contains missing values, and remaining combos, and uses a graph coloring algorithm.
    *
    * @param tests
    * @return
    */
  def updateTestColoring(tests: Array[Array[Char]], combos: RDD[Array[Char]], sc: SparkContext, v: Int, numberProcessors: Int = 6): Array[Array[Char]] = {

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

    var coloredTests = orderColoring(numberProcessors, input, sc)

    coloredTests ++ testsWithoutStars
  }


  /**
    * Version finale. Mise au propre pas mal
    *
    * @param tests
    * @param combos
    * @param v
    * @param t
    * @param sc
    * @return
    */
  def horizontalgrowthfinal(tests: Array[Array[Char]], combos: RDD[Array[Char]], v: Int,
                            t: Int, sc: SparkContext, hstep: Int = -1):
  (Array[Array[Char]], RDD[Array[Char]]) = {

    var finalTests = new ArrayBuffer[Array[Char]]()
    case class key_v(var test: Int, version: Char)
    var newCombos = combos

    //Start M at 1% of total test size
    var m = tests.size / 100
    if (m < 1) m = 1
    var i = 0 //for each test

    //Set the M value from the static value if there was one provided.
    if (hstep != -1) m = hstep

    loop2 //go into the main loop
    def loop2(): Unit = {

      var newTests = new ArrayBuffer[Array[Char]]()

      println(s"value of M : $m for value of i =  $i")

      //Exit condition of the loop
      if (i >= tests.length) {
        return
      }

      //Pick the M next tests
      var someTests = takeM(tests, m, i)
      if (someTests.size < m) m = someTests.size

      //Broadcast the tests
      val someTests_bcast = sc.broadcast(someTests)

      val s1 = newCombos.mapPartitions(partition => {
        var hashmappp = scala.collection.mutable.HashMap.empty[key_v, Int]
        partition.foreach(combo => {
          val someTests = someTests_bcast.value
          var list = new ArrayBuffer[key_v]()
          //Get the version of the combo
          val c = combo(0)

          //If a test in someTests covers this combo, we put it inside a list
          for (j <- 0 until someTests.size) {
            val answer = isComboHere2(combo, someTests(j), t)
            if (answer == true) { //if true, we add this version to the possible tests
              list += key_v(j, c)
            }
          }

          //Put the list of possible tests inside the hash table
          list.foreach(elem => {
            if (hashmappp.get(elem).isEmpty) //if entry is empty
              hashmappp.put(elem, 1)
            else {
              hashmappp(elem) += 1
            }
          })
        })
        Iterator(hashmappp)
      })

      //Unpersist the broadcast variable
      someTests_bcast.unpersist(false)

      //TODO: Optimize this part

      //Final aggregation of the counts using reduceByKey
      var res = s1.flatMap(hash => hash.toSeq).reduceByKey((a, b) => a + b)

      //This part should be done on a local machine for speed.

      //Find the best version of the tests
      val res2 = res.map(e => Tuple2(e._1.test, (e._1.version, e._2))).reduceByKey((a, b) => {
        if (a._2 > b._2) a else b
      }).collect()

      //Add all of these as new tests
      for (i <- 0 until res2.size) {
        val id = res2(i)._1
        val version = res2(i)._2._1
        val testMeat = someTests(id)
        val newTest = growby1(testMeat, version)
        newTests += newTest
      }

      //Delete combos using the new tests
      newCombos = progressive_filter_combo(newTests.toArray, newCombos, sc, 500)

      //Build a list of tests that did not cover combos
      for (i <- 0 until someTests.size) {
        var found = false
        loop

        def loop(): Unit = {
          for (k <- 0 until res2.size) {
            if (res2(k)._1 == i) {
              found = true
              return
            }
          }
        }

        //Add the test, with a star
        if (found == false) {
          val testMeat = someTests(i)
          val newTest = growby1(testMeat, '*')
          newTests += newTest
        }
      }

      newCombos = newCombos.localCheckpoint()
      finalTests = finalTests ++ newTests //Concatenate the array into final tests

      i += m
      loop2
    }
    //Now we return the results, and also the uncovered combos.
    (finalTests.toArray, newCombos)
  } //fin fonction horizontal growth 1 percent old


  /**
    * We have a combo, and a few tests. We have to generate a tuple combo,test
    * Also include the root of the test in the result
    */
  def combo_and_tests(combo: Array[Char], someTests: ArrayBuffer[Array[Char]]) = {
    var mergedTest = ""
    var root = 0
    var i = 0

    loop

    def loop(): Unit = {
      if (i == someTests.size) return

      val res = mergeForSetCover(someTests(i), combo)
      if (!res.isEmpty) {
        mergedTest = res.get
        root = i
        return
      }

      i += 1
      loop
    }

    if (mergedTest != "")
      (combo, Some(root, mergedTest))
    else (combo, None)
  }


  /**
    * Takes a test suite that contains missing values, and remaining combos, and uses a graph coloring algorithm.
    * m is the speed for the set cover algorithm
    *
    * @param tests
    * @return
    */
  def updateTestsSetCover(tests: Array[Array[Char]], combos: RDD[Array[Char]],
                          sc: SparkContext, v: Int, vstep: Int = 1): Array[Array[Char]] = {

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

    //We have two different set cover algorithms, depending on the value of M
    var filledTests =
      if (vstep == 1) greedy_algorithm2(sc, v, sc.makeRDD(input))
      else greedy_setcover_buffet(sc, v, sc.makeRDD(input), vstep)

    filledTests ++ testsWithoutStars
  }




  /**
    * The distributed graph coloring algorithm
    *
    * @param n
    * @param t
    * @param v
    * @param sc
    * @return
    */
  def distributed_graphcoloring(n: Int, t: Int, v: Int, sc: SparkContext,
                                memory: Int = 4000, algorithm: String = "OrderColoring"): Array[Array[Char]] = {
    val expected = utils.numberTWAYCombos(n, t, v)
    println("Distributed Graph Coloring")
    println(s"Using memory = $memory megabytes and algorithm = $algorithm")
    println(s"Problem : n=$n,t=$t,v=$v")
    println(s"Expected number of combinations is : $expected ")
    println(s"Formula is C($n,$t) * $v^$t")

    import java.io._
    val pw = new PrintWriter(new FileOutputStream(filename, true))

    var t1 = System.nanoTime()

    val tests = progressivecoloring_final(fastGenCombos(n, t, v, sc), sc, memory, algorithm) //4000 pour 100 2 2

    var t2 = System.nanoTime()
    var time_elapsed = (t2 - t1).toDouble / 1000000000

    pw.append(s"$t;$n;$v;GRAPHCOLORING;$time_elapsed;${tests.size}\n")
    println(s"$t;$n;$v;GRAPHCOLORING;$time_elapsed;${tests.size}\n")
    pw.flush()

    //If the option to save to a text file is activated
    if (save == true) {
      println(s"Saving the test suite to a file named $t;$n;$v.txt")
      //Save the test suite to file
      saveTestSuite(s"$t;$n;$v.txt", tests)
    }

    //Return the test suite
    tests
  }

  /**
    * Just extend the test suite for one parameter perfectly.
    * Waiting on the theorem. Soon!
    *
    * @param tests
    */
  def extendOrthogonal(tests: Array[Array[Char]]) = {


  }

  /**
    * Todo : code the duplication method for covering arrays of strength 3
    */
  def duplicationMethod(): Unit = {


  }


  /**
    * Reads a file that contains a hypergraph, in edn format
    * Returns an array buffer that represents an hypergraph.
    * The nodes are strings, not integers
    *
    * @param filename
    */
  def read_edn_hypergraph(filename: String) = {

    var hypergraph = new ArrayBuffer[Array[String]]()

    for (line <- Source.fromFile(filename).getLines()) {
      val res = parseLine(line)
      if (!res.isEmpty) hypergraph += res.get
    }


    //Just parse a single line of text
    def parseLine(line: String): Option[Array[String]] = {
      var ptr = line

      var i = 0
      if (line(0) != '"') return None
      i += 1

      while (line(i) != '{') {
        i += 1
      }
      i += 1

      ptr = line.substring(i)
      ptr = ptr.replace("}", " ")
      ptr = ptr.replace(",", " ")
      val tokens = ptr.split(" ")
      Some(tokens)
    }

    hypergraph

  }

  /**
    * This function handles the process of solving a hypergraph problem found in a .edn hypergraph file
    *
    * @param filename
    */
  def solve_edn_hypergraph(filename: String, sc: SparkContext): Unit = {
    import edn.edn_hypergraphs._
    import java.io._
    val pw = new PrintWriter(new FileOutputStream("edn.txt", true))

    println(s"Starting the hypergraph vertex cover algorithm for problem $filename")
    val a = read_edn_hypergraph(filename)
    val rdd = sc.makeRDD(a)

    var t1 = System.nanoTime()
    val size = hypergraph_edn(sc, rdd)
    var t2 = System.nanoTime()
    var time_elapsed = (t2 - t1).toDouble / 1000000000

    println(s"Covered the hypergraph with $size vertices")
    pw.append(s"$filename;HYPERGRAPH_EDN;$time_elapsed;$size\n")
    println(s"$filename;HYPERGRAPH_EDN;$time_elapsed;$size\n")
    pw.flush()
  }


  /**
    * This is a sequential coloring algorithm. It is extremely quick, which makes me a bit sad.
    * Algorithm :
    * ->Generate combos.
    * ->Combos to Graph Coloring
    * ->Generate a random coloring sequence.
    * ->Color the sequence in a single while loop
    * ->Return the graph in the following format (color, original combo text)
    */
  def singlethreadcoloring(n: Int, t: Int, v: Int, sc: SparkContext, numberProcessors: Int = 6): Array[Array[Char]] = {

    val expected = utils.numberTWAYCombos(n, t, v)
    println("Graph Coloring single thread (Order Coloring)")
    println(s"Number of parallel graph colorings : $numberProcessors")
    println(s"Problem : n=$n,t=$t,v=$v")
    println(s"Expected number of combinations is : $expected ")
    println(s"Formula is C($n,$t) * $v^$t")

    var t1 = System.nanoTime()

    import java.io._
    val pw = new PrintWriter(new FileOutputStream(filename, true))

    //Step 1 : Cover t parameters
    val steps = generate_all_steps(n, t)
    val r1 = sc.makeRDD(steps) //Parallelize the steps
    val r2 = r1.flatMap(step => generate_from_step(step, t)) //Generate all the parameter vectors
    var testsRDD = r2.flatMap(pv => generate_vc(pv, t, v)).cache() //Generate the tway combos

    var t2 = System.nanoTime()
    var time_elapsed = (t2 - t1).toDouble / 1000000000
    println(s"Generation time : $time_elapsed seconds")

    t1 = System.nanoTime()
    var tests = orderColoring(numberProcessors, testsRDD.collect(), sc)

    t2 = System.nanoTime()

    time_elapsed = (t2 - t1).toDouble / 1000000000
    println(s"SIMPLE COLORING TIME : $time_elapsed seconds")

    //Record the results into the file in append mode
    pw.append(s"$t;${n};$v;STCOLORING;$time_elapsed;${tests.size}\n")
    println(s"$t;${n};$v;STCOLORING;$time_elapsed;${tests.size}\n") //print it too
    pw.flush()

    tests
  }


} //end class gen

object test3 extends App {
  //Todo : utiiliser ca :  .set("spark.local.dir", "/media/data/") //The 4TB hard drive can be used for shuffle files
  //todo : essayer de faire la couverture de M tests en moins d'étapes. Ça sauverait de la mémoire, et du temps.
  //Todo : enlever cache dans toutes les fonctions et juste remplacer par localCheckpoint au bon endroit?

  //Todo : file channel sont les memory mapped files en java. Regarder ça.
  //https://docs.oracle.com/javase/10/docs/api/java/nio/channels/FileChannel.html
  //https://en.wikipedia.org/wiki/Memory-mapped_file
  //todo tester n=100 t=2 v=2 avec IPOG Set Cover

  //todo compresser les adjlists de coloring en un BitSet peut etre?

  //todo essayer plusieurs types de garbage collector pour Java8
  //voir https://docs.oracle.com/javase/8/docs/technotes/guides/vm/gctuning/collectors.html

  import gen._

  val conf = new SparkConf().setMaster("local[1]").setAppName("Combination generator").set("spark.driver.maxResultSize", "0")
    .set("spark.checkpoint.compress", "true")
  val sc = new SparkContext(conf)
  sc.setLogLevel("OFF")

  // solve_edn_hypergraph("test2.edn", sc)

  //  import graphviz.graphviz_graphs._
  //  val a = graphcoloring_graphviz("petersen.txt", sc, 100, "OC")


  //var t = 3
  //  var n = 6
  //  var v = 5
  //
  //val tests = simple_setcover(n,t,v,sc, -1)
  //   println("\n\nVerifying test suite ... ")
  //    println(verifyTestSuite(tests, fastGenCombos(n, t, v, sc), sc))


  //runTestSuiteColoring(3,2,2,6,20,4,sc)
  //val tests = runTestSuite(2,2,7,20,4,sc)

  //  val v = 3
  //  val test = Array('*','1','0','*')
  //  testVersions2(test, v)

  //var n = 10
  //  var t = 7
  //  var v = 4
  //
  // val tests =  simple_setcover(n,t,v,sc)
  //println("We have " + tests.size + " tests")
  // println("Printing the tests....")
  // tests foreach (utils.print_helper(_))


  import progressivecoloring.progressive_coloring._

  //
  var n = 3
  var t = 2
  var v = 2

  val tests = distributed_graphcoloring(n, t, v, sc, 500, "KP") //4000 pour 100 2 2
  println("We have " + tests.size + " tests")
  println("Printing the tests....")
  tests foreach (utils.print_helper(_))

  //  val tests = parallel_ipogm_setcover(n,t,v,sc)
  //    println("We have " + tests.size + " tests")
  //    println("Printing the tests....")
  //    tests foreach (utils.print_helper(_))
  ///
  //Problème ici....
  //  val tests = newipogcoloring(n, t, v, sc, 6)
  //  println("We have " + tests.size + " tests")
  //  println("Printing the tests....")
  //  tests foreach (utils.print_helper(_))

  // val ts = readTestSuite("7-8-4-testsuite.txt")

  // println("\n\nVerifying test suite ... ")
  //  println(verifyTestSuite(tests, fastGenCombos(n, t, v, sc), sc))

  //
  //        var remainingCombos = progressive_filter_combo(tests, fastGenCombos(n,t,v,sc), sc)
  //        println("Remaining combos : ")
  //        remainingCombos.collect().foreach( utils.print_helper(_))

}

