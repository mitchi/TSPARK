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

package generator

import cmdline.resume_info
import generator.gen.filename
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
import setcover.Algorithm2._
import scala.annotation.tailrec
import scala.util.Random
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable
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
    * Run a test suite for IPOG algorithms
    * Used locally
    */
  def runTestSuite(initialT: Int, initialV: Int, maxT: Int, maxN: Int,
                   maxV: Int, sc: SparkContext, algorithm: String = "parallel_ipog",
                   numberProcessors: Int = 6, hstep: Int = -1, vstep: Int = -1): Unit = {
    //T
    for (t <- initialT to maxT) {
      //vary v
      for (v <- initialV to maxV) {
        algorithm match {
          case "parallel_ipog" => newipogcoloring(maxN, t, v, sc, numberProcessors)
          case "parallel_ipog_m" => parallel_ipogm(maxN, t, v, sc, numberProcessors, hstep)
          case "parallel_ipog_m_setcover" => parallel_ipogm_setcover(maxN, t, v, sc, hstep, vstep, None)
        }
      }
    }
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
    * Here the idea is to use the set cover algorithm to perform an optimized horizontal extension.
    *
    * First, we take the test suite, and we extend every test with every possible value of its domain for the new parameter.
    * When using domain sizes based on v, we simply generate v tests for every test.
    * If v=4, this will quadruple our new number of tests. But it's ok.
    *
    * Then, what we do is we map every combo to every fitting test.
    * the RDD will look like this : combo line : list of tests.
    * The list of tests can be encoded into integers + integer compression if the list of tests stays little.
    * It can also be generated on the fly for every combo if we keep the broadcasted list of tests on hand.
    *
    * Then, we do a distributed count.
    *
    * The algorithm ends when all the combos that can be covered, are covered.
    *
    * .....................................
    * For every combo, we generate V versions of the test, and we test to see if they are compatible.
    * But first, we should check if this combo has this parameter active or not. If it doesn't, it makes
    * our life easier and we can stop right now. No hard work.
    *
    * Check if the combo can be alive in the test. Write a function for this. Just check for the nth character.
    *
    *
    * For each test, and their v variants, we check if they can cover the combo. If they do, we write them on the list.
    * Every combo will be an object.  This object contains the original combo, and the list of test variants that covers it.
    *
    * We take the combos that have empty lists and save them. These combos will be used in the vertical extension.
    *
    *
    * Idee : garder le numero, pour chaque test choisi, du test original? Ou alors, trier tout et remplacer le test qui fit le plus.
    *
    * @param tests
    * @param v
    * @param sc
    * @return
    */
  def setcover_horizontalextension(tests: Array[Array[Char]], combos: RDD[Array[Char]], v: Int, t: Int, sc: SparkContext):
  (Array[Array[Char]], RDD[Array[Char]]) = {
    //Take our test suite and broadcast it.
    val broadcasted_tests = sc.broadcast(tests)

    //    println("Printing the combos before ")
    //    combos.collect.foreach(print_helper(_))
    //    println("***************")

    //Every combo can now contain tests

    var t1 = System.nanoTime()

    var newCombos = combos.map(combo => {
      var possibleTests = new ArrayBuffer[(Array[Char], Int)]()

      //Go through all tests, and put valid tests in the possibleTests data structure
      for (i <- 0 until broadcasted_tests.value.length) {
        val test = broadcasted_tests.value(i)

        //Is this a test that contains stars (missing values)?
        var stars = if (test.contains('*'))
          true
        else
          false

        //Generate all possible versions of this test case
        var versions = testVersions(test, v)

        //Check for compatibility
        for (j <- 0 until versions.size) {
          val answer = isComboHere(combo, versions(j), t)
          if (answer == true) { //if true, we add this version to the possible tests

            if (stars == false) {
              possibleTests += Tuple2(versions(j), i) //we also include the test root as well
            }
            else {
              val merged = mergeTests(versions(j), combo).get
              possibleTests += Tuple2(merged, i)
            }
          }
        }
      }
      //Try every test with this combo
      //result is a tuple : (combo, list of tests)
      (combo, possibleTests)

    })
    newCombos.cache()

    println("There are " + newCombos.count() + "combos")
    var t2 = System.nanoTime()
    println("Elapsed time for generating the possible tests: " + (t2 - t1).toDouble / 1000000000 + " seconds")


    //    newCombos.collect().foreach(  e => {
    //      print_helper(e._1)
    //      val list =  for (i<-e._2) yield utils.print_helper2(i._1, " root: " + i._2.toString)
    //      print(s"List : $list")
    //      println("---")
    //    })

    var bestTests = new ArrayBuffer[(String, Int)]()

    var j = 0 //counter for the test we choose to cover
    var remainingElements = false
    var uncoveredRoots = new ArrayBuffer[Int]()

    //Now we choose the best tests to cover our combos
    loop

    def loop(): Unit = {

      newCombos = newCombos.localCheckpoint() //checkpoint for speed

      //      println(s"There are ${newCombos.count()} combos remaining")
      //      println(s"***************ITERATION $j **************************")

      //Stop the algorithm when we have extended every existing test
      if (j == tests.length)
        return

      t1 = System.nanoTime()

      //Choose the most frequent test, for the given root
      val s1 = newCombos.flatMap(elem => {
        val strings = for (i <- elem._2; if (i._2 == j))
          yield (utils.arrayToString(i._1), (i._2, 1)) //the accumulator is the last element of the tuple. This is important.
        strings
      })

      println("s1.count" + s1.count())

      t2 = System.nanoTime()
      println("Elapsed time for the flatMap " + (t2 - t1).toDouble / 1000000000 + " seconds")

      //Check for empty collection.This means that there are no more combos for that root.
      if (s1.isEmpty()) {
        // println("The collection is empty for this root. Moving to the next root ")
        remainingElements = true
        return
      }

      t1 = System.nanoTime()

      val s2 = s1.reduceByKey((a, b) => {
        (a._1, a._2 + b._2)
      })

      //      println(s"Now we want to select the best test version for root $j")
      //      println(s"Printing the flatmap + reducebyKey results for the root:$j")
      //      s2.collect().foreach( println)

      //If we want greedy random, we can choose from a list here. From now, it won't be greedy random.
      val best: (String, (Int, Int)) = s2.reduce((a, b) => {
        if (a._2._2 > b._2._2) a
        else b
      })

      t2 = System.nanoTime()
      println("Elapsed time for reduceByKey + reduce " + (t2 - t1).toDouble / 1000000000 + " seconds")

      //      println("Best test chosen is " + best._1)
      //      println("Now removing every combo that contains the test "+ best._1+" and the root "+best._2._1)

      //Transformer en Array[Char] après.
      //Mettre dans notre liste des tests choisis.
      bestTests += Tuple2(best._1, best._2._1)

      //      println("Printing before removing: ")
      //      newCombos.collect().foreach(  e => {
      //        print_helper(e._1)
      //        val list =  for (i<-e._2) yield utils.print_helper2(i._1, " root: " + i._2.toString)
      //        print(s"List : $list")
      //        println("---")
      //      })

      //Remove combos in the RDD that contain this test

      t1 = System.nanoTime()

      newCombos = newCombos.flatMap(elem => {
        val list = elem._2
        var answer = false

        loop

        def loop(): Unit = {
          for (i <- list) {
            if (utils.arrayToString(i._1) == best._1) {
              //print( "equal : "+ utils.arrayToString(i._1) + " " + best._1 + "\n")
              answer = true
              return
            }
            //            //Other way to destroy a combo : we have already chosen this root.
            //            if (i._2 == best._2._1)
            //            {
            //              answer = true
            //              return
            //            }
          }
        }

        if (answer == true) None
        else Some(elem)
      })

      println("Count after removing " + newCombos.count())
      t2 = System.nanoTime()
      println("Elapsed time for removing shit" + (t2 - t1).toDouble / 1000000000 + " seconds")


      //      println("Printing after removing")
      //      newCombos.collect().foreach(  e => {
      //        print_helper(e._1)
      //        val list =  for (i<-e._2) yield utils.print_helper2(i._1, " root: " + i._2.toString)
      //        print(s"List : $list")
      //        println("---")
      //      })


      j += 1 //Go to the next test

      loop
    } //end of big loop function

    //Add all the tests of the proper root to the test suite. Add a * at the end, because they were not needed to cover anything this time
    var otherTests = new ArrayBuffer[Array[Char]]()

    if (remainingElements == true) {
      for (i <- j until tests.size) {
        val newTest = growby1(tests(i), '*')
        //print("On ajoute le test : ")
        //print_helper(newTest)
        otherTests += newTest
      }
    }

    //Transform to string
    var testsToArray = for (i <- bestTests) yield utils.stringToArray(i._1)
    testsToArray = otherTests ++ testsToArray

    //Now we return the results, and also the uncovered combos.
    (testsToArray.toArray, newCombos.map(e => e._1))
  }


  /**
    * We sort the test suite by putting the tests with the stars first.
    *
    * @param tests
    * @return
    */
  def sortForStars(tests: Array[Array[Char]]): Array[Array[Char]] = {
    import scala.collection.mutable.ListBuffer

    var list = new ListBuffer[Array[Char]]()

    for (i <- tests) {
      if (i.contains('*)) { //if the test contains a star
        list.prepend(i)
      }
      else list.append(i)
    }
    return list.toArray
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

    //    println("Printing the incomplete tests : ")
    //    incompleteTests.foreach(print_helper(_))

    //Do the contrary here. Remove those we have just added from the original test set. This could be more optimal but would need custom code I guess.
    var testsWithoutStars = tests.flatMap(arr => {
      if (containsStars(arr) == true) {
        None
      }
      else Some(arr)
    })
    //    println("Printing the complete tests : ")
    //    testsWithoutStars.foreach(print_helper(_))

    //var input = combos.union(sc.makeRDD(incompleteTests))
    var input = combos.collect union incompleteTests

    //var coloredTests = parallelLittleColoring(input, sc)
    var coloredTests = orderColoring(numberProcessors, input, sc)

    coloredTests ++ testsWithoutStars
  }


  /**
    * Not an adapative version of M. We always aim to do 100 iterations of horizontal growth
    * We calculate the M value using 1% of the test size.
    * We can also receive a M value by parameter.
    *
    * @param tests
    * @param combos
    * @param v
    * @param t
    * @param sc
    * @return
    */
  def horizontalgrowth_1percent(tests: Array[Array[Char]], combos: RDD[Array[Char]], v: Int,
                                t: Int, sc: SparkContext, hstep: Int = -1):
  (Array[Array[Char]], RDD[Array[Char]]) = {
    //Take our test suite and broadcast it.

    var bestTests2 = new ArrayBuffer[Array[Char]]()
    var remainingElements = false

    case class element(var test: Int, version: Char)
    case class comboEntry(var combo: Array[Char], var list: ArrayBuffer[element])

    var newCombos = combos

    //Start M at 1% of total test size
    var m = tests.size / 100
    if (m < 1) m = 1
    var i = 0 //for each test

    //Set the M value from the static value if there was one provided.
    if (hstep != -1) m = hstep

    loop2 //go into the main loop
    def loop2(): Unit = {

      println(s"value of M : $m for value of i =  $i")

      //Exit condition of the loop
      if (i >= tests.length) {
        remainingElements = false
        return
      }

      //Pick the M next tests
      var someTests = takeM(tests, m, i)
      if (someTests.size < m) m = someTests.size

      //Broadcast the tests
      val someTests_bcast = sc.broadcast(someTests)

      case class key_v(var test: Int, version: Char)

      val s1 = newCombos.mapPartitions(partition => {
        var hashmappp = scala.collection.mutable.HashMap.empty[key_v, Int]
        partition.foreach(combo => {
          //Create the list on the fly for the element. This will save memory
          val someTests = someTests_bcast.value
          //Get the version of the combo
          var list = new ArrayBuffer[element]()
          val c = combo(0)

          //Create a small list
          for (j <- 0 until someTests.size) {
            val answer = isComboHere2(combo, someTests(j), t)
            if (answer == true) { //if true, we add this version to the possible tests
              list += element(j, c)
            }
          }

          //Put the list inside the hash table
          list.foreach(elem => {
            val test = elem.test
            if (test != -1) { //ignore
              val key = key_v(test, elem.version)
              if (hashmappp.get(key).isEmpty) //if entry is empty
                hashmappp.put(key, 1)
              else {
                hashmappp(key) += 1
              }
            }
          })
        })
        Iterator(hashmappp)
      })

      //Unpersist it right away
      someTests_bcast.unpersist(false)

      //Fold would be better here
      //Voir le StackOverflow pour merge des maps https://stackoverflow.com/questions/7076128/best-way-to-merge-two-maps-and-sum-the-values-of-same-key
      var res = s1.fold(mutable.HashMap.empty[key_v, Int])(
        (acc, values) => {
          values.foreach(elem => { //for each element of a
            val key: key_v = elem._1
            val value = elem._2
            if (acc.get(key).isEmpty) acc(key) = elem._2
            else acc(key) += value
          })
          acc
        })

      //When our M tests do not cover any combos, we still need to add the tests, but we add them with a star. The graph coloring
      //Will have to take care of it later.
      if (res.isEmpty == true) {
        for (i <- 0 until someTests.size) {
          val meat = someTests(i)
          val newTest = growby1(meat, '*')
          bestTests2 += newTest
        }

        remainingElements = true
        return
      }

      //Find the best test inside the hashtable.
      var bestTests = new ArrayBuffer[bestT]() //initiate array of size m. Index of array is the test.
      //Init that array
      for (k <- 0 until m) {
        bestTests += bestT()
      }

      //Find the best versions for each test, from the merged hash table
      res.toSeq.foreach(elem => {
        val i: Int = elem._1.test
        val count = elem._2
        val version = elem._1.version
        if (count > bestTests(i).count) {
          bestTests(i).count = count
          bestTests(i).version = version
        }
      })

      //Assemble the final test using the original test and the version
      for (i <- 0 until bestTests.size) {
        val meat = someTests(i)
        val newTest = growby1(meat, bestTests(i).version)
        bestTests2 += newTest
      }

      //Broadcast the tests to delete
      val testsToDelete_bcast = sc.broadcast(bestTests)

      //Filter the combos that are covered by this test we just chose
      //A combo stores the test, and version that covers it.
      newCombos = newCombos.flatMap(combo => {
        val testsToDelete = testsToDelete_bcast.value

        var delete = false
        var j = 0

        var list = new ArrayBuffer[element]()
        val c = combo(0)

        //Create a small list
        for (j <- 0 until someTests.size) {
          val answer = isComboHere2(combo, someTests(j), t)
          if (answer == true) { //if true, we add this version to the possible tests
            list += element(j, c)
          }
        }


        loop4

        def loop4(): Unit = {
          if (j == list.length) //end of list condition
            return
          val list_elem = list(j)
          if (list_elem.test != -1 && testsToDelete(list_elem.test).version == list_elem.version) {
            delete = true
            return
          }
          j += 1
          loop4
        }

        if (delete == true)
          None
        else Some(combo)
      }).persist(StorageLevel.MEMORY_AND_DISK)

      //Unpersist right away
      testsToDelete_bcast.unpersist(false)

      //  if (i % 3 == 0) //seems to give better performance
      newCombos = newCombos.localCheckpoint()

      i += m //move m by 2

      loop2
    }

    loop3 //go into the loop for the first time.
    //We use this loop to finish extending the tests
    def loop3(): Unit = {
      if (remainingElements == false) return //get out of this loop right away if we have finished properly.
      i += m //increment to the next test
      loop2 //call the main loop to finish another test

      ////////////////////////////
      loop3
    }

    //Now we return the results, and also the uncovered combos.
    (bestTests2.toArray, newCombos)

  } //fin fonction horizontal growth 1 percent old


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

      //Final aggregation of the counts using reduceByKey
      var res = s1.flatMap(hash => hash.toSeq).reduceByKey((a, b) => a + b)

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
    * Not an adapative version of M. We always aim to do 100 iterations of horizontal growth
    * We calculate the M value using 1% of the test size.
    * We can also receive a M value by parameter.
    *
    * @param tests
    * @param combos
    * @param v
    * @param t
    * @param sc
    * @return
    */
  def horizontalgrowth_1percent_wip(tests: Array[Array[Char]], combos: RDD[Array[Char]], v: Int,
                                    t: Int, sc: SparkContext, hstep: Int = -1):
  (Array[Array[Char]], RDD[Array[Char]]) = {
    //Take our test suite and broadcast it.
    var bestTests2 = new ArrayBuffer[Array[Char]]() //the tests we return
    var remainingElements = false

    case class element(var test: Int, version: Char)
    case class comboEntry(var combo: Array[Char], var list: ArrayBuffer[element])

    var newCombos = combos

    //Start M at 1% of total test size
    var m = tests.size / 100
    if (m < 1) m = 1
    var i = 0 //for each test

    //Set the M value from the static value if there was one provided.
    if (hstep != -1) m = hstep

    //Todo : Creer plus de partitions ici

    loop2 //go into the main loop
    def loop2(): Unit = {

      println(s"value of M : $m for value of i =  $i")

      //Exit condition of the loop
      if (i >= tests.length) {
        remainingElements = false
        return
      }

      //Pick the M next tests
      var someTests = takeM(tests, m, i)
      if (someTests.size < m) m = someTests.size

      //Broadcast the tests
      val someTests_bcast = sc.broadcast(someTests)

      case class key_v(var test: Int, version: Char)

      val s1 = newCombos.mapPartitions(partition => {
        var hashmappp = scala.collection.mutable.HashMap.empty[key_v, Int]
        partition.foreach(combo => {
          //Create the list on the fly for the element. This will save memory
          val someTests = someTests_bcast.value
          //Get the version of the combo
          var list = new ArrayBuffer[element]()
          val c = combo(0)

          //Create a small list
          for (j <- 0 until someTests.size) {
            val answer = isComboHere2(combo, someTests(j), t)
            if (answer == true) { //if true, we add this version to the possible tests
              list += element(j, c)
            }
          }

          //Put the list inside the hash table
          list.foreach(elem => {
            val test = elem.test
            if (test != -1) { //ignore
              val key = key_v(test, elem.version)
              if (hashmappp.get(key).isEmpty) //if entry is empty
                hashmappp.put(key, 1)
              else {
                hashmappp(key) += 1
              }
            }
          })
        })
        Iterator(hashmappp)
      })

      //Unpersist it right away
      someTests_bcast.unpersist(false)

      //Fold would be better here
      //Voir le StackOverflow pour merge des maps https://stackoverflow.com/questions/7076128/best-way-to-merge-two-maps-and-sum-the-values-of-same-key
      var res = s1.fold(mutable.HashMap.empty[key_v, Int])(
        (acc, values) => {
          values.foreach(elem => { //for each element of a
            val key: key_v = elem._1
            val value = elem._2
            if (acc.get(key).isEmpty) acc(key) = elem._2
            else acc(key) += value
          })
          acc
        })

      //When our M tests do not cover any combos, we still need to add the tests, but we add them with a star. The graph coloring
      //Will have to take care of it later.
      if (res.isEmpty == true) {
        for (i <- 0 until someTests.size) {
          val meat = someTests(i)
          val newTest = growby1(meat, '*')
          bestTests2 += newTest
        }

        remainingElements = true
        return
      }

      //Find the best test inside the hashtable.
      var bestTests = new ArrayBuffer[bestT]() //initiate array of size m. Index of array is the test.
      //Init that array
      for (k <- 0 until m) {
        bestTests += bestT()
      }

      //Find the best versions for each test, from the merged hash table
      res.toSeq.foreach(elem => {
        val i: Int = elem._1.test
        val count = elem._2
        val version = elem._1.version
        if (count > bestTests(i).count) {
          bestTests(i).count = count
          bestTests(i).version = version
        }
      })

      var testsToDelete = new ArrayBuffer[Array[Char]]()

      //Assemble the final test using the original test and the version
      for (i <- 0 until bestTests.size) {
        val meat = someTests(i)
        val newTest = growby1(meat, bestTests(i).version)
        testsToDelete += newTest
        bestTests2 += newTest
      }

      println("Printing the tests to delete")
      testsToDelete.foreach(print_helper(_))

      //Cover combos with these tests
      progressive_filter_combo(testsToDelete.toArray, newCombos, sc)

      newCombos = newCombos.localCheckpoint()
      i += m //move m by 2

      loop2
    }

    loop3 //go into the loop for the first time.
    //We use this loop to finish extending the tests
    def loop3(): Unit = {
      if (remainingElements == false) return //get out of this loop right away if we have finished properly.
      i += m //increment to the next test
      loop2 //call the main loop to finish another test

      ////////////////////////////
      loop3
    }

    //Now we return the results, and also the uncovered combos.
    (bestTests2.toArray, newCombos)

  } //fin fonction horizontal growth 1 percent


  /**
    * Very similar to newipogcoloring but supports seeding with an existing test suite.
    *
    * @param n
    * @param t
    * @param v
    * @param sc
    * @return
    */
  def parallel_ipog(n: Int, t: Int, v: Int, sc: SparkContext, numberProcessors: Int = 6, testsuite_filename: String = "", skipToParam: Int = 0): Array[Array[Char]] = {

    val expected = utils.numberTWAYCombos(n, t, v)
    println("Parallel IPOG with resume")
    println(s"Number of parallel graph colorings to optimize: $numberProcessors")
    println(s"Problem : n=$n,t=$t,v=$v")
    println(s"Expected number of combinations is : $expected ")
    println(s"Formula is C($n,$t) * $v^$t")

    var time_elapsed = 0

    import java.io._
    val pw = new PrintWriter(new FileOutputStream(filename, true))

    if (skipToParam == 0 && testsuite_filename != "") {
      println("No skip to param value defined, or no filename defined. Exiting")
      System.exit(1)
    }

    var tests = Array[Array[Char]]() //empty collection

    if (testsuite_filename != "")
      tests = readTestSuite(testsuite_filename)
    else tests = fastGenCombos(t, t, v, sc).collect()

    //We have started with t covered parameters
    var i = if (skipToParam != 0)
      skipToParam - t - 1
    else 0

    var t1 = System.nanoTime()


    println("printing initial tests")
    tests.foreach(test => print_helper(test))

    loop

    //Cover all the remaining parameters
    def loop(): Unit = {
      //Exit condition
      if (i + t == n) return

      println("Currently covering parameter : " + (i + t + 1))
      var newCombos = genPartialCombos(i + t, t - 1, v, sc).persist(StorageLevel.MEMORY_AND_DISK)

      //Apply horizontal growth
      val r1 = setcoverhash_progressive(tests, newCombos, v, t, sc)

      newCombos = r1._2 //Retrieve the combos that are not covered
      tests = r1._1 //Replace the tests

      //If there are still combos left to cover, apply a vertical growth algorithm
      if (newCombos.isEmpty() == false) {
        tests = updateTestColoring(tests, newCombos, sc, v)
      }

      i += 1 //move to another parameter

      println(s"ts size : ${tests.size}")

      var t2 = System.nanoTime()
      var time_elapsed = (t2 - t1).toDouble / 1000000000

      pw.append(s"$t;${i + t};$v;PARALLEL_IPOG;$time_elapsed;${tests.size}\n")
      println(s"$t;${i + t};$v;PARALLEL_IPOG;$time_elapsed;${tests.size}\n")
      pw.flush()

      saveTestSuite(s"testsuite$t-${i + t}-$v.txt", tests)

      System.gc()
      loop
    }

    //Return the test suite
    tests
  }


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
    * This is a version that uses the set cover algorithm instead of graph coloring. It should go more slowly, but offer better results.
    * Supports resuming a test suite as well.
    *
    * @param n
    * @param t
    * @param v
    * @param sc
    * @return
    */
  def parallel_ipogm_setcover(n: Int, t: Int, v: Int, sc: SparkContext, hstep: Int = -1, vstep: Int = -1,
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

