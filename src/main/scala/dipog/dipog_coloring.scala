package dipog
import central.gen.{isComboHere2, progressive_filter_combo}
import cmdlineparser.TSPARK.resume
import enumerator.distributed_enumerator.{fastGenCombos, genPartialCombos, growby1}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.roaringbitmap.RoaringBitmap
import roaringcoloring.roaring_coloring.coloring_roaring
import utils.utils.{containsStars, numberTWAYCombos, print_helper, saveTestSuite, takeM}
import withoutSpark.NoSparkv5.{addTableauEtoiles, addToTableau, generateOtherList, initTableau, initTableauEtoiles}

import scala.collection.mutable.ArrayBuffer

class dipog_coloring extends Serializable
{
  //Nom standard pour les résultats
  val filename = "results.txt"
  var debug = false
  import cmdlineparser.TSPARK.save //Variable globale save, qui existe dans l'autre source file


  /**
    * Fast cover using the OX algorithm
    *
    * Ici, on a la situation suivante:
    *
    * On a un grand nombre de tests qui doivent être étendus de 1 paramètre.
    * On doit donc faire un grand nombre de comparaisons avec nos combos, pour trouver les tests qui sont compatibles avec eux.
    *
    * Ici, on va utiliser remplacer ces comparaisons de STRING par l'algorithme OX.
    * Donc, au lieu d'avoir combo X nbrTests comparaisons pour chaque combo, on va avoir combo + temps OX
    * Ce qui est largement mieux
    *
    */
  def genTables(someTests: Array[Array[Char]], n: Int, v : Int) = {
  {
    val tableau: Array[Array[RoaringBitmap]] = initTableau(n, v)
    val etoiles: Array[RoaringBitmap] = initTableauEtoiles(n)
    //Le id du test, on peut le générer ici sans problème
    var i = -1
    val a = someTests.map( test => {
      i+=1
      (test, i.toLong)
    })

    addTableauEtoiles(etoiles, a, n, v)
    addToTableau(tableau, a, n, v)

    (tableau, etoiles)
  }

    /**
      * On trouve les tests qui sont compatibles avec le combo, en utilisant l'algorithme OX
      * On retourne la liste de ces tests
      * @param combo
      * @param tableau
      * @param etoiles
      */
  def findValid(combo : Array[Char],
                tableau: Array[Array[RoaringBitmap]],
                etoiles: Array[RoaringBitmap]) = {

    var i = 0 //quel paramètre?
    var certifiedInvalidGuys = new RoaringBitmap()

    for (it <- combo) {
      if (it != '*') {
        val paramVal = it - '0'
        val list = tableau(i)(paramVal) //on prend tous les combos qui ont cette valeur. (Liste complète)
        val listEtoiles = etoiles(i) //on va prendre tous les combos qui ont des etoiles pour ce parametre (Liste complète)
        val invalids = generateOtherList(list, listEtoiles)
        //On ajoute dans la grosse liste des invalides
        certifiedInvalidGuys or invalids
      } //fin du if pour le skip étoile
      i += 1
    } //fin for pour chaque paramètre du combo

    //On va chercher la liste de tous les tests valides
    certifiedInvalidGuys.flip(0.toLong
      , certifiedInvalidGuys.last())

    certifiedInvalidGuys
  }


  /**
    * On utilise cette fonction pour faire le pont entre la phase de Horizontal Growth, et la phase de Vertical Growth
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

    //Sélectionner le bon algorithme ici
    var coloredTests = coloring_roaring(sc.makeRDD(input), sc, chunksize, algorithm)

    coloredTests ++ testsWithoutStars
  }



  /**
    *
    * Ici, on applique notre algorithme de Horizontal Growth pour D-IPOG-Coloring
    *
    * @param tests
    * @param combos
    * @param v
    * @param t
    * @param sc
    * @return
    */
  def horizontalgrowth(tests: Array[Array[Char]], combos: RDD[Array[Char]],
                       v: Int, t: Int,
                       sc: SparkContext, hstep: Int = -1):
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
      val someTests_bcast: Broadcast[ArrayBuffer[Array[Char]]] = sc.broadcast(someTests)

      //genTables(someTests.toArray, n, v)

      val s1 = newCombos.mapPartitions(partition => {
        var hashmappp = scala.collection.mutable.HashMap.empty[key_v, Int]

        //Pour chaque combo de cette partition...
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

        hashmappp.iterator
      })

      //Unpersist the broadcast variable
      someTests_bcast.unpersist(false)

      //Final aggregation of the counts using reduceByKey
      var res = s1.reduceByKey((a, b) => a + b)

      //Find the best version of the tests using the cluster
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
    * A hybrid IPOG (Distributed Horizontal Growth + Distributed Graph Coloring) that covers M tests at a time during set cover.
    * M value is determined at runtime.
    * @param n
    * @param t
    * @param v
    * @param sc
    * @return
    */
  def distributed_ipog_coloring_roaring(n: Int, t: Int, v: Int, sc: SparkContext,
                                        hstep: Int = -1,
                                        chunksize: Int = 20000, algorithm: String = "OC"): Array[Array[Char]] = {

    val expected = numberTWAYCombos(n, t, v)
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
      tests.foreach(print_helper(_))
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
        newCombos.collect().foreach(print_helper(_))
      }

      //Apply horizontal growth
      // val r1 = setcover_m_progressive(tests, newCombos, v, t, sc)
      val r1 = horizontalgrowth(tests, newCombos, v, t, sc, hstep)

      newCombos = r1._2 //Retrieve the combos that are not covered
      tests = r1._1 //Replace the tests

      if (debug == true) {
        println("Printing the tests after horizontal growth...")
        tests.foreach(print_helper(_))
      }

      println(s" ${newCombos.count()} combos remaining , sending to graph coloring")

      //If there are still combos left to cover, apply a vertical growth algorithm
      if (newCombos.isEmpty() == false) {

        if (debug == true) {
          println("Printing the remaining combos...")
          newCombos.collect().foreach(print_helper(_))
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







}
