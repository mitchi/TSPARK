package dipog
import central.gen.verifyTestSuite
import cmdlineparser.TSPARK.resume
import enumerator.enumerator.{genPartialCombos, growby1, localGenCombos}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.roaringbitmap.RoaringBitmap
import roaringcoloring.roaring_coloring.coloring_roaring
import utils.utils._
import withoutSpark.NoSparkv5.{addTableauEtoiles, addToTableau, fastVerifyTestSuite, initTableau, initTableauEtoiles}

import scala.collection.mutable.ArrayBuffer

/**
  * On change le graph coloring, et on ajoute d'autres optimisations également
  * On fait une version sans Apache Spark
  */

object dipog_coloring3 extends Serializable {
  //Nom standard pour les résultats
  val filename = "results.txt"
  var debug = false
  import cmdlineparser.TSPARK.save //Variable globale save, qui existe dans l'autre source file


  /**
    * Ici, on a la garantie que list est non-vide.
    *
    * @param id
    * @param list
    * @param etoiles
    * @return
    */
  def generateOtherDelete(list: RoaringBitmap,
                          etoiles: RoaringBitmap, numberTests : Long) = {

    val possiblyValidGuys = list.clone()
    possiblyValidGuys.or(etoiles)
    possiblyValidGuys.flip(0.toLong
      , numberTests)
    possiblyValidGuys
  }

  /**
    * Petit algorithme pour effacer plus rapidement les combos
    *
    * @param testSuite
    * @param combos
    * @return true if the test suite validates
    */
  def fastDeleteCombo(testsToDelete: Array[Array[Char]], v: Int,
                      combos: RDD[Array[Char]], sc : SparkContext): RDD[Array[Char]] = {

    if (testsToDelete.isEmpty) return combos

    val n = testsToDelete(0).size
    val numberOfTests = testsToDelete.size
    val tab: Array[Array[RoaringBitmap]] = initTableau(n, v)
    val et: Array[RoaringBitmap] = initTableauEtoiles(n)

    //Le id du test, on peut le générer ici sans problème
    var i = -1
    val a: Array[(Array[Char], Long)] = testsToDelete.map(test => {
      i+=1
      (test, i.toLong)
    })

    addTableauEtoiles(et, a, n, v)
    addToTableau(tab, a, n, v)

    val tableaubcast = sc.broadcast(tab)
    val etoilesbcast = sc.broadcast(et)

    //Pour tous les combos du RDD
    val r1 = combos.flatMap(combo => {

      val tableau = tableaubcast.value
      val etoiles = etoilesbcast.value
      var i = 0 //quel paramètre?
      var certifiedInvalidGuys = new RoaringBitmap()
      for (it <- combo) { //pour tous les paramètres de ce combo
        if (it != '*') {
          val paramVal = it - '0'
          val list = tableau(i)(paramVal) //on prend tous les combos qui ont cette valeur. (Liste complète)
          val listEtoiles = etoiles(i) //on va prendre tous les combos qui ont des etoiles pour ce parametre (Liste complète)
          val invalids = generateOtherDelete(list, listEtoiles, numberOfTests)
          certifiedInvalidGuys or invalids
        }
        //On va chercher la liste des combos qui ont ce paramètre-valeur
        i += 1
      }

      certifiedInvalidGuys.flip(0.toLong
        , numberOfTests)

      val it = certifiedInvalidGuys.getBatchIterator
      if (it.hasNext == true) {
        None
      } else {
        Some(combo)
      }
    })
    //On retourne le RDD (maintenant filtré))
    r1
  }

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
  def genTables(someTests: Array[Array[Char]], n: Int, v: Int) = {
    val tableau: Array[Array[RoaringBitmap]] = initTableau(n, v)
    val etoiles: Array[RoaringBitmap] = initTableauEtoiles(n)

    //Le id du test, on peut le générer ici sans problème
    var i = -1
    val a = someTests.map(test => {
      i += 1
      (test, i.toLong)
    })

    addTableauEtoiles(etoiles, a, n, v)
    addToTableau(tableau, a, n, v)

    (tableau, etoiles)
  }

  /**
    * Ici, on a la garantie que list est non-vide.
    *
    * @param id
    * @param list
    * @param etoiles
    * @return
    */
  def generateOtherList(list: RoaringBitmap,
                        etoiles: RoaringBitmap, nTests: Int) = {

    val possiblyValidGuys = list.clone()
    possiblyValidGuys.or(etoiles)

    possiblyValidGuys.flip(0.toLong
      , nTests)

    possiblyValidGuys
  }

  /**
    * On trouve les tests qui sont compatibles avec le combo, en utilisant l'algorithme OX
    * On retourne la liste de ces tests
    *
    * @param combo
    * @param tableau
    * @param etoiles
    */
  def findValid(combo: Array[Char],
                tableau: Array[Array[RoaringBitmap]],
                etoiles: Array[RoaringBitmap], nTests: Int) = {

    var i = 0 //quel paramètre?
    val certifiedInvalidGuys = new RoaringBitmap()

    //On enlève le premier paramètre
    var slicedCombo = combo.slice(1, combo.length)

    for (it <- slicedCombo) {
      if (it != '*') {
        val paramVal = it - '0'
        val list = tableau(i)(paramVal) //on prend tous les combos qui ont cette valeur. (Liste complète)
        val listEtoiles = etoiles(i) //on va prendre tous les combos qui ont des etoiles pour ce parametre (Liste complète)
        val invalids = generateOtherList(list, listEtoiles, nTests)
        certifiedInvalidGuys or invalids
      } //fin du if pour le skip étoile
      i += 1
    } //fin for pour chaque paramètre du combo

    //On flip pour avoir l'ensemble des valides
    certifiedInvalidGuys.flip(0.toLong
      , nTests)

    val it = certifiedInvalidGuys.getBatchIterator
    if (it.hasNext == true) {
      Some(certifiedInvalidGuys)
    } else {
      None
    }

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
  def horizontalgrowth(tests: Array[Array[Char]], combos: Array[(Array[Char], Long)],
                       v: Int, t: Int, hstep: Int = -1):
      (Array[Array[Char]], RDD[Array[Char]]) = {

    var finalTests = new ArrayBuffer[Array[Char]]()

    //Clé composite: (Numéro du test, et sa version)
    case class key_v(var test: Int, version: Char) {
      override def toString: String = {
        var output = ""
        output += s"test: $test version $version count:"
        output
      }
    }
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

      //Exit condition of the loop
      if (i >= tests.length) {
        return
      }

      //Pick the M next tests
      var someTests = takeM(tests, m, i)
      if (someTests.size < m) m = someTests.size

      // val someTests_bcast: Broadcast[ArrayBuffer[Array[Char]]] = sc.broadcast(someTests)
      val n = tests(0).size
      val nTests = someTests.size

      val tables = genTables(someTests.toArray, n, v)
      val tableau = tables._1
      val etoiles = tables._2

      val hashmappp = scala.collection.mutable.HashMap.empty[key_v, Int]
      newCombos.foreach(combo => {
          //val someTests = someTests_bcast.value
          var list = new ArrayBuffer[key_v]()
          val c = combo._1(0) //Get the version of the combo
          val valids: Option[RoaringBitmap] = findValid(combo._1, tableau, etoiles, nTests)

          if (valids.isDefined) {
            //Batch iterator ici
            val it = valids.get.getBatchIterator
            val buffer = new Array[Int](256)
            while (it.hasNext) {
              val batch = it.nextBatch(buffer)
              for (i <- 0 until batch) {
                list += key_v(buffer(i), c)
              }
            }
            //Aggrégation initiale. La clé de la table de hachage, c'est le (test,version).
            list.foreach(elem => {
              if (hashmappp.get(elem).isEmpty) //if entry is empty
                hashmappp.put(elem, 1)
              else {
                hashmappp(elem) += 1
              }
            })
          }
      })

      //Find the best version of the tests using the cluster TODO: On peut surement enlever cette étape et remplacer par du code local
      val map2: Map[Int, Iterable[(key_v, Int)]] = hashmappp.toIterable.groupBy(_._1.test)
      val res2 = map2.map( elem => {
        var bestVersion = '''
        var bestCount = -1
        elem._2.foreach( e => {
          if (e._2 > bestCount) {
            bestCount = e._2
            bestVersion = e._1.version
          }
        })
        (elem._1, bestVersion)
      }).toArray

      //Add all of these as new tests
      for (i <- 0 until res2.size) {
        val id = res2(i)._1
        val version = res2(i)._2
        val testMeat = someTests(id)
        val newTest = growby1(testMeat, version)
        newTests += newTest
      }

      //newCombos = progressive_filter_combo(newTests.toArray, newCombos, sc, 500).localCheckpoint()
      newCombos = fastDeleteCombo(newTests.toArray, v, newCombos, sc).localCheckpoint()

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
    *
    * @param n
    * @param t
    * @param v
    * @param sc
    * @return
    */
  def start(n: Int, t: Int, v: Int, hstep: Int = -1,
            chunksize: Int = 40000, algorithm: String = "OC", seed: Long): Array[Array[Char]] = {

    val expected = numberTWAYCombos(n, t, v)
    println("Local IPOG Coloring with M tests")
    println(s"Horizontal growth is performed in $hstep iterations")
    println(s"Chunk size: $chunksize vertices")
    println(s"Algorithm for graph coloring is: $algorithm")
    println(s"Problem: n=$n,t=$t,v=$v")
    println(s"Expected number of combinations is: $expected ")
    println(s"Formula is C($n,$t) * $v^$t")
    var time_elapsed = 0

    import java.io._
    val pw = new PrintWriter(new FileOutputStream(filename, true))

    //We have started with t covered parameters
    var i = 0

    var t1 = System.nanoTime()
    //Horizontal extend all of them
    var tests = localGenCombos(t, t, v, seed)

    loop

    //Cover all the remaining parameters
    def loop(): Unit = {
      //Exit condition
      if (i + t == n) return

      println("Currently covering parameter : " + (i + t + 1))
      var newCombos = genPartialCombos(i + t, t - 1, v)
      println(s" ${newCombos.size} combos to cover")
      println(s" And we currently have ${tests.size} tests")

      val r1 = horizontalgrowth(tests, newCombos, v, t, sc, hstep)

      newCombos = r1._2 //Retrieve the combos that are not covered
      tests = r1._1 //Replace the tests

      println(s" ${newCombos.count()} combos remaining , sending to graph coloring")

      //If there are still combos left to cover, apply a vertical growth algorithm
      if (newCombos.isEmpty() == false) {
        tests = entergraphcoloring(tests, newCombos, sc, chunksize, algorithm)
      }

      i += 1 //move to another parameter

      println(s"ts size : ${tests.size}")

      var t2 = System.nanoTime()
      var time_elapsed = (t2 - t1).toDouble / 1000000000

      pw.append(s"$t;${i + t};$v;DIPOG_COLORING_FAST_ROARING;$time_elapsed;${tests.size}\n")
      println(s"$t;${i + t};$v;DIPOG_COLORING_FAST_ROARING;$time_elapsed;${tests.size}\n")
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

/**
  * Petit objet pour tester cet algorithme, rien de trop compliqué
  */
object test_dipogcoloring3 extends App {

  var n = 8
  var t = 7
  var v = 4

  import cmdlineparser.TSPARK.compressRuns
  import dipog.dipog_coloring3.start
  compressRuns = true
  var seed = System.nanoTime()
  val tests = start(n, t, v, -1, 100000, "OC", seed)

    println("We have " + tests.size + " tests")
    //println("Printing the tests....")
    //tests foreach (print_helper(_))

  println("\n\nVerifying test suite ... ")
  val combos: Array[(Array[Char], Long)] = localGenCombos(n,t,v,seed)
  val answer = fastVerifyTestSuite(tests, n, v, combos)
  if (answer == true) println("Test suite is verified")
  else println("Test suite is not verified")

}
