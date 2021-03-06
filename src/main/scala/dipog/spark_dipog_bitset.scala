package dipog
import central.gen.{isComboHere, verifyTestSuite}
import cmdlineparser.TSPARK.resume
import com.acme.BitSet
import enumerator.distributed_enumerator.{fastGenCombos, genPartialCombos, growby1}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import utils.utils._

import scala.collection.mutable.ArrayBuffer

object spark_dipog_bitset extends Serializable {
  //Nom standard pour les résultats
  val filename = "results.txt"
  var debug = false
  import cmdlineparser.TSPARK.save //Variable globale save, qui existe dans l'autre source file

  /**
  Version No-Spark de Filter Combo
    */
  def filter_combo(combo: Array[Char], bv: Array[Array[Char]], t: Int): Boolean = {

    //Search all the broadcasted tests to see if the combo is there
    var i = 0
    var end = bv.length
    var returnValue = false
    val tests = bv

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
    * The problem with the filter combo technique is that it can be very slow when the test suite becomes very large.
    * It is better to filter using a fixed number of tests at a time. Otherwise we will be testing the same combos multiple times.
    *
    * @param testSuite
    * @param combos
    */
  def progressive_filter_combo(testSuite: Array[Array[Char]], combos: Array[Array[Char]], speed: Int = 500): Array[Array[Char]] = {
    //Pick 1000 tests and filter the combos with them
    //Find out what the t value is
    var t = findTFromCombo(combos(0))
    var i = 0

    val testSuiteSize = testSuite.length
    var filtered = combos

    //filtered.cache()
    def loop(): Unit = {

      val j = if (i + speed > testSuiteSize) testSuiteSize else i + speed
      val broadcasted_tests = (testSuite.slice(i, j))
      //Broadcast and filter using these new tests
      println("Broadcasting the tests, and filtering them")
      filtered = filtered.filter(combo => filter_combo(combo, broadcasted_tests, t))

      println("Number of combos after filter " + filtered.size )
      if ((i + speed > testSuiteSize)) return //if this was the last of the tests
      i = j //update our i
      loop
    }

    loop
    //Return the RDD of combos
    filtered
  }



  /**
    * Ici, on a la garantie que list est non-vide.
    *
    * @param id
    * @param list
    * @param etoiles
    * @return
    */
  def generateOtherDelete(list: BitSet, numberTests: Long) = {

    var possiblyValidGuys = list.clone()
    possiblyValidGuys.notEverything()
    possiblyValidGuys
  }

  /**
    * Petit algorithme pour effacer plus rapidement les combos
    *
    * Bugfix: Pas le droit d'utiliser les étoiles du test pour delete des combos
    *
    * @param testSuite
    * @param combos
    * @return true if the test suite validates
    */
  def fastDeleteCombo(nouveauxTests: Array[Array[Char]], v: Int,
                      combos: RDD[Array[Char]], nbrBits: Int, sc : SparkContext): RDD[Array[Char]] = {

    if (nouveauxTests.isEmpty)
      return combos
    val n = nouveauxTests(0).size

    val numberOfTests = nouveauxTests.size
    val tableau: Array[Array[BitSet]] = initTableau(n, v, nbrBits)

    //Le id du test, on peut le générer ici sans problème
    var i = -1
    val a: Array[(Array[Char], Long)] = nouveauxTests.map(test => {
      i += 1
      (test, i.toLong)
    })
    addToTableau(tableau, a, n, v)

    val tableaubcast = sc.broadcast(tableau)

    //Pour tous les combos du RDD
    val r1 = combos.flatMap(combo => {
      var i = 0 //quel paramètre?
      var certifiedInvalidGuys = BitSet(nbrBits)
      for (it <- combo) { //pour tous les paramètres de ce combo
        if (it != '*') {
          val paramVal = it - '0'
          val list = tableaubcast.value(i)(paramVal) //on prend tous les combos qui ont cette valeur. (Liste complète)

          val invalids = generateOtherDelete(list, numberOfTests)
          certifiedInvalidGuys or invalids
        }
        //On va chercher la liste des combos qui ont ce paramètre-valeur
        i += 1
      }
      certifiedInvalidGuys.notEverything()
      val it = certifiedInvalidGuys.iterator
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
    * On crée le tableau qu'on va utiliser.
    * On skip les * lorsqu'on remplit ce tableau avec les valeurs
    * Ce tableau se fait remplir avec chaque traitement de chunk.
    *
    * */
  def initTableau(n: Int, v: Int, nbrBits: Int) = {
    var tableau = new Array[Array[BitSet]](n)

    //On met très exactement n paramètres, chacun avec v valeurs. On ne gère pas les *
    for (i <- 0 until n) {
      tableau(i) = new Array[BitSet](v)
      for (v <- 0 until v) {
        tableau(i)(v) = BitSet(nbrBits)
      }
    }
    tableau
  }

  /**
    * On crée un tableau pour gérer seulement les étoiles
    * Roaring Bitmap dans le tableau
    */
  def initTableauEtoiles(n: Int, nbrBits: Int) = {
    var tableauEtoiles = new Array[BitSet](n)

    for (i <- 0 until n) {
      tableauEtoiles(i) = BitSet(nbrBits)
    }

    tableauEtoiles
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
  def genTables(someTests: Array[Array[Char]], n: Int, v: Int, nbrBits: Int) = {
    val tableau: Array[Array[BitSet]] = initTableau(n, v, nbrBits)
    val etoiles: Array[BitSet] = initTableauEtoiles(n, nbrBits)

    //Le id du test, on peut le générer ici sans problème
    var i = -1
    val a = someTests.map(test => {
      i += 1
      (test, i.toLong)
    })

    addTableauEtoiles(etoiles, a, n)
    addToTableau(tableau, a, n, v)

    (tableau, etoiles)
  }

  def addTableauEtoiles(etoiles: Array[BitSet],
                        chunk: Array[(Array[Char], Long)], n: Int) = {

    //On remplit cette structure avec notre chunk
    for (combo <- chunk) { //pour chaque combo
      for (i <- 0 until n) { //pour chaque paramètre
        val cc = combo._1(i)
        if (cc == '*') {
          etoiles(i) set (combo._2.toInt) //on ajoute dans le ArrayBuffer . On pourrait mettre l'index global aussi.mmm
        }
      }
    }
    etoiles
  }


  /**
    * On recoit un tableau, et on ajoute l'information avec le chunk de combos
    * On ajoute sans cesse dans le tableau
    *
    * @param chunk
    * @param n
    * @param v
    */
  def addToTableau(tableau: Array[Array[BitSet]],
                   chunk: Array[(Array[Char], Long)], n: Int, v: Int) = {

    //On remplit cette structure avec notre chunk
    for (combo <- chunk) { //pour chaque combo
      for (i <- 0 until n) { //pour chaque paramètre
        val cc = combo._1(i)
        if (cc != '*') {
          val vv = combo._1(i) - '0' //on va chercher la valeur
          tableau(i)(vv) set combo._2.toInt //on ajoute dans le ArrayBuffer . On pourrait mettre l'index global aussi.mmm
        }
      }
    }

    //On retourne notre travail
    tableau
  }

  /**
    * Ici, on a la garantie que list est non-vide.
    *
    * @param id
    * @param list
    * @param etoiles
    * @return
    */
  def generateOtherList(list: BitSet) = {

    var possiblyValidGuys = list.clone()
    possiblyValidGuys.notEverything()
    possiblyValidGuys
  }

  /**
    *
    * @param combo
    * @param tableau
    * @param etoiles
    */
  def findValid(combo: Array[Char],
                tableau: Array[Array[BitSet]],
                etoiles: Array[BitSet], nTests: Int, nbrBits: Int) = {

    var i = 0 //quel paramètre?
    var certifiedInvalidGuys = BitSet(nbrBits)

    //On enlève le premier paramètre
    var slicedCombo = combo.slice(1, combo.length)

    for (it <- slicedCombo) {
      if (it != '*') {
        val paramVal = it - '0'
        val list = tableau(i)(paramVal) //on prend tous les combos qui ont cette valeur. (Liste complète)
        val invalids = generateOtherList(list)
        certifiedInvalidGuys or invalids
      } //fin du if pour le skip étoile
      i += 1
    } //fin for pour chaque paramètre du combo

    //On flip pour avoir l'ensemble des valides
    certifiedInvalidGuys.notEverything()

    val it = certifiedInvalidGuys.iterator
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
  def entergraphcoloring(tests: Array[Array[Char]], combos: Array[Array[Char]], n: Int, v: Int): Array[Array[Char]] = {

    import withoutSpark.NoSparkv6.graphcoloring

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

    var input = combos union incompleteTests
    var coloredTests = graphcoloring(input, v)._1
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
                       sc: SparkContext, hstep: Int = 100):
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
    var m = tests.size / hstep
    if (m < 1) m = 1
    var i = 0 //for each test

    println("Value of m : " + m)

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

      //Little debug info
      //println(s"i=$i, nTests=$nTests")

      val tables = genTables(someTests.toArray, n, v, m) //m = nbrbits
      val tableau = tables._1
      val etoiles = tables._2
      val etoilesbcast = sc.broadcast(etoiles)
      val tableaubcast = sc.broadcast(tableau)

      val s1: RDD[(key_v, Int)] = newCombos.mapPartitions(partition => {
        val hashmappp = scala.collection.mutable.HashMap.empty[key_v, Int]

        //Pour chaque combo de cette partition, on trouve les tests possibles
        partition.foreach(combo => {
          //val someTests = someTests_bcast.value
          var list = new ArrayBuffer[key_v]()
          val c = combo(0) //Get the version of the combo

          val valids: Option[BitSet] = findValid(combo, tableaubcast.value, etoilesbcast.value, nTests, m)

          if (valids.isDefined) {
            val it = valids.get.iterator
            small_loop; def small_loop(): Unit = {
              while (it.hasNext) {
                val elem = it.next()
                if (elem >= m) return
                list += key_v(elem, c)
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
        hashmappp.iterator
      })

      //Unpersist the broadcast variable
      etoilesbcast.unpersist(false)
      tableaubcast.unpersist(false)

      //Aggrégation finale avec le pattern reduceByKey
      var res = s1.reduceByKey((a, b) => a + b)

      //Find the best version of the tests using the cluster TODO: On peut surement enlever cette étape et remplacer par du code local
      val res2 = res.map(e => Tuple2(e._1.test, (e._1.version, e._2))).reduceByKey((a, b) =>
      {
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

      //Todo: ajouter la version plus rapide
      //newCombos = progressive_filter_combo(newTests.toArray, newCombos, sc, 500).localCheckpoint()
      if (newTests.isEmpty == false) {
        newCombos = fastDeleteCombo(newTests.toArray, v, newCombos, m, sc)
      }
      newCombos.localCheckpoint()

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
  def start_bitset(n: Int, t: Int, v: Int, sc: SparkContext,
                   hstep: Int = 100,
                   chunksize: Int = 20000, algorithm: String = "OC", seed: Long): Array[Array[Char]] = {

    val expected = numberTWAYCombos(n, t, v)
    println("Distributed IPOG Coloring")
    println("Using Roaring Bitmaps + OFLIP algorithm in Horizontal Growth")
    println("Using OFLIP + Roaring Bitmaps + Order Coloring in Graph Coloring")
    println(s"Graph coloring chunk size: $chunksize vertices")
    println(s"Seed: $seed")
    println(s"Horizontal growth in $hstep iterations")
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
      var newCombos = genPartialCombos(i + t, t - 1, v, sc).cache()

      println(s" ${newCombos.count()} combos to cover")
      println(s" we currently have ${tests.size} tests")

      //Apply horizontal growth
      // val r1 = setcover_m_progressive(tests, newCombos, v, t, sc)
      val r1 = horizontalgrowth(tests, newCombos, v, t, sc, hstep)

      newCombos = r1._2 //Retrieve the combos that are not covered
      tests = r1._1 //Replace the tests

      println(s" ${newCombos.count()} combos remaining , sending to graph coloring")

      //If there are still combos left to cover, apply a vertical growth algorithm
      if (newCombos.isEmpty() == false) {
        tests = entergraphcoloring(tests, newCombos.collect(), n, v)
      }

      i += 1 //move to another parameter

      println(s"ts size : ${tests.size}")

      var t2 = System.nanoTime()
      var time_elapsed = (t2 - t1).toDouble / 1000000000

      pw.append(s"$t;${i + t};$v;DIPOG_COLORING_ROARING;seed=$seed;hstep=$hstep;$time_elapsed;${tests.size}\n")
      println(s"$t;${i + t};$v;DIPOG_COLORING_ROARING;seed=$seed;hstep=$hstep;$time_elapsed;${tests.size}\n")
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
object test_spark_dipog_bitset extends App {

  val conf = new SparkConf().setMaster("local[*]").
    setAppName("DIPOG COLORING RELEASE ")
    .set("spark.driver.maxResultSize", "10g")
  val sc = new SparkContext(conf)

  sc.setLogLevel("OFF")

  var n = 100
  var t = 2
  var v = 2
  import cmdlineparser.TSPARK.compressRuns
  import dipog.spark_dipog_bitset.start_bitset
  val seed = System.nanoTime()

  compressRuns = true
  val tests = start_bitset(n, t, v, sc, 100, 100000, "OC", seed)

  println("We have " + tests.size + " tests")
  //  println("Printing the tests....")
  //  tests foreach (print_helper(_))

  //J'utilise pas le fast verify pour le moment
  println("\n\nVerifying test suite ... ")
  println(verifyTestSuite(tests, fastGenCombos(n, t, v, sc), sc))

}
