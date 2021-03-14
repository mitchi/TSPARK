package dipog
import central.gen.verifyTestSuite
import cmdlineparser.TSPARK.resume
import enumerator.distributed_enumerator.{fastGenCombos, genPartialCombos, growby1}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.roaringbitmap.RoaringBitmap
import utils.utils._
import withoutSpark.NoSparkv5.{addTableauEtoiles, addToTableau, initTableau, initTableauEtoiles}

import scala.collection.mutable.ArrayBuffer

object spark_dipog_roaring2 extends Serializable {
  //Nom standard pour les résultats
  val filename = "results.txt"
  var debug = false
  import cmdlineparser.TSPARK.save //Variable globale save, qui existe dans l'autre source file

  /**
    * Même chose que spark_dipog_roaring, sauf qu'ici on utilise un XOR sur une RoaringBitmap pour faire le flip
    * Je pense que c'est plus rapide.
    *
    * @param id
    * @param list
    * @param etoiles
    * @return
    */
  def generateOtherDelete(list: RoaringBitmap, numberTests: Long, fullBitMap: RoaringBitmap) = {

    val possiblyValidGuys = list.clone()
    //   possiblyValidGuys.flip(0.toLong
    //    , numberTests)
    possiblyValidGuys.xor(fullBitMap)

    possiblyValidGuys
  }

  /**
    * Petit algorithme pour effacer plus rapidement les combos
    *
    * @param testSuite
    * @param combos
    * @return true if the test suite validates
    */
  def fastDeleteCombo(nouveauxTests: Array[Array[Char]], v: Int,
                      combos: RDD[Array[Char]], sc: SparkContext): RDD[Array[Char]] = {

    if (nouveauxTests.isEmpty) return combos
    val n = nouveauxTests(0).size

    val numberOfTests = nouveauxTests.size
    val tableau: Array[Array[RoaringBitmap]] = initTableau(n, v)

    val fullBitMap = new RoaringBitmap()

    //Le id du test, on peut le générer ici sans problème
    var i = -1
    val a: Array[(Array[Char], Long)] = nouveauxTests.map(test => {
      i += 1; fullBitMap.add(i)
      (test, i.toLong)
    })

    addToTableau(tableau, a, n, v)
    val tableaubcast = sc.broadcast(tableau)
    val fullBitMapbcast = sc.broadcast(fullBitMap)

    //Pour tous les combos du RDD
    val r1 = combos.flatMap(combo => {
      var i = 0 //quel paramètre?
      var certifiedInvalidGuys = new RoaringBitmap()
      for (it <- combo) { //pour tous les paramètres de ce combo
        if (it != '*') {
          val paramVal = it - '0'
          val list = tableaubcast.value(i)(paramVal) //on prend tous les combos qui ont cette valeur. (Liste complète)
          val invalids = generateOtherDelete(list, numberOfTests, fullBitMapbcast.value)
          certifiedInvalidGuys or invalids
        }
        //On va chercher la liste des combos qui ont ce paramètre-valeur
        i += 1
      }

      //On flip tous les certifiés mauvais pour obtenir la liste de ceux qui sont compatibles
      //Donc, si cette liste est pleine, on a pas besoin de faire le flip
      //Il faut donc que certificedInvalidGuys contienne le même nombre que le nombre de tests
      //Quand cardinalité de certifiedInvalidGuys = nombre de Tests, on est sûr que le flip va produire l'ensemble vide
      //Quand on a l'ensemble non-vide après le flip, il contient les tests qui peuvent détruire ce combo
      //Et donc on détruit le combo

      //Méthode alternative: Calcul de la cardinalité
      //      var cardinalityBeforeFlip = certifiedInvalidGuys.getCardinality
      //      if (cardinalityBeforeFlip == numberOfTests) {
      //        Some(combo)
      //      }
      //      else None

      // var deadCombo = true
      //      var cardinalityBeforeFlip = certifiedInvalidGuys.getCardinality
      //      if (cardinalityBeforeFlip == numberOfTests) {
      //            deadCombo = false
      //      }
      //      else deadCombo = true



      certifiedInvalidGuys.flip(0.toLong
        , numberOfTests)
      //  certifiedInvalidGuys.xor(fullBitMapbcast.value)


      //var cardinalityAfterFlip = certifiedInvalidGuys.getCardinality

      val it = certifiedInvalidGuys.getBatchIterator
      if (it.hasNext == true) {
        // println(s"Deleting the combo. Number of tests is $numberOfTests, Cardinality before is $cardinalityBeforeFlip, Cardinality after is $cardinalityAfterFlip")
        None
      } else {
        // println(s"Keeping the combo. Number of tests is $numberOfTests, Cardinality before is $cardinalityBeforeFlip, Cardinality after is $cardinalityAfterFlip")
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

      val tables = genTables(someTests.toArray, n, v)
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

          val valids: Option[RoaringBitmap] = findValid(combo, tableaubcast.value, etoilesbcast.value, nTests)

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
      newCombos = fastDeleteCombo(newTests.toArray, v, newCombos, sc)
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
  def start(n: Int, t: Int, v: Int, sc: SparkContext,
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
object test_spark_dipog_roaring2 extends App {

  val conf = new SparkConf().setMaster("local[*]").
    setAppName("DIPOG COLORING RELEASE ")
    .set("spark.driver.maxResultSize", "10g")
  val sc = new SparkContext(conf)

  sc.setLogLevel("OFF")

  var n = 8
  var t = 7
  var v = 3
  import cmdlineparser.TSPARK.compressRuns
  import dipog.spark_dipog_roaring2.start
  val seed = System.nanoTime()

  compressRuns = true
  val tests = start(n, t, v, sc, 100, 100000, "OC", seed)

  println("We have " + tests.size + " tests")
  //  println("Printing the tests....")
  //  tests foreach (print_helper(_))

  //J'utilise pas le fast verify pour le moment
  println("\n\nVerifying test suite ... ")
  println(verifyTestSuite(tests, fastGenCombos(n, t, v, sc), sc))

}
