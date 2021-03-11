package dipog

import com.acme.BitSet
import enumerator.enumerator.{genPartialCombos, growby1, localGenCombos2, verify}
import org.roaringbitmap.RoaringBitmap
import utils.utils._
import withoutSpark.NoSparkv5.{addTableauEtoiles, addToTableau, initTableau, initTableauEtoiles}
import scala.collection.mutable.ArrayBuffer

/**
  ** Cette version marche sans Apache Spark. Elle utilise également un graph coloring local
  *  Fonctionne plutot bien
  */

object local_dipog_softdelete extends Serializable {
  //Nom standard pour les résultats
  val filename = "results.txt"
  var debug = false

  import cmdlineparser.TSPARK.save //Variable globale save, qui existe dans l'autre source file

  var totalTimeClone = 0.0
  var totalTimeFlip =  0.0
  var totalTimeFlip2 = 0.0
  /**
    * Ici, on a la garantie que list est non-vide.
    *
    * @param id
    * @param list
    * @param etoiles
    * @return
    */
  def generateOtherDelete(list: RoaringBitmap,
                          etoiles: RoaringBitmap, numberTests: Long) = {

    val t1 = System.nanoTime()
    val possiblyValidGuys = list.clone()
    val t2 = System.nanoTime()
    totalTimeClone += (t2 - t1).toDouble / 1000000000

    possiblyValidGuys.or(etoiles)

    val t3 = System.nanoTime()
    possiblyValidGuys.flip(0.toLong
      , numberTests)
    val t4 = System.nanoTime()
    totalTimeFlip += (t4 - t3).toDouble / 1000000000

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
                      combos: Array[(Array[Char], Int)], deletedGuys: BitSet): Array[Int] = {

    var t1 = System.nanoTime()
    var totalTimeCombos = 0.0
    var totalTimeGenerateOtherDelete = 0.0

    //Return tableau vide s'il n'y a rien a effacer
    if (nouveauxTests.isEmpty) return Array()
    val n = nouveauxTests(0).size

    val numberOfTests = nouveauxTests.size
    val tableau: Array[Array[RoaringBitmap]] = initTableau(n, v)
    val etoiles: Array[RoaringBitmap] = initTableauEtoiles(n)

    //Le id du test, on peut le générer ici sans problème
    var i = -1
    val a: Array[(Array[Char], Long)] = nouveauxTests.map(test => {
      i += 1
      (test, i.toLong)
    })

    addTableauEtoiles(etoiles, a, n, v)
    addToTableau(tableau, a, n, v)

    var t2 = System.nanoTime()
    val timeElapsed = (t2 - t1).toDouble / 1000000000
    println(s"Temps pour init + add : $timeElapsed seconds" )

    //Pour tous les combos du RDD, on retourne les ids de ceux qui sont effacés
    val killed_list = combos.flatMap(combo => {
      var keep = true

      if ( deletedGuys.get(combo._2) == false) {
        var i = 0 //quel paramètre?
        var certifiedInvalidGuys = new RoaringBitmap()
        for (it <- combo._1) { //pour tous les paramètres de ce combo
          if (it != '*') {
            val paramVal = it - '0'
            val list = tableau(i)(paramVal) //on prend tous les combos qui ont cette valeur. (Liste complète)
            val listEtoiles = etoiles(i) //on va prendre tous les combos qui ont des etoiles pour ce parametre (Liste complète)

            val t3 = System.nanoTime()
            val invalids = generateOtherDelete(list, listEtoiles, numberOfTests)
            val t4 = System.nanoTime()
            totalTimeGenerateOtherDelete += (t4 - t3).toDouble / 1000000000

            certifiedInvalidGuys or invalids
          }
          //On va chercher la liste des combos qui ont ce paramètre-valeur
          i += 1
        }

        val t9 = System.nanoTime()
        certifiedInvalidGuys.flip(0.toLong
          , numberOfTests)
        val t10 = System.nanoTime()
        totalTimeFlip2 += (t10 - t9).toDouble / 1000000000

        val it = certifiedInvalidGuys.getBatchIterator
        if (it.hasNext == true) {
          //Delete
          keep = false
        } else {
        }
      }
      if (keep == false) {
        Some(combo._2)
      }
      else {
        None
      }
    })
    //On retourne le RDD (maintenant filtré))

    println(s"Temps total du body : $totalTimeCombos seconds" )
    println(s"Temps total du totalTimeGenerateOtherDelete : $totalTimeGenerateOtherDelete seconds" )
    println(s"Temps total du totalTimeClone : $totalTimeClone seconds" )
    println(s"Temps total du totalTimeFlip : $totalTimeFlip seconds" )
    println(s"Temps total du totalTimeFlip2 : $totalTimeFlip2 seconds" )
    killed_list
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
  def horizontalgrowth(tests: Array[Array[Char]], combos: Array[Array[Char]],
                       v: Int, t: Int, hstep: Int = -1):
  (Array[Array[Char]], Array[Array[Char]]) = {

    var finalTests = new ArrayBuffer[Array[Char]]()

    //Clé composite: (Numéro du test, et sa version)
    case class key_v(var test: Int, version: Char) {
      override def toString: String = {
        var output = ""
        output += s"test: $test version $version count:"
        output
      }
    }

    var counterCombo = -1
    var deletedCombos = BitSet(combos.size+16) //Init avec des zéros a l'intérieur bien sur
    var deletedList = ArrayBuffer[Int]()

    var newCombos = combos.map( combo => {
      counterCombo +=1
      (combo, counterCombo)
    })

    //Start M at 1% of total test size
    var m = tests.size / 100
    if (m < 1) m = 1
    var i = 0 //for each test
    val n = tests(0).size

    //Set the M value from the static value if there was one provided.
    if (hstep != -1) m = hstep

    var totalTime_findValid = 0.0
    var totalTime_genTables = 0.0
    var totalTime_delete = 0.0
    var totalTime_aggregate = 0.0

    var numberCalls = 0

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
      val nTests = someTests.size

      val t3 = System.nanoTime()
      val tables = genTables(someTests.toArray, n, v)
      val t4 = System.nanoTime()
      totalTime_genTables += (t4 - t3).toDouble / 1000000000

      val tableau = tables._1
      val etoiles = tables._2

      val hashmappp = scala.collection.mutable.HashMap.empty[key_v, Int]
      //Faire version conccurrente ici


      println("Printing deleted combos bitset: ")
      println(deletedCombos)
      println("Printing deleted list")
      deletedList.foreach( e=> print(e)); println()

      newCombos.foreach(combo => {
        if (deletedCombos.get(combo._2) == false) {
          //val someTests = someTests_bcast.value
          var list = new ArrayBuffer[key_v]()
          val c = combo._1(0) //Get the version of the combo

          val t3 = System.nanoTime()
          val valids: Option[RoaringBitmap] = findValid(combo._1, tableau, etoiles, nTests)
          numberCalls +=1
          val t4 = System.nanoTime()
          totalTime_findValid += (t4 - t3).toDouble / 1000000000

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
            //Aggrégation initiale. La clé de la table de hachage, c'est (test,version).
            list.foreach(elem => {
              if (hashmappp.get(elem).isEmpty) //if entry is empty
                hashmappp.put(elem, 1)
              else {
                hashmappp(elem) += 1
              }
            })
          }
        }
      }) //Fin du foreach
      val t7 = System.nanoTime()

      val map2: Map[Int, Iterable[(key_v, Int)]] = hashmappp.groupBy(_._1.test)
      val res2 = map2.map(elem => {
        var bestVersion = '''
        var bestCount = -1
        elem._2.foreach(e => {
          if (e._2 > bestCount) {
            bestCount = e._2
            bestVersion = e._1.version
          }
        })
        (elem._1, bestVersion)
      }).toArray

      val t8 = System.nanoTime()
      totalTime_aggregate += (t8 - t7).toDouble / 1000000000


      //Add all of these as new tests
      for (i <- 0 until res2.size) {
        val id = res2(i)._1
        val version = res2(i)._2
        val testMeat = someTests(id)
        val newTest = growby1(testMeat, version)
        newTests += newTest
      }

      //newCombos = progressive_filter_combo(newTests.toArray, newCombos, sc, 500).localCheckpoint()
      val t5 = System.nanoTime()
      val teststests = newTests.toArray
      val combien = teststests.size
      val nbCombos = newCombos.size

      //Reset global counters
      totalTimeClone = 0.0
      totalTimeFlip = 0.0
      totalTimeFlip2 = 0.0

      //Autre delete. On fait juste marquer que l'element ne peut pas etre utilisé.


      println("On imprime la liste des tests:")
      teststests.foreach( e => print_helper(e))
      println("On imprime la liste des combos")
      newCombos.foreach( e => {
         print(e._2 + " " + print_helper2(e._1) + "\n")
      })

      val killedLIst = fastDeleteCombo(teststests, v, newCombos, deletedCombos)

      println("On imprime la liste des combos tués")
      killedLIst.foreach( elem => {
        print(elem + " ")
        deletedList += elem //ajoute a la liste
        deletedCombos.set(elem) //ajoute au bitset
      })
      println()

      val t6 = System.nanoTime()
      val timeElapsed = (t6 - t5).toDouble / 1000000000
      println(s"On efface $nbCombos combos avec nos $combien tests: $timeElapsed seconds")
      totalTime_delete += timeElapsed

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

    println(s"Total time spent on genTables: $totalTime_genTables seconds")
    println("Total time spent on findValid: " + totalTime_findValid + " seconds")
    println(s"Total time spent on delete: $totalTime_delete seconds")
    println(s"Total time spent on aggregate: $totalTime_aggregate seconds")
    println(s"Number of findValid calls: $numberCalls")


    println("Il faut faire un dernier delete")
    var trimmedRDD = newCombos.flatMap( combo => {
      val id = combo._2
      if (deletedCombos.get(id) == true) {
        Some(combo._1)
      }
      else None
    })


    //Now we return the results, and also the uncovered combos.
    (finalTests.toArray, trimmedRDD)
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

    import java.io._
    val pw = new PrintWriter(new FileOutputStream(filename, true))

    //We have started with t covered parameters
    var i = 0

    var t1 = System.nanoTime()
    //Horizontal extend all of them
    var tests = localGenCombos2(t, t, v, seed)
    val t2 = System.nanoTime()
    val time_elapsed = (t2 - t1).toDouble / 1000000000
    println(s"Generated the combos in " + time_elapsed + " seconds")

    loop

    //Cover all the remaining parameters
    def loop(): Unit = {
      //Exit condition
      if (i + t == n) return

      println("Currently covering parameter : " + (i + t + 1))
      var newCombos: Array[Array[Char]] = genPartialCombos(i + t, t - 1, v, seed) //Generate partial combos using the same seed
      println(s" ${newCombos.size} combos to cover")
      println(s" And we currently have ${tests.size} tests")


      val t4 = System.nanoTime()
      val r1 = horizontalgrowth(tests, newCombos, v, t, hstep)
      val t5 = System.nanoTime()
      println(s"Horizontal growth is done in  " + (t5 - t4).toDouble / 1000000000 + " seconds")


      newCombos = r1._2 //Retrieve the combos that are not covered
      tests = r1._1 //Replace the tests

      println(s" ${newCombos.size} combos remaining , sending to graph coloring")

      //If there are still combos left to cover, apply a vertical growth algorithm
      if (newCombos.size > 0) {

        val t4 = System.nanoTime()
        tests = entergraphcoloring(tests, newCombos, n, v)
        val t5 = System.nanoTime()
        println(s"Graph Coloring is done in  " + (t5 - t4).toDouble / 1000000000 + " seconds")
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
object test_localdipog_softdelete extends App {

  var n = 3
  var t = 2
  var v = 2

  import cmdlineparser.TSPARK.compressRuns
  import enumerator.enumerator.localGenCombos2
  import local_dipog_softdelete.start

  compressRuns = true
  var seed = System.nanoTime()
  val tests = start(n, t, v, -1, 100000, "OC", seed)

  println("We have " + tests.size + " tests")
  println("Printing the tests....")
  tests foreach (print_helper(_))

  println("\n\nVerifying test suite ... ")
  val combos = localGenCombos2(n, t, v, seed)
  val answer = verify(tests, n, v, combos)
  if (answer == true) println("Test suite is verified")
  else println("Test suite is not verified")
}
