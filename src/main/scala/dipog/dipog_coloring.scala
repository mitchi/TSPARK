package dipog

import central.gen.verifyTestSuite
import cmdlineparser.TSPARK.resume
import enumerator.distributed_enumerator.{fastGenCombos, genPartialCombos, growby1}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.roaringbitmap.RoaringBitmap
import roaringcoloring.roaring_coloring.coloring_roaring
import utils.utils._
import withoutSpark.NoSparkv5.{addTableauEtoiles, addToTableau, initTableau, initTableauEtoiles}

import scala.collection.mutable.ArrayBuffer

object dipog_coloring extends Serializable {
  //Nom standard pour les résultats
  val filename = "results.txt"
  var debug = true
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
                          combos: RDD[Array[Char]], sc : SparkContext) = {

    println("On imprime les tests a effacer")
    testsToDelete.foreach (test => println(print_helper2(test) + "\n"))

    val t = testsToDelete(0)
    if (t(0) == '1' && t(1) == '0' && t(2) == '1'){
        var debughere = "truetrue"
    }


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

    println("On imprime le tableau apres remplissage")
    for (i <- 0 until n) { //pour tous les paramètres
      for (j <- 0 until v) { //pour toutes les valeurs
        println(s"p$i=$j " + tab(i)(j).toString)
      }
    }

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
         // println("list is " + list.toString)
            val listEtoiles = etoiles(i) //on va prendre tous les combos qui ont des etoiles pour ce parametre (Liste complète)
            val invalids = generateOtherDelete(list, listEtoiles, numberOfTests)
         //   println("invalids is " + invalids.toString)
            //On ajoute dans la grosse liste des invalides
            certifiedInvalidGuys or invalids
          }
        //On va chercher la liste des combos qui ont ce paramètre-valeur
        i += 1
        }
      //CertifiedInvalidGuys contient la liste de tous les tests qui ne fonctionnent pas.
      //On inverse la liste pour obtenir les tests qui fonctionnent
      //Si cette liste n'est pas vide, il existe un test qui détruit ce combo
      //Sinon, le combo reste
      certifiedInvalidGuys.flip(0.toLong
        , numberOfTests)

      val it = certifiedInvalidGuys.getBatchIterator
      if (it.hasNext == true) {
        println("Le combo "+print_helper2(combo)+" est détruit par les tests")
        None
      } else {
        println("Le combo "+print_helper2(combo)+" survit")
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

   // println("Printing the tests and the numbering..")
   // a.foreach(elem => println(s"${elem._2}   " + print_helper2(elem._1)))

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
    var certifiedInvalidGuys = new RoaringBitmap()

    //On enlève le premier paramètre
    var slicedCombo = combo.slice(1, combo.length)
   // println("Sliced combo is " + print_helper2(slicedCombo))

    if (slicedCombo(0) == '*' && slicedCombo(1) == '1') {
      println("Debug from here")
    }

    for (it <- slicedCombo) {
      if (it != '*') {
        val paramVal = it - '0'
        val list = tableau(i)(paramVal) //on prend tous les combos qui ont cette valeur. (Liste complète)
          val listEtoiles = etoiles(i) //on va prendre tous les combos qui ont des etoiles pour ce parametre (Liste complète)
          val invalids = generateOtherList(list, listEtoiles, nTests)
          //On ajoute dans la grosse liste des invalides
          certifiedInvalidGuys or invalids
      } //fin du if pour le skip étoile
      i += 1
    } //fin for pour chaque paramètre du combo

    //On flip pour avoir l'ensemble des valides
    certifiedInvalidGuys.flip(0.toLong
      , nTests)

    val it = certifiedInvalidGuys.getBatchIterator
    if (it.hasNext == true) {
      println("Le combo est compatible avec des tests de la BD")
      Some(certifiedInvalidGuys)
    } else {
      println("Le combo n'est PAS compatible avec les tests de la BD")
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
  def horizontalgrowth(tests: Array[Array[Char]], combos: RDD[Array[Char]],
                       v: Int, t: Int,
                       sc: SparkContext, hstep: Int = -1):
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
    //  println(s"value of M : $m for value of i =  $i")

      //Exit condition of the loop
      if (i >= tests.length) {
        return
      }

      //Pick the M next tests
      var someTests = takeM(tests, m, i)
      if (someTests.size < m) m = someTests.size
      println(s"Taking a chunk of m=$m tests to extend...")
      println("Here are the tests of the chunk:")
      someTests.foreach( print_helper(_))

      //Broadcast the tests
      println("Adding the tests of the chunk to the database...")


      // val someTests_bcast: Broadcast[ArrayBuffer[Array[Char]]] = sc.broadcast(someTests)
      val n = tests(0).size
      val nTests = someTests.size

      val tables = genTables(someTests.toArray, n, v)
      val tableau = tables._1
      val etoiles = tables._2
      val etoilesbcast = sc.broadcast(etoiles)
      val tableaubcast = sc.broadcast(tableau)

      println("On imprime le tableau apres remplissage")
      for (i <- 0 until n) { //pour tous les paramètres
        for (j <- 0 until v) { //pour toutes les valeurs
          println(s"p$i=$j " + tableau(i)(j).toString)
        }
      }

      println("On imprime le tableau etoiles")
      var tt = 0
      for (elem <- etoiles) {
        println(s"p$tt=*" + " " + elem.toString)
        tt += 1
      }

      val s1: RDD[(key_v, Int)] = newCombos.mapPartitions(partition => {
        val hashmappp = scala.collection.mutable.HashMap.empty[key_v, Int]

        //Pour chaque combo de cette partition, on trouve les tests possibles
        partition.foreach(combo => {

          //val someTests = someTests_bcast.value
          var list = new ArrayBuffer[key_v]()
          val c = combo(0) //Get the version of the combo

         println("The combo " + print_helper2(combo) + " checks the database for possible extension...")
          val valids: Option[RoaringBitmap] = findValid(combo, tableaubcast.value, etoilesbcast.value, nTests)
        //  println("OX algorithm complete")

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

            println("On imprime la table de hachage partielle: ")
            for (elem <- hashmappp) {
              println(elem)
            }

          }
        })

          println("On imprime la table de hachage terminée: ")
          for (elem <- hashmappp) {
            println(elem)
          }

          hashmappp.iterator
        })

        //Unpersist the broadcast variable
        etoilesbcast.unpersist(false)
        tableaubcast.unpersist(false)
        //someTests_bcast.unpersist(false)

        //Aggrégation finale avec le pattern reduceByKey
        var res = s1.reduceByKey((a, b) => a + b)

        //Find the best version of the tests using the cluster TODO: On peut surement enlever cette étape et remplacer par du code local
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
        //Todo: ajouter la version plus rapide
        //Ca fonctionne avec le vieux filter!
        //newCombos = progressive_filter_combo(newTests.toArray, newCombos, sc, 500)
        newCombos = fastDeleteCombo(newTests.toArray, v, newCombos, sc).localCheckpoint()

       println("Impression des combos après le delete")
       val tempArray = newCombos.collect()
       tempArray.foreach( print_helper(_))

//        //Build a list of tests that did not cover combos
//        for (i <- 0 until someTests.size) {
//          var found = false
//          loop
//
//          def loop(): Unit = {
//            for (k <- 0 until res2.size) {
//              if (res2(k)._1 == i) {
//                found = true
//                return
//              }
//            }
//          }
//
//          //Add the test, with a star
//          if (found == false) {
//            val testMeat = someTests(i)
//            val newTest = growby1(testMeat, '*')
//            newTests += newTest
//          }
//        }

        //newCombos = newCombos.localCheckpoint()

        println("On ajoute les tests dans newTests aux tests finaux..")
        println("Tests de newTests...")
        newTests.foreach( print_helper(_))

        finalTests = finalTests ++ newTests //Concatenate the array into final tests

       println("Final tests, apres concatenation des nouveaux tests:")
       finalTests.foreach( print_helper(_))

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
        var newCombos = genPartialCombos(i + t, t - 1, v, sc).cache()

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
  object test_dipogcoloring extends App {

    val conf = new SparkConf().setMaster("local[1]").
      setAppName("DIPOG COLORING ")
      .set("spark.driver.maxResultSize", "10g")
    val sc = new SparkContext(conf)

    sc.setLogLevel("OFF")
    println(s"Printing sc.appname : ${sc.appName}")
    println(s"Printing default partitions : ${sc.defaultMinPartitions}")
    println(s"Printing sc.master : ${sc.master}")
    println(s"Printing sc.sparkUser : ${sc.sparkUser}")
    println(s"Printing sc.resources : ${sc.resources}")
    println(s"Printing sc.deploymode : ${sc.deployMode}")
    println(s"Printing sc.defaultParallelism : ${sc.defaultParallelism}")
    println(s"Printing spark.driver.maxResultSize : ${sc.getConf.getOption("spark.driver.maxResultSize")}")
    println(s"Printing spark.driver.memory : ${sc.getConf.getOption("spark.driver.memory")}")
    println(s"Printing spark.executor.memory : ${sc.getConf.getOption("spark.executor.memory")}")
    println(s"Printing spark.serializer : ${sc.getConf.getOption("spark.serializer")}")
    println(s"Printing sc.conf : ${sc.getConf}")
    println(s"Printing boolean sc.islocal : ${sc.isLocal}")

    var n = 3
    var t = 2
    var v = 2

    import cmdlineparser.TSPARK.compressRuns
    import dipog.dipog_coloring.start

    compressRuns = true
    val tests = start(n, t, v, sc, -1, 20000, "OC")

    println("We have " + tests.size + " tests")
    println("Printing the tests....")
    tests foreach (print_helper(_))

    //J'utilise pas le fast verify pour le moment
    println("\n\nVerifying test suite ... ")
    println(verifyTestSuite(tests, fastGenCombos(n, t, v, sc), sc))


  }
