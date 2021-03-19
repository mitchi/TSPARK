package dipoghypergraph

import com.acme.BitSet
import enumerator.distributed_enumerator.combo_to_tests
import hypergraph_cover.Algorithm2.{greedy_algorithm2, greedy_setcover_buffet, progressive_filter_combo_string}
import hypergraph_cover.greedypicker.greedyPicker
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.utils.{arrayToString, containsStars, stringToArray}

import scala.collection.mutable.ArrayBuffer

/**
  * On utilise Horizontal Growth avec Bitsets
  * On utilise Fastdelete + BitSets dans le Vertical Cover
  */

class dipog_hypergraph extends Serializable
{

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
          // certifiedInvalidGuys = certifiedInvalidGuys | invalids
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
    * This version uses Array[Char] tests, not integer tests.
    * We do on-the-fly generation for possible tests, and we put them right away inside the hash table.
    * Then we fold the hash tables, this is our mapreduce. And we choose the best test at every iteration
    * This algorithm will save all the memory it can, while being more cpu intensive. This is a good thing.
    *
    * This version covers M tests at the same time.
    * This value of M is calculated using an algorithm. A test is picked if it is at least 50% different from all the
    * previously picked K tests  (K < M)
    *
    * @param sc
    * @param v
    * @param rdd
    * @return
    */
  def greedy_setcover_buffet(sc: SparkContext,
                             v: Int,
                             rdd: RDD[Array[Char]],
                             vstep: Int = -1): Array[Array[Char]] = {
    //sc.setCheckpointDir(".") //not used when using local checkpoint
    val randomGen = scala.util.Random
    //.cache()
    var logEdgesChosen = ArrayBuffer[Array[Char]]()
    var counter = 1

    //Get the number of partitions
    val num_partitions = rdd.getNumPartitions
    println("Number of partitions : " + num_partitions)

    //  Unroll the RDD into a RDD of tests in String form
    var currentRDD = rdd.map(combo => {
      arrayToString(combo)
    }).cache()

    val sizeRDD = rdd.count()

    //Max picks = 1 percent of the size
    var maxPicks = sizeRDD / 100
    // if (maxPicks < 500) maxPicks = 500
    if (maxPicks < 5) maxPicks = 2

    println("Repartitionning...")
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
          partition.foreach(combo => {
            val list = combo_to_tests(stringToArray(combo), v)
            list.foreach(elem => {
              val key = arrayToString(elem)
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
        currentRDD = progressive_filter_combo_string(chosenTests.toArray, currentRDD, sc)

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


}
