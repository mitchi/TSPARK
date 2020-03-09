package hypergraph_cover

import scala.collection.mutable.ArrayBuffer

/**
  * The greedy picker singleton is used to pick a diverse subsets of tests.
  * Edmond La Chance UQAC 2020
  *
  *
  *
  */

object greedypicker {

  /**
    * Compare two tests and return a score of difference.
    * The idea here is that this function will help in SetCover functions to select tests that have more differences.
    * The bigger the difference between two tests, the better it is.
    *
    * @param test1
    * @param test2
    * @return
    */
  def differenceScore(test1: Array[Char], test2: Array[Char]): Int = {
    var difference = 0
    for (i <- 0 until test1.size) {
      if (test1(i) != test2(i)) difference += 1
    }
    difference
  }

  /**
    * This is a greedy algorithm for selecting good tests during the set cover algorithm
    * We will select a number of tests that are equally good at covering combos.
    * Then, we will compare the second test to the first test
    * Then, the third test will be compared to the second and first test and so on.
    *
    * We will add a test if its difference with every other test is at least 50%
    *
    * Tester dans le futur : Ajouter le check de maxPicks dans cette fonction, au lieu d'envoyer un plus petit tableau
    *
    */
  def greedyPicker(tests: ArrayBuffer[Array[Char]]): ArrayBuffer[Array[Char]] = {
    var chosenTests = new ArrayBuffer[Array[Char]]()
    chosenTests += tests(0) //choose the first test right away
    val sizeOfTests = tests(0).size

    //If there's only one test, return right away
    if (tests.size < 2) return chosenTests
    // var j = 1

    for (i <- 1 until tests.size) {
      var thisTest = tests(i)
      var j = 0

      var found = true

      //Compare this test to all the others. Pick it if it's always at least 50% different
      loop

      def loop(): Unit = {
        if (j == chosenTests.size) return
        val oneOfTheChosen = chosenTests(j)

        val difference = differenceScore(thisTest, oneOfTheChosen)
        val diff = (difference.toDouble / sizeOfTests.toDouble) * 100

        //If the test has only a little difference, we don't pick it
        if (diff < 40.0) {
          found = false
          return
        }
        j += 1
        loop
      }

      //If the test was good, we pick it
      if (found == true) {
        chosenTests += thisTest
      }

    } //end for loop

    chosenTests
  }


}

