package enumerator

import generator.{_step}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

/**
  * Edmond La Chance UQAC 2020
  * This singleton object contains functions that describe a distributed enumerator of combinations (t-way or Phi-way combos)
  * By using Buffer[Char] instead of Array[Char], we can use Arrays as keys in Apache Spark
  */

import utils.utils._

object distributed_enumerator extends Serializable {

  object Helpers {

    implicit class dodo(x: Array[Char]) {
      def enString(): String = {
        var s = ""
        x.foreach(c => s += c)
        s
      }
    }

  }

  /*
  L.map { case (key, values) =>
  (key, values.groupBy(identity).mapValues(_.size).toArray)
}
   */


  /**
    * Generate all combos
    * P0 P1 P2 P3 P4 P5.  t=3, avec P0 P2 P4  donne 10101
    * Generate all n-way parameter combinations in which the number of 1s is exactly n
    * *
    *
    * Here we generate steps, so that we can parallelize the work of generating all the parameter vectors
    * Generate steps pourrait être mis dans un flatMap je suppose
    * A step is a starting vector,and an end vector
    * We can generate if, and only if, we can step a previous vector and stay valid (we add an extra zero to the right)
    * *
    * Slide : On prend tous les caractères, et on fait roll vers la gauche.
    * On part de la fin, on rajoute un zéro, et on décale tous les 1
    * *
    * At step0, there are 0 leading zeroes
    * At step1, there is only 1 leading zeroes etc...
    * Generating a new step should be O(1) from the next step
    * *
    * Steps start at zero
    */

  def generate_next_step(sv: Array[Char], t: Int, step: Int):
  Tuple2[Array[Char], Boolean] = {
    //Generate all parameter vector steps
    var limit = sv.length - 1
    var newsv = sv.clone()
    var valid = true

    //Calculate position 1
    var pos1 = limit - step

    //Calculate position 2
    var pos2 = limit - step - t
    //Check if its valid
    if (pos2 < 0) {
      valid = false
    }


    if (valid == true) {
      //Can we generate an end condition?
      newsv(pos1) = '0'
      newsv(pos2) = '1'
    }

    //End condition
    (newsv, valid)
  }


  /** Returns true if equal, false if different */
  def compareSteps(s1: Array[Char], s2: Array[Char]): Boolean = {

    var result = true
    var i = 0

    def loop(): Unit = {
      if (i == s1.length) return //we have reached the end of the string, we stop and they are the same
      if (s1(i) != s2(i)) {
        result = false
        return
      } //the strings differ,we return false
      i += 1
      loop
    }

    loop

    result
  }


  /** Take a step, and generate parameter vectors until we have the stopping condition */
  def generate_from_step(step: _step, t: Int):
  ArrayBuffer[Array[Char]] = {
    var num = 0 //to count iterations
    var tab = step.startingpv.clone()
    var expected = step.nextpv
    var lastStep = step.lastPV
    var results = new ArrayBuffer[Array[Char]]()

    //    //Generate all parameter vectors now
    def mainLoop(): Unit = {
      // num += 1
      // print(s"\n$num.")
      //tab.foreach(print)

      //Compare our current PV to the expected end
      if (lastStep == false &&
        compareSteps(tab, expected) == true) {
        //println("\n\nWE END IT HERE\n\n")
        return
      }

      //Add to results
      results += tab.clone()

      val res = findg(tab)
      val g = res._1
      val c = res._2
      if (g == -1) {
        //println("\n\nLAST STEP\n\n")
        return //if we can't find a valid g, we end this here
      }

      //If the final digit is 1, we change g from 0 to 1, and the following parameter from 1 to 0
      if (tab(tab.length - 1) == '1') {
        tab(g) = '1'
        tab(g + 1) = '0'
      }

      //else, the final digit is '0'
      else {
        tab(g) = '1' //change g from 0 to 1
        val x = t - c - 1
        fill_lastdigits('0', tab, tab.length - 1 - g)
        fill_lastdigits('1', tab, x)
      }


      mainLoop
    }

    mainLoop

    // println(s"Number of iterations : $num")

    results

  }


  /** Fill the last t digits with char
    * Modifies tab directly */
  def fill_lastdigits(c: Char, tab: Array[Char], t: Int): Unit = {
    //If t is zero, no need to do any work
    if (t == 0) return

    //Fill the last t digits with '1'
    var i = tab.length - 1
    var cpt = 0

    //Fill the last t digits with 1
    def loop(): Unit = {
      tab(i - cpt) = c
      cpt += 1
      if (cpt == t) {
        return
      }
      loop
    }

    loop //launch the tail recursive function
  }


  /** Find  the best g. Return -1 if we can't find a g.
    * We also return c */
  def findg(tab: Array[Char]): Tuple2[Int, Int] = {

    var g = -1 //index of g. Default is -1, which means that we couldnt find g
    var i = 0 //We start to search g at the beginning.
    var c = 0 //number of 1s (useful in one of the cases).
    var previousC = 0
    var limit = tab.length - 1 //limit for i+1
    def loop(): Unit = {

      if (i + 1 > limit) {
        return
      }

      if (tab(i) == '1') //handle c
        c += 1

      //If this is a valid g
      if (tab(i) == '0' && tab(i + 1) == '1') {
        g = i //save it as the new g
        previousC = c //save the state of c
      }
      i += 1

      loop
    }

    loop //call the tail recursive function

    //Return g and c
    (g, previousC)
  }


  /** Try to increment left. Start at the beginning of the array */
  def increment_left(combinations: Array[Char], v: Int): Boolean = {
    var i: Int = 0
    var theEnd = false //whether or not its over
    //var valueToAdd : Byte = 1

    val limit = v + '0'

    def loop(): Unit = {

      //We have finished filling the table
      if (i == combinations.length) {
        theEnd = true
        return
      }

      //We can still increment here
      if (combinations(i) + 1 < limit) {
        combinations(i) = (combinations(i) + 1).toChar
        return
      }

      //We cannot increment
      else if (combinations(i) + 1 == limit) {
        combinations(i) = '0'
        i += 1
      }

      loop
    }

    loop

    theEnd

  }

  /** Copy a completed combination sequence to the parameter vector */
  def copycombinations(combinations: Array[Char], indexes: ArrayBuffer[Int], pv: Array[Char]): Unit = {

    var i = 0

    def loop(): Unit = {

      if (i == combinations.length) return

      //Get the value of the combi
      val valueCombi = combinations(i)
      //get the index
      val index = indexes(i)
      //Replace inside pv
      pv(index) = valueCombi

      i += 1

      loop
    }

    loop
  }


  /** Main code to generate the value combinations from a paramater vector
    *
    * Algorithm : First we generate a small array called combinations. This is where we enumerate value combinations.
    * Then, we iterate and store the indexes of the parameter vector.
    * We store the value combination in the parameter vector. We clone the parameter vector and store the object in a return array.
    * We generate the next value combination, and repeat this process until every value combo has been enumerated.
    * We return the t-way combos at the end.
    * */
  def generate_vc(tab: Array[Char], t: Int, v: Int): ArrayBuffer[Array[Char]] = {

    //1. Generate a small array to combine everything
    //Create initial vector
    var combinations = new Array[Char](t)
    for (i <- 0 until t) {
      combinations(i) = '0'
    }

    //First we note the index of each parameter = 1. This way, we can access them directly afterwards
    var indexes = new ArrayBuffer[Int]()

    //Now we find all the 1s in the parameter vector. We put their index inside the indexes array
    var i = tab.length - 1
    while (i != -1) {
      if (tab(i) == '1') indexes += i
      i -= 1
    }

    var results = new ArrayBuffer[Array[Char]]()

    //Fill tab with "*" characters to show that the parameter is not used
    fill_lastdigits('*', tab, tab.length)

    var end = false

    while (!end) {

      //Store this combination of values into the parameter vector
      copycombinations(combinations, indexes, tab)
      results += tab.clone()

      end = increment_left(combinations, v)
    }

    results

  }

  /** Single threaded version. Very fast also!
    * Stops when it cannot find a g value */
  def generate(tab: Array[Char], t: Int) = {

    var results = new ArrayBuffer[Array[Char]]()

    //    //Generate all parameter vectors now
    def mainLoop(): Unit = {
      results += tab.clone()

      val res = findg(tab)
      val g = res._1
      val c = res._2
      if (g == -1) return //if we can't find a valid g, we end this here

      //If the final digit is 1, we change g from 0 to 1, and the following parameter from 1 to 0
      if (tab(tab.length - 1) == '1') {
        tab(g) = '1'
        tab(g + 1) = '0'
      }

      //else, the final digit is '0'
      else {
        tab(g) = '1' //change g from 0 to 1
        val x = t - c - 1
        fill_lastdigits('0', tab, tab.length - 1 - g)
        fill_lastdigits('1', tab, x)
      }

      mainLoop
    }

    mainLoop

    results

  } //end generate function


  //Call this if you want to generate the parameter vectors
  def generateParameterVectors(sc: SparkContext, n: Int, t: Int,
                               path: String = "", debug: Boolean = false): Option[RDD[Array[Char]]] = {

    val steps = generate_all_steps(n, t)

    if (debug == true) {
      println("\nPrinting all the steps :")
      steps.foreach(s => print(s))
    }
    val expected = numberParamVectors(n, t)

    val r1 = sc.makeRDD(steps) //Parallelize the steps
    val r2: RDD[Array[Char]] = r1.flatMap(step => generate_from_step(step, t)) //Generate all the parameter vectors

    if (path != "") {
      r2.map(arr => arr.mkString).coalesce(1).saveAsTextFile(path)
      None
    }

    else {
      println(s"\nExpected number of parameter vectors is : $expected ")
      println(s"Parameter vectors for t=$t,n=$n : ")
      r2.collect.foreach(arr => print_helper(arr))
      Some(r2)
    } //else we print to screen
  }


  /** Generates all the steps we need based on n : number of parameters and t : interaction strength */
  def generate_all_steps(n: Int, t: Int): ArrayBuffer[_step] = {

    //Little check to see if t <= n
    if (t > n) {
      println("Error. t > n ")
      System.exit(1)
    }

    //Create initial vector
    var tab = new Array[Char](n)
    for (i <- 0 until n) {
      tab(i) = '0'
    }

    //Fill the t last digits with 1
    fill_lastdigits('1', tab, t)

    var steps = new ArrayBuffer[_step]()
    var step = 0
    var current = tab

    def loop(): Unit = {
      var result = generate_next_step(current, t, step)
      if (result._2 == true) //if the next step is valid, we store this one
        steps += _step(step, current, result._1, false)

      else if (result._2 == false) {
        steps += _step(step, current, result._1, true)
        return //We have finished our work here
      }

      //Change the current PV
      current = result._1
      step += 1

      loop
    }

    loop //launch tail recursive function

    steps
  }

  //Here we generate all the value combinations
  def generateValueCombinations(sc: SparkContext, n: Int, t: Int, v: Int,
                                path: String = "", debug: Boolean = false): Unit = {

    val steps = generate_all_steps(n, t)
    val r1 = sc.makeRDD(steps) //Parallelize the steps
    val r2 = r1.flatMap(step => generate_from_step(step, t)) //Generate all the parameter vectors
    val r3 = r2.flatMap(pv => generate_vc(pv, t, v))

    val expected = numberTWAYCombos(n, t, v)

    println(s"Expected number of combinations is : $expected ")

    val filesize = expected * n / 1000 / 1000 //filesize in bytes, kbytes, megabytes
    println(s"Expected file size is : $filesize megabytes")

    println(s"Formula is C($n,$t) * $v^$t")

    if (path != "")
      r3.map(arr => arr.mkString).coalesce(1).saveAsTextFile(path)
    else {
      println("Guidelines : * character is used to show that this parameter is not present in the tway combo and values start at 0")
      println(s"Value combinations for t=$t,n=$n,v=$v : \n")
      r3.collect.foreach(vc => print(vc))
    } //else we print to screen

  } //fin fonction generateValueCombinations


  /** Generate partial combos
    *
    * This is used in the IPOG algorithm to generate the combos, only for the next parameter.
    * What we do is that we generate parameter vectors for (t-1) and then we add this parameter.
    *
    * */
  def genPartialCombos(n: Int, t: Int, v: Int, sc: SparkContext): RDD[Array[Char]] = {
    val steps = generate_all_steps(n, t)
    val r1 = sc.makeRDD(steps) //Parallelize the steps
    val r2 = r1.flatMap(step => generate_from_step(step, t)) //Generate all the parameter vectors
    //Add the next parameter
    val r3 = r2.map(growby1(_, '1'))
    val r4 = r3.flatMap(pv => generate_vc(pv, t + 1, v)) //Generate the tway combos

    r4
  }


  /**
    * This function generates the t-way combos if you give the right parameters.
    * This function does not cache the RDD by default. Therefore, if you collect, fold, reduce or do any action using this RDD
    * Spark will delete it from memory after using it. Cache(), Checkpoint or persist the RDD yourself to avoid this.
    *
    * @param n
    * @param t
    * @param v
    * @param sc SparkContext
    * @return RDD of combos
    */
  def fastGenCombos(n: Int, t: Int, v: Int, sc: SparkContext): RDD[Array[Char]] = {

    //Step 1 : Cover t parameters
    val steps = generate_all_steps(n, t)
    val r1 = sc.makeRDD(steps) //Parallelize the steps
    val r2 = r1.flatMap(step => generate_from_step(step, t)) //Generate all the parameter vectors
    var combosRDD = r2.flatMap(pv => generate_vc(pv, t, v)) //removed cache here. Functions have to cache on their own
    combosRDD
  }


  /** Keep t characters in the array
    * Returns a new array?? gotta test this */
  def cutArray(tab: Array[Char], t: Int): Array[Char] = {
    tab.slice(tab.length - t, tab.length)
  }

  /** Generate an empty vector. Used by the tway combo generator */
  def generate_empty(n: Int): Array[Char] = {
    //Create initial vector
    var tab = new Array[Char](n)
    for (i <- 0 until n) {
      tab(i) = '0'
    }

    //Fill the t last digits with 1
    fill_lastdigits('1', tab, n)
    tab
  }


  /** Do the horizontal extension for only one test
    * We supply the number of values. This number can vary if we a number of variables that varies with the parameter.
    * We add the new element at the beginning of the array (bigger parameter = early in the array) */
  def growby1(tab: Array[Char], value: Char): Array[Char] = {
    val newSize = tab.length + 1
    var tab2 = new Array[Char](newSize)
    tab.copyToArray(tab2, 1)
    tab2(0) = value
    tab2
  }


  /**
    * Input : A combo in Array[Char] form
    * Input :  v, a fixed number of values per parameters. In the future, this should be changed to a Map(p1 -> 3 values, p2-> 4 values)
    * Output : An array of tests
    *
    * The problem of this function is that it generates an absurd number of tests if the number of blank spaces in the combo is high,
    * and also if v is high
    *
    * When this happens, we should be using some kind of sampling
    */
  def combo_to_tests(combo: Array[Char], v: Int): Array[Array[Char]] = {
    //First, count the number of stars
    var numberOfStars = 0
    combo.foreach(c => if (c == '*') numberOfStars += 1)

    //1. Generate a small array to combine everything
    //Create initial vector
    var combinations = new Array[Char](numberOfStars)
    for (i <- 0 until numberOfStars) {
      combinations(i) = '0'
    }

    //First we note the index of each parameter = 1. This way, we can access them directly afterwards
    var indexes = new ArrayBuffer[Int]()

    //We find all the places where there are stars. We save these places
    var i = combo.length - 1
    while (i != -1) {
      if (combo(i) == '*') indexes += i
      i -= 1
    }

    var results = new ArrayBuffer[Array[Char]]()
    var end = false

    while (!end) {

      //Store this combination of values into the parameter vector
      copycombinations(combinations, indexes, combo)
      results += combo.clone()
      end = increment_left(combinations, v)
    }

    //Now we have to turn the combo array to an Integer value.
    results.toArray
  }

}
