package utils

import enumerator.distributed_enumerator.{fastGenCombos, genPartialCombos}
import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Various functions
  */

object utils {

  /**
    * Returns a random char between 0 and v (v exclusive)
    */
  def rdm_char(v: Int): Char = {
    (Math.random() % v + '0').toChar
  }


  /*
This function generates a random sequence of numbers of the desired interval and returns it
 */
  def generateRandomSequence(amount: Int): Array[Int] = {
    //Generate long IDs, tiebreakers and other things
    var tiebreakers: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    //Generate normal IDs
    for (i <- 0 until amount.toInt) tiebreakers += i
    //Generate tiebreakers
    tiebreakers = Random.shuffle(tiebreakers)
    tiebreakers.toArray
  }

  /**
    * http://biercoff.com/easily-measuring-code-execution-time-in-scala/
    * Executes the block, returns the result, and prints the time.
    */
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0).toDouble / 1000000000 + " seconds")
    result
  }

  var timeValue = 0.0

  /**
    * Use time2 if you want a better print of timeValue. It stores the latest time inside timeValue
    * You need to import utils.timeValue
    */
  def time2[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    timeValue = (t1 - t0).toDouble / 1000000000
    result
  }


  /**
    * Prints correctly. Returns a string as well
    */
  def print_helper(e: Array[Char]): String = {
    var emptyString = ""
    e.foreach(c => {
      print(c)
      emptyString += c
    })
    print("\n")
    emptyString
  }

  /**
    * Just returns a string
    */
  def
  print_helper2(e: Array[Char], additional: String = ""): String = {
    var emptyString = ""
    e.foreach(c => {
      emptyString += c
    })
    emptyString + additional
  }

  def factorial(n: BigInt): BigInt = {
    if (n == 1) return 1: BigInt

    var result: BigInt = 1
    var i = 1: BigInt

    def loop(): Unit = {
      if (i > n) return
      result *= i
      i += 1
      loop
    }

    loop

    result
  }

  def numberParamVectors(n: Int, t: Int): BigInt = {
    val top = factorial(n)
    val result = top / (factorial(t) * factorial(n - t))
    result
  }

  def numberTWAYCombos(n: Int, t: Int, v: Int): BigInt = {
    val toto: BigInt = BigInt(v).pow(t)
    numberParamVectors(n, t) * toto
  }

  /**
    * Input : Array of chars.
    * Output : Single 64 bit integer
    */
  def calculateIndexTCOMBO(tab: Array[Char], v: Int): Long = {
    var exponent = 0
    var finalIndex = 0

    //detransform Chars into Ints
    tab.reverse.foreach(digit => {
      val d = digit - '0'.toInt
      val partial = d * (Math.pow(v, exponent).toInt)
      finalIndex += partial
      exponent += 1
    })
    finalIndex
  }

  /**
    * Input : Array of chars.
    * Output : A String. This allows Spark to MapReduce properly
    */
  def arrayToString(arr: Array[Char]): String = {
    var result = ""
    arr.foreach(result += _)
    result
  }

  /**
    * Input : String
    * Output : Array of chars
    */
  def stringToArray(s: String): Array[Char] = {
    var result = new ArrayBuffer[Char]()
    s.foreach(result += _)
    result.toArray
  }

  def comparetwoarrays(a: Array[Char], b: Array[Char]): Boolean = {
    var answer = true

    def loop(): Unit = {
      for (i <- 0 until a.length) {
        if (a(i) != b(i)) {
          answer = false
          return
        }
      }
    }

    loop() //Call the loop
    answer
  }

  def findTestinTests(test: Array[Char], tests: Array[Array[Char]]): Boolean = {
    var answer = false

    def loop(): Unit = {
      tests.foreach(t => {
        val a = comparetwoarrays(test, t)
        if (a == true) {
          answer = true
          return
        }
      })
    }

    loop
    answer
  }

  /**
    * Find the value of T from a combo
    *
    * @param combo
    * @return
    */
  def findTFromCombo[A](combo: Seq[A]): Int = {
    var t = 0
    for (letter <- combo) {
      if (letter != '*') t += 1
    }
    t
  }


  /**
    * This function reads a file of tests
    * It ignores comment lines (#)
    *
    * @param filename a file name path
    * @return a test suite
    *         https://alvinalexander.com/scala/how-to-open-read-text-files-in-scala-cookbook-examples
    */
  def readTestSuite(filename: String): Array[Array[Char]] = {
    import scala.io.Source

    var tests = new ArrayBuffer[Array[Char]]()

    for (line <- Source.fromFile(filename).getLines) {
      //Read a test
      if (line(0) != '#') { //Ignore comment lines
        val test = ArrayBuffer[Char]()
        var i = 0
        for (e <- line; if e != ' ') {
          test += e
          i += 1
        }
        tests += test.toArray
      }

    }
    tests.toArray
  }

  /**
    *
    * @param filename the name of the file
    * @param tests    the test suite
    */
  def saveTestSuite(filename: String, tests: Array[Array[Char]]): Unit = {
    //Open file for writing
    import java.io._
    val pw = new PrintWriter(new FileOutputStream(filename, false))

    //For all tests. Write them to the file
    for (test <- tests) {
      val a = print(test)
      pw.append(s"$a\n")
      pw.flush()
    }
    //Close the file
    pw.close()
  }


  /**
    * Algorithm : If the test contains a star, we return true
    *
    * @param a test
    * @return true if the test contains a star
    */
  def containsStars(test: Array[Char]): Boolean = {
    var answer = false

    def loop(): Unit = {
      test.foreach(c => {
        if (c == '*') {
          answer = true
          return
        }
      })
    }

    loop()
    answer
  }




  /**
    * Take M tests to cover
    * *!$@$#@$#@
    *
    * @param tests
    * @param m
    */
  def takeM(tests: Array[Array[Char]], m: Int, i: Int = 0) = {
    var results = ArrayBuffer[Array[Char]]()
    var j = i //start the index at the right place
    var counter = 0
    loop

    def loop(): Unit = {
      if (j == tests.size) return //If m is bigger than the remaining tests, we will take less than m
      if (counter == m) return //we stop when we have taken m elements
      results += tests(j)
      j += 1
      counter += 1
      loop
    }

    results
  }


  /**
    * This is a utility function. It prints the combos that have to be covered in order, and nothing else.
    *
    * @param n
    * @param t
    * @param v
    * @param sc
    * @return
    */
  def print_combos_in_order(n: Int, t: Int, v: Int, sc: SparkContext): Unit = {

    val expected = utils.numberTWAYCombos(n, t, v)
    println("Printing combos in order")
    println(s"Problem : n=$n,t=$t,v=$v")
    println(s"Expected number of combinations is : $expected ")
    println(s"Formula is C($n,$t) * $v^$t")

    var i = 0

    println("Printing the initial tests")
    val tests = fastGenCombos(t, t, v, sc).collect().foreach(print_helper(_))

    loop

    def loop(): Unit = {
      //Exit condition
      if (i + t == n) return

      println(s"Printing combos for parameter ${i + t + 1}")
      var newCombos = genPartialCombos(i + t, t - 1, v, sc)
      newCombos.collect.foreach(print_helper(_))

      println("******************************")

      i += 1

      loop
    }
  }


}
