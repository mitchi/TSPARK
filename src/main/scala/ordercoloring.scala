package ordercoloring

import enumerator.distributed_enumerator.{generate_all_steps, generate_from_step, generate_vc}
import central.gen.filename
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.utils

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object OrderColoring {

  /**
    * This function is used with RDD.map. It is used to transform the problem into a Graph Coloring problem.
    * If the graph is too big to fit inside a broadcasted variable, cut it into parts, and repeat the process with each part.
    * Shouldn't happen in practice, because Graph Coloring is pretty slow, although it does not consume too much memory.
    * *
    * Input : a vertex (a tway combo) basically, a broadcasted structure that contains
    * Output : the same vertex, with a portion of its ajdlist filled.
    * Return : True if there is an edge between A and B, False if there is not.
    * *
    * Algorithm.
    * We have two arrays. They can represent two combos.
    * What we want to see is whether or not Combo A and Combo B are compatible.
    * When they are compatible, it means that they could cohabit in the same test.
    * Therefore, when we look at all their individual values, Combo A's Parameter P5 can have the same value as Combo B's
    * Parameter P5. And their parameters can be completely different. However, if A's P5 and B's P5 have a different value,
    * they cannot be tested with the same test. Therefore, for a graph coloring problem, we would put an edge between these
    * two tests.
    * *
    * The algorithm returns false ("not-compatible") at the first sign of it.
    * And when two tests are not compatible, we return "true" at isAdjacent.
    * *
    * This function does not work well when you compare two "tests". It doesn't work well at all. Don't use it.
    * It works with test + combo only.
    */

  def isAdjacent(a: Array[Char], b: Array[Char]): Boolean = {
    var i = 0
    var answer = false

    @tailrec
    def loop(): Unit = {

      if (i == a.length) return

      //We basically ignore the stars. A = '*' and B not-a-star is compatible.
      //Only case when we return true
      if (
        a(i) != '*' && b(i) != '*' &&
          a(i) != b(i)) {
        answer = true
        return
      }
      i += 1
      loop
    }

    loop

    answer
  }

  /**
    * MUTABLE.
    * This function works with a lookup table of colors ( color not here = 0, here = 1)
    * Color using this function and good parameters
    * Use this function the vertex is adjacent to the other, and we want to know what the best color is
    *
    * @param vertices
    * @param vertex
    * @return THE COLOR THAT WE CAN TAKE
    */
  def color(neighbor_colors: Array[Int]): Int = {
    for (i <- 1 to neighbor_colors.length) {
      if (neighbor_colors(i) != 1) //if this color is not present
        return i //we choose it
    }
    return 1 //the case when the color lookup table is empty
  }


  /** Take the tests, and a sequence, and return a new series of tests in order.
    * This is used with the algorithm called OrderColoring
    *
    * @param sequence
    * @param tests
    * @return
    */
  def newTestsOrder(sequence: ArrayBuffer[Int], tests: Array[Array[Char]]): Array[Array[Char]] = {
    var newTests = tests.clone()
    for (i <- 0 until newTests.size) {
      newTests(i) = tests(sequence(i))
    }
    newTests
  }

  /**
    * We use order to execute a faster graph coloring.
    * We color all vertices in order
    *
    * @param numberProcessors
    * @param vertices
    * @param sc
    * @return
    */
  def orderColoring(numberProcessors: Int = 6, vertices: Array[Array[Char]], sc: SparkContext): Array[Array[Char]] = {
    val count = vertices.size
    val vertices_bcast = sc.broadcast(vertices)

    //Generate a random sequence, then execute the graph coloring algorithm
    val s1 = sc.makeRDD(1 to numberProcessors, numberProcessors).mapPartitionsWithIndex((index, it) => {
      var sequence: ArrayBuffer[Int] = new ArrayBuffer[Int]()
      for (i <- 0 until count.toInt) sequence += i
      Random.setSeed(System.nanoTime() + index.toLong)
      sequence = Random.shuffle(sequence)
      Iterator(newTestsOrder(sequence, vertices_bcast.value))
    })

    //All the graph colorings
    val s2: RDD[(Int, Array[Int], Array[Array[Char]])] = s1.map(vertices => {
      littleColoring3(vertices)
      //   littleColoring4(vertices)
    })

    //Choose the best coloring
    var coloring = s2.collect().reduce((a, b) => if (a._1 < b._1) a else b)

    var tt = ArrayBuffer[(Int, Array[Char])]()
    //Transform back to tests
    for (i <- 0 until coloring._3.size) {
      val color = coloring._2(i)
      val test = coloring._3(i)
      tt += Tuple2(color, test)
    }

    //Create the tests
    coloringToTests(sc.makeRDD(tt))
  }


  /** Little coloring for the order coloring algorithm
    * The neighbors, are always the lower vertices. Never the top ones.
    * We can still increase the speed by relying less on the Garbage Collector to size the array
    *
    * @param sequence
    * @param vertices
    * @return
    */
  def littleColoring3(vertices: Array[Array[Char]]) = {
    var maxColor = 0
    var colors = new Array[Int](vertices.size) //lookup table for a vertex and its color

    //Color each vertex in the order. i is also the number of vertices colored, and the current vertex.
    //At any time, i has i-1 neighbors.
    for (i <- 0 until vertices.size) {
      //Build the list of adjacent colors.
      val neighbor_colors = new Array[Int](maxColor + 2) //we could optimize this

      var j = 0
      loop

      def loop(): Unit = {
        if (j == i) return //our exit condition
        val answer = isAdjacent(vertices(i), vertices(j))
        if (answer == true) {
          neighbor_colors(colors(j)) = 1 //set the color using this neighbor
        }
        j += 1
        loop
      }

      //Choose from this list the best color
      val c = color(neighbor_colors)
      if (c > maxColor) maxColor = c
      colors(i) = c
    }
    (maxColor, colors, vertices)
  }


  /**
    * Input : A number of structures of type (Color, Combos)
    * Output : A number of tests
    * *
    * Algorithm : We can actually use reduceByKey to do this.
    * *
    * By the way, it is in my opinion, useless to do graph coloring for vertical growth UNLESS
    * we run the algorithm multiple times.
    * After all, we are spending precious time preparing the data structure for a quick graph coloring.
    * It makes sense to run the algorithm way more.
    */
  def coloringToTests(vertices: RDD[(Int, Array[Char])]): Array[Array[Char]] = {
    val res = vertices.reduceByKey((a, b) => {
      val r = mergeTests(a, b)
      if (r.isEmpty) {
        println("Error here. Debug this")
      }
      r.get
    })

    //Return the thing
    res.map(r => r._2).collect()
  }


  /**
    * Try to put the combo B inside A. The two "combos" don't have to be the same length.
    * Input : combo A, combo B.
    * Output : a test that contains the two combos, or false. We can use Either to do this.
    * If this function is called from the Graph Coloring, the two combos would have the same color.
    * Algorithm :
    * We have to make sure that all of b's values are transfered correctly. Therefore, first we must count how many special values
    * b contains. Then we try to transfer all of them.
    * If it works, we return Some, else we return None.
    * *
    * Remember, b goes inside a because this function handles arrays of a different size.
    */

  def mergeTests(a: Array[Char], b: Array[Char]): Option[Array[Char]] = {

    //Count how many special values are contained in b
    var special = 0
    b.foreach(c => {
      if (c != '*') special += 1
    })

    var i = 0
    var c = a.clone()

    def loop(): Unit = {

      if (i == a.length) {
        return
      }

      //Skip if both of them are stars. I use a dummy statement here, instead of a continue.
      if (a(i) == '*' && b(i) == '*') 2

      //if a(i) is a star and b(i) is not, we can send b(i) there.
      else if (a(i) == '*' && b(i) != '*') {
        c(i) = b(i)
        special -= 1 //we have matched one character
      }

      //If they have the same character, it still counts as a match!
      else if (a(i) == b(i)) {
        special -= 1
      }

      //If they have different characters. We have a no match and we can return early

      i += 1
      loop
    }

    loop

    //If we have special = 0, it means that we have successfully merged B inside A.
    if (special == 0)
      Some(c)
    else {
      //println("Error")
      None
    }


  } //fin merge tests



}


