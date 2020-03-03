
/**
  * This is a package with which we can apply a progressive K&P algorithm.
  * The advantage here is that we have access to this parallel graph coloring algorithm, but the algorithm
  * scales according to the strength of the cluster.
  * *
  * For t-way problems, this algorithm will be effective when the number of parameters is much bigger than the interaction strength
  * For instance, t=2, n=100, v=2 is a great problem for graph coloring with K&P. Many iterations are done in parallel.
  * because the graph is quite sparse (not dense).
  * Edmond La Chance UQAC December 2019
  */
package newalgo

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * The main data structures we need for this algorithm are :
  * 1) The lookuptable vertex:color. This needs O(n) space. This datastructure is always broadcasted to the cluster
  * for every iteration
  *
  * In this algorithm, we map over every vertex every iteration to color the graph.
  * The tiebreakers are the ids of the vertices.
  * By regenerating these ids every time (by shuffling the t-way combos, we will get a different result
  * every time.
  *
  *
  * MAP :
  * ADJLIST + bcast colors =====>   COLOR OR NO COLOR
  * Return colors to master, broadcast colors again. Repeat until all this batch has been done.
  *
  * Then generate more adjlists and repeat the process until everything is colored.
  *
  */


object progressive_kp extends Serializable {

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


  /**
    * We will modify the colors array.
    * This function returns when the K&P algorithm has colored all new vertices.
    *
    * The adjmatrix can be a RDD or an Array.
    * Ideally, we are using an adjmatrix that spans the whole cluster. This is a more effective way to use
    * all the RAM of the cluster.
    *
    * We can remove vertices when they are colored.
    *
    * @return
    */
  def progressiveKP(colors: Array[Int], adjMatrix: RDD[(Long, Array[Byte])], remaining: Int,
                    maxColor: Int, sc: SparkContext) = {

    var iterationCounter = 1
    var currentMaxColor = maxColor
    var remainingToBeColored = remaining //c'est ici le problème
    var rdd = adjMatrix

    loop

    def loop(): Unit = {
      while (true) {

        //The main exit condition. We have colored everything.
        if (remainingToBeColored <= 0) return

        // println(s"Iteration $iterationCounter ")

        //Broadcast the list of colors we need from the driver program
        val colors_bcast = sc.broadcast(colors)

        //From an adjmatrix RDD, produce a RDD of colors
        //We can either become a color, or stay the same.
        var colorsRDD = rdd.flatMap(elem => {


          val thisId = elem._1.toInt
          val adjlist = elem._2
          val colors = colors_bcast.value
          var betterPeasant = false
          var c = 0 //the color we find for this guy

          //First, check if the node is a peasant. If it is, we can continue working
          //Otherwise, if the node is a knight (the node has a color), we stop working right away.
          if (colors(thisId) == 0) { //this node is a peasant

            //First answer the following question : Can we become a knight in this iteration?
            //For this, we have to have the best tiebreaker. To check for this, we look at our adjlist.
            //If we find an earlier peasant that we are connected to, then we have to wait. If it's a knight and not a peasant, we are fine.
            //To check for peasants, we go through our adjlist from a certain i to a certain j

            //Check to see if there's a better peasant. Just go through the adjlist of the vertex
            var i = 0
            loop2

            def loop2(): Unit = {
              if (i == adjlist.size) return //Return when we have exhausted the list of neighbors

              //Grab the color of current neighbor
              if (adjlist(i) == 1 && colors(i) == 0) { //it's a peasant we are neighbor to. We are not the better peasant
                betterPeasant = true
                return
              }
              i += 1
              loop2
            }

            if (betterPeasant == false) {
              //If this peasant becomes a Knight :
              // Build list of neighbor colors by looking at previous guys
              val neighborcolors = new Array[Int](currentMaxColor + 2)
              for (i <- 0 until adjlist.size) {
                val a = adjlist(i)
                //If this guy is adjacent, we can take note of his color
                if (a == 1) {
                  val adjcolor = colors(i)
                  neighborcolors(adjcolor) = 1
                }
              }
              //Pick the best available color
              c = color(neighborcolors)
            }
          } //end of if node is a peasant


          //Return the id and the color. If no color was found return None?
          if (c != 0) {
            Some(thisId, c)
          }
          else None
        })

        //Unpersist the colors broadcast data structrure
        colors_bcast.unpersist(false)

        //Every vertex we have colored, we mark it in the colors data structure.
        //We also filter out the vertices that don't improve to a knight
        //val results = colorsRDD.collect().map(e => e.get)
        val results = colorsRDD.collect()

        //Update the colors structure, and update the maximum color at the same time
        results.foreach(elem => {
          colors(elem._1) = elem._2
          remainingToBeColored -= 1
          if (elem._2 > currentMaxColor) currentMaxColor = elem._2
        })

        iterationCounter += 1

      } //while true loop here
    } //def loop here

    iterationCounter -= 1
    println(s"colored the chunk in $iterationCounter iterations")

    //Return the new array of colors, and the currentMaxColor
    (colors, currentMaxColor)
  }

}

