
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
package roaringkp

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.roaringbitmap.RoaringBitmap

/**
  * Same thing, but we use the RoaringBitmap instead.
  *
  */

object roaringkp extends Serializable {

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
  def progressiveKP(colors: Array[Int], adjMatrix: RDD[(Long, RoaringBitmap)], remaining: Int,
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

          val neighborcolors = new Array[Int](currentMaxColor + 2)

          //First, check if the node is a peasant. If it is, we can continue working
          //Otherwise, if the node is a knight (the node has a color), we stop working right away.
          if (colors(thisId) == 0) { //this node is a peasant

            //First answer the following question : Can we become a knight in this iteration?
            //For this, we have to have the best tiebreaker. To check for this, we look at our adjlist.
            //If we find an earlier peasant that we are connected to, then we have to wait. If it's a knight and not a peasant, we are fine.
            //To check for peasants, we go through our adjlist from a certain i to a certain j

            //Batch iteration through the neighbors of this guy
            val buffer = new Array[Int](256)
            val it = adjlist.getBatchIterator

            loop //enter loop
            def loop(): Unit = {
              while (it.hasNext) {
                val batch = it.nextBatch(buffer)
                for (i <- 0 until batch) {
                  val neighbor = buffer(i)
                  val neighborColor = colors(neighbor)
                  neighborcolors(neighborColor) = 1
                  if (neighborColor == 0) {
                    betterPeasant = true
                    return
                  }
                }
              }
            } //fin loop

            //We can become a knight here
            if (betterPeasant == false) {
              c = color(neighborcolors)
            }
          } //fin du if this node is a peasant

          //Return the id and the color. If no color was found return None?
          if (c != 0) {
            Some(thisId, c)
          }
          else None
        }) //fin du map

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
    //println(s"colored the chunk in $iterationCounter iterations")

    //Return the iteration counter and the maxCounter
    (iterationCounter, currentMaxColor)
  }

}

