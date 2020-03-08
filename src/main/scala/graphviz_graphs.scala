package graphviz

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import progressivecoloring.progressive_coloring.{determineStep, genadjlist_hashtableReduceByKey, ordercoloring_progress}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random

//Support pour graphes graphviz
//On lit le fichier graphviz, on charge les arêtes sur le cluster, et ensuite on construit le graphe avec MapReduce

object graphviz_graphs {

  //Lire le graphe graphviz
  //Le fichier doit pouvoir produire un graphe qui fit en mémoire (donc les très grands graphes ne sont pas supportés)
  //Un fichier graphviz décrit les edges
  //  graph G {
  //    0 [label="p1 = 1 p2 = 1 p3 = 1 "];
  //    0 -- 1;

  def readGraphVizFile(filename: String, sc: SparkContext) = {
    var biggestVertex = 0 //remember the biggest vertex, useful for graph coloring later on.

    //Comment inferer le return??
    def treatLine(line: String): Option[(Int, Int)] = {
      //les sommets commencent a 1
      val values = line.split(" ") //Split with space separator

      //Ligne genre graph ou commentaire
      if (values(0)(0) > '9' || values(0)(0) < '0') {
        return None
      }

      //Gerer le label (pas besoin)
      if (values(1)(0) == '[') return None

      //Faire une edge
      val src = values(0).toInt
      val dst = values(2).replace(";", "").toInt

      if (src > biggestVertex) biggestVertex = src
      if (dst > biggestVertex) biggestVertex = dst

      Some((src, dst))
    }

    var edges = new ArrayBuffer[(Int, Int)]()

    //For all the lines in the file
    for (line <- Source.fromFile(filename).getLines()) {
      val e = treatLine(line)
      if (e.nonEmpty)
        edges += e.get
    }

    (edges, biggestVertex)
  }


  /**
    * Expands into the proper graph form in the cluster
    *
    * Supports a maximum of 2^32 vertices
    * @param edges
    * @param sc
    */
  def createGraph(edges: ArrayBuffer[(Int, Int)], sc: SparkContext) = {

    import progressivecoloring.progressive_coloring._

    var rdd = sc.makeRDD(edges)

    //Initial aggregation with hash tables
    val r2 = rdd.mapPartitions(partition => {
      val hashtable = scala.collection.mutable.HashMap.empty[Long, Array[Byte]]

      partition.foreach(edge => {
        val src = edge._1
        val dst = edge._2

        if (src == 1 && dst == 6) {
          var u = 2 + 2
        }

        val biggestVertex = if (src > dst) src else dst
        val lowestVertex = if (src < dst) src else dst

        //Add to SRC always
        if (!hashtable.contains(biggestVertex))
          hashtable(biggestVertex) = new Array[Byte](biggestVertex)

        //Also add the hashtable for the lowest vertex
        if (!hashtable.contains(lowestVertex))
          hashtable(lowestVertex) = new Array[Byte](lowestVertex)

        //Add DST as a neighbor of this node
        hashtable(biggestVertex)(lowestVertex) = 1

      })
      Iterator(hashtable)
    })

    //Final aggregation with reduceByKey
    val r3 = r2.flatMap(hash => hash.toSeq).reduceByKey((a, b) => fuseadjlists(a, b)).cache()


    r3
  }


  //Generate a partial graph by filtering the unwanted vertices first.
  //Then, we can call the same function
  def filterEdges(edges: ArrayBuffer[(Int, Int)], sc: SparkContext, step: Int, i: Int) = {

    val filtered = edges.flatMap(edge => {
      val src = edge._1
      val dst = edge._2

      //Remove the edge if its out of the range we want
      if (src > i + step || dst > i + step)
        None
      else Some(edge)
    })


    filtered
  }

  /**
    * Change the order of the vertices (this will change the coloring)
    * To do this, we can swap the numbers of the edges for other numbers.
    * Therefore we need a map of a vertex id to a new id
    *
    * @param edges
    */
  def shuffleVertices(edges: ArrayBuffer[(Int, Int)], biggestVertex: Int) = {
    val seed = System.nanoTime()
    Random.setSeed(seed)
    val b: Seq[Int] = for (i <- 0 to biggestVertex) yield i
    val newSequence = Random.shuffle(b) //this sequence is a map (vertexid -> new id)

    //Transform the structure into a new one
    val newEdges = edges.map(edge => {
      val src = edge._1
      val dst = edge._2
      // (newSequence(src), b(dst))
      (newSequence(src), newSequence(dst))
    })


    // val c = for (i <- 0 until newSequence.size) yield Tuple2( newSequence(i), i)
    //Inverse the map
    var d = newSequence.toArray
    for (i <- 0 until newSequence.size) {
      val x = newSequence(i)
      d(x) = i
    }

    //Return the new Edges, and a map of vertexid -> original_id
    (newEdges, d)
  }

  /**
    * Print the coloring using the original numbering of the graph
    *
    * @param colors
    * @param originalSequence
    */
  def remap_vertices(colors: Array[Int], theMap: Seq[Int]) = {

    var i = 0
    var results = new ArrayBuffer[(Int, Int)]()
    loop

    def loop(): Unit = {
      if (i == theMap.size) return
      val newId = i
      val oldId = theMap(i)
      results += Tuple2(oldId, colors(i))
      i += 1
      loop
    }

    results.sortBy(_._1)
  }

  /** Run a graph coloring of a graphviz graph file
    * Return a map of vertex:colors to standard output
    *
    * @param filename
    * @param sc
    * @param memory
    * @param algorithm
    * @return
    */
  def graphcoloring_graphviz(filename: String, sc: SparkContext, memory: Int, algorithm: String = "OrderColoring") = {

    val a = readGraphVizFile(filename, sc)
    var biggestVertex = a._2
    val originalEdges = a._1
    val colors = new Array[Int](biggestVertex + 1) //colors for every vertex, starting at 0. We pass this data structure around and modify it

    val double_return = shuffleVertices(originalEdges, biggestVertex)
    var edges = double_return._1
    val map = double_return._2

    var maxColor = 0
    var i = 0

    var step = 0 //calculated value
    var chunkNo = 0

    loop

    def loop(): Unit = {
      chunkNo += 1
      println(s"Now processing Chunk $chunkNo of the graph using $algorithm algorithm")
      System.gc() //calling garbage collection here

      if (i >= biggestVertex + 1) return //if we have colored everything, we are good to go

      //Calculate the step using our current position and the amount of available memory.
      step = determineStep(memory, i)
      step = 5

      println(s"Currently working with a chunk of the graph with a maximum of $step vertices. The chunk's maximal size is $memory megabytes")

      //Genrate the adjlists for every combo in that list
      val r1: RDD[(Long, Array[Byte])] = createGraph(filterEdges(edges, sc, step, i), sc).cache()

      //Use KP when the graph is sparse (not dense)
      val r2 =
        if (algorithm == "KP") {
          knights_and_peasants(colors, r1, maxColor, sc)
        }
        else { //algorithm = "OC". The single threaded needs a sorted matrix of adjlists
          ordercoloring_progress(colors, r1.collect().sortBy(_._1), i, maxColor)
        }

      //Update the max color
      maxColor = r2._2
      r1.unpersist(true)
      i += step

      loop
    }

    println(s"The highest color of the graph is: $maxColor")
    println(s"The vertices with the color 0 are solitary vertices")
    //No need to create the tests after this
    print("The colors of the vertices: \n")
    val colored_vertices = remap_vertices(colors, map)
    colored_vertices.foreach(tuple => {
      println(s"id:${tuple._1} color:${tuple._2}")
    })

    //Transform into tests
    maxColor
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
  def knights_and_peasants(colors: Array[Int], adjMatrix: RDD[(Long, Array[Byte])],
                           maxColor: Int, sc: SparkContext) = {

    var iterationCounter = 1
    var currentMaxColor = maxColor
    var rdd = adjMatrix

    loop

    def loop(): Unit = {
      while (true) {

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

        //Our exit condition : no new knight in the last iteration
        if (results.isEmpty) return

        //Update the colors structure, and update the maximum color at the same time
        results.foreach(elem => {
          colors(elem._1) = elem._2
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

//    var res = for {
//      i <- 0 until theMap.size
//      color = colors(i)
//      oldId = theMap(color)
//    } yield Tuple2(oldId, color)

//Generate the map
//    var i = 0
//    val c = newSequence.map( e => {
//     val ret =  Tuple2(e,i)
//      i+=1
//      ret
//    }).sortBy( _._1)


//val r4 = r3.collect()
//
//println("Printing the hash tables")
//r4.foreach( e => {
//print(e._1 + " ")
//e._2.foreach( b => print(b))
//print("\n")
//})