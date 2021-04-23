package phiway

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.roaringbitmap.RoaringBitmap
import roaringcoloring.roaring_coloring.ordercoloring

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import phiway._

object phiway_coloring extends Serializable {

  var debug = true
  var filename = "results.txt"

  /**
   * Au lieu d'utiliser un paramètre v, on utilise la variable globale domainsizes
   *
   * @param vertices
   * @return
   */
  def coloringToTests(vertices: RDD[(Int, clause)]): Array[String] = {

    //Transform clauses to a form proper for merging
    var vv = vertices.mapValues(clause => {
      transformClause(clause)
    })

    //We merge the clauses using the cluster
    val res = vv.reduceByKey((a, b) => {
      mergetwoclauses(a, b)
    })

    //We transform these reduced clauses into a final form to create a proper test
    val rr = res.map(e => EOVtoTest(e._2)).collect()

    rr
  }


  /**
   * Is adjacent function for building graphs from a set of phiway clauses
   *
   * @param a
   * @param b
   * @return
   */
  def isAdjacent(a: clause, b: clause): Boolean = {
    var i = 0
    var retValue = false
    val len = a.conds.length

    loop

    def loop(): Unit = {

      if (i == len) return
      val answer = compatible(a(i), b(i))
      //Si une condition n'est pas compatible, la clause est adjacente dans le graphe
      if (answer == false) {
        retValue = true
        return
      }
      i += 1
      loop
    }

    retValue
  }

  /**
   * Roaring bitmaps, Phiway clauses
   */

  def genadjlist2(clauses: RDD[(clause, Long)],
                  clausesToColor: Array[(clause, Long)],
                  sc: SparkContext) = {

    //First step is to group every combo with every other combo to do a MapReduce.
    val bcastdata = sc.broadcast(clausesToColor)
    val chunkSize = clausesToColor.size
    val limitID = clausesToColor.last._2 //Le plus gros ID dans le chunk de clauses qu'on a

    //For every partition, we build a hash table of COMBO -> List of neighbors (all neighbors are strictly less than combo)
    val r1 = clauses.mapPartitions(partition => {
      val hashtable = scala.collection.mutable.HashMap.empty[Long, RoaringBitmap]

      //Go through all the clauses of the partition
      partition.foreach(elem => {
        val thisClauseID = elem._2 //Le ID de la clause, sur la partition
        val someClauses = bcastdata.value //On recupere les clauses de la broadcast variable

        //Enter a loop to fill-in the hash table with adjacency data created from this clause

        //Go through all the clauses of the chunk
        loop;

        def loop(): Unit = {
          for (i <- someClauses) {
            val chunkClauseID = i._2
            //If this clause ID is bigger or equal, we dont generate the adjacency data.
            if (thisClauseID >= chunkClauseID)
              return

            val clauseA = elem._1
            val clauseB = i._1
            val answer = isAdjacent(clauseA, clauseB)
            //println(s"$clauseA adjacentTo $clauseB : $answer")

            if (answer == true) {
              //Check if lookuptable exists. Create it if it does not
              if (!hashtable.contains(chunkClauseID)) {
                hashtable(chunkClauseID) = new RoaringBitmap()
              }
              hashtable(chunkClauseID).add(thisClauseID.toInt) //add value to roaring bitmap
            }
          }
        } //fin loop

      }) //Fin partition foreach

      var rr2 = hashtable.toIterator
      rr2
    }) //fin mapPartitions


    // Debug mode. We print the adjacency lists
    if (debug == true) {
      val d1 = r1.mapPartitionsWithIndex((partition, it) => {

        var output = ""
        output += "Partition " + partition + "\n"
        it.foreach(e => {
          output += e._1 + " "
          // e._2.foreach(b => output += b)
          output += e._2.toString

          output += "\n"
        })

        Iterator(output)
      }).collect().foreach(println)
    }

    val r3 = r1.reduceByKey((a, b) => {
      //fuseadjlists(a, b)
      a.or(b)
      a
    })

    val r4 = r3.mapValues(e => {
      e.runOptimize()
      e
    })

    //Destroy the DAG here
    // r4.localCheckpoint()

    if (debug == true) {
      println("Fused adjlists")
      val d2 = r3.mapPartitionsWithIndex((partition, it) => {
        var output = ""
        output += "Partition " + partition + "\n"
        it.foreach(e => {
          output += e._1 + " "
          // e._2.foreach(b => output += b)
          output += e._2.toString
          output += "\n"
        })
        Iterator(output)
      }).collect().foreach(println)
    }
    r4
  }

  /**
   * Roaring bitmaps, Phiway clauses
   *
   */

  def genadjlist(i: Long, chunkSize: Long, clauses: RDD[(clause, Long)],
                 clausesToColor: Array[(clause, Long)], sc: SparkContext) = {

    //First step is to group every combo with every other combo to do a MapReduce.
    val bcastdata = sc.broadcast(clausesToColor)
    val chunkSize = clausesToColor.size

    //For every partition, we build a hash table of COMBO -> List of neighbors (all neighbors are strictly less than combo)
    val r1 = clauses.mapPartitions(partition => {
      val hashtable = scala.collection.mutable.HashMap.empty[Long, RoaringBitmap]

      partition.foreach(elem => {
        val thisId = elem._2

        // Si le ID du combo est plus haut que ceux des combos choisis, on fait aucun travail.
        if (thisId > i + chunkSize) {
          val bbb = 2 //dummy statement for Scala
        }
        else {
          val someClauses = bcastdata.value
          var j = if (thisId >= i)
            (thisId - i).toInt
          else 0
          loop

          def loop(): Unit = {

            //Basic exit condition
            if (j == someClauses.size) return

            //If id of this general combo is lower than the combo to color, we can work
            if (thisId < i + j) {

              //Check if lookuptable exists. Create it if it does not
              if (!hashtable.contains(i + j)) {
                hashtable(i + j) = new RoaringBitmap()
              }

              val answer = isAdjacent(elem._1, someClauses(j)._1)
              val t1 = elem._1
              val t2 = someClauses(j)._1
              //println(s"$t1 adjacentTo $t2 : $answer")

              if (answer == true)
                hashtable(i + j).add(thisId.toInt) //add value to roaring bitmap
              // hashtable(i + j)(thisId.toInt) = 1
              //else hashtable(i + j)(thisId.toInt) = 0
            }

            j += 1
            loop
          }
        }
      })

      //New ArrayBufer with results
      // val rr = new ArrayBuffer[(Long, RoaringBitmap)]()
      var rr2 = hashtable.toIterator
      rr2
      //Iterator(hashtable.toArray)
      //Return an iterator here
    })


    // Debug mode. We print the adjacency lists
    if (debug == true) {
      val d1 = r1.mapPartitionsWithIndex((partition, it) => {

        var output = ""
        output += "Partition " + partition + "\n"
        it.foreach(e => {
          output += e._1 + " "
          // e._2.foreach(b => output += b)
          output += e._2.toString

          output += "\n"
        })

        Iterator(output)
      }).collect().foreach(println)
    }

    val r3 = r1.reduceByKey((a, b) => {
      //fuseadjlists(a, b)
      a.or(b)
      a
    })

    val r4 = r3.mapValues(e => {
      e.runOptimize()
      e
    })

    //Destroy the DAG here
    // r4.localCheckpoint()

    if (debug == true) {
      println("Fused adjlists")
      val d2 = r3.mapPartitionsWithIndex((partition, it) => {
        var output = ""
        output += "Partition " + partition + "\n"
        it.foreach(e => {
          output += e._1 + " "
          // e._2.foreach(b => output += b)
          output += e._2.toString
          output += "\n"
        })
        Iterator(output)
      }).collect().foreach(println)
    }
    r4
  }


  /**
   * Used to filter out combos or clauses so that we don't work with too many of them
   * at the same time
   *
   * @param combos
   * @param i
   * @param step
   * @return
   */
  def filterBig(combos: RDD[(clause, Long)], i: Int, step: Int):
  RDD[(clause, Long)] = {
    combos.flatMap(e => {
      if (e._2 > i + step) None
      else Some(e)
    })
  }


  /** We assign  numbers to the clauses. We make sure that the numbers are spread out over the partitions.
   * Phiway coloring
   *
   * @param combos
   */
  def phiway_numberclauses(clauses: RDD[clause], sc: SparkContext) = {

    //Grab the number of combos per partition. Make it a map
    val sizes: Array[Int] = clauses.mapPartitionsWithIndex((partition, iterator) => {
      Iterator(Tuple2(partition, iterator.size))
    }).collect().sortBy(_._1).map(e => e._2)

    val numberPartitions = sizes.length

    //We define this helper function right away
    def getNextPartition(current: Int): Int = {
      if (current == numberPartitions - 1) return 0
      else return current + 1
    }

    //Array that contains the results. Left is partition, Right is number
    var results = new ArrayBuffer[Tuple2[Int, Long]]()

    //Alternate the numbering for the partitions, and take in consideration the partition sizes
    val count = clauses.count()

    var i = 0 //numbering starts at 0
    var j = 0 //first partition is 0 too
    //Alternate the placement of numbers
    //Algorithm : We place the number in the partition if there is still enough space in the partition. If there is not,
    //we move to the next partition
    loop

    def loop(): Unit = {

      while (true) {

        if (i == count) return //biggest number reached

        //Check if the partition is okay too
        if (sizes(j) == 0) {
          j = getNextPartition(j)
        }

        //There are numbers remaining in this partition. Add the result and continue to to next number, and the next partition
        else {
          sizes(j) -= 1 //decrease the quantity remaining in the partition
          results += Tuple2(j, i.toLong)
          i += 1
          j = getNextPartition(j)
        }

      }

    }

    //Bcast variable
    val bcast = sc.broadcast(results)

    val numberedCombos: RDD[(clause, Long)] = clauses.mapPartitionsWithIndex((partitionNo, it) => {

      //Just grab the numbers here
      val numbers: Seq[Long] = bcast.value.flatMap(elem => {
        if (partitionNo == elem._1) Some(elem._2)
        else None
      })

      var i = 0
      val ress = it.map(elem => {
        val result = (elem, numbers(i))
        i += 1
        result
      })
      ress
    })

    //Return ze result
    numberedCombos
  }


  /**
   * Phiway coloring
   *
   * @param combos    the RDD of combos
   * @param sc        SparkContext
   * @param chunkSize the number of vertices to move at the same time.
   * @param algorithm OrderColoring or Knights and Peasants
   * @return
   */
  def phiway_coloring(clauses: RDD[clause],
                      sc: SparkContext,
                      chunkSize: Int,
                      algorithm: String = "OC") = {
    //First, color 100 000 elements. If the graph is colored, we return. Else we continue here.
    val count = clauses.count()
    val colors = new Array[Int](count.toInt) //colors for every vertex, starting at 0. We pass this data structure around and modify it

    //Save the seed so that Spark can regenerate the combos on demand. This would save memory but add CPU time
    //checkpoint to HDFS or on a ssd drive would also work
    val seed = System.nanoTime()
    println(s"Using seed $seed")

    var totalIterations = 0

    //Print the combos 1 time
    if (debug == true) {

      println("Printing clauses before shuffle")
      clauses.mapPartitionsWithIndex((index, iterator) => {
        var stringPartition = ""
        stringPartition += "Partition " + index + "\n"
        iterator.foreach(e => {
          stringPartition += e + "\n"
        })
        Iterator(stringPartition)
      }).collect().foreach(println)
    }

    //Shuffle the combos before. Doing this ensures a different result every run.
    var myclauses = clauses.mapPartitions(it => {
      //        if (debug == true) {
      //        Random.setSeed(1)
      //        }
      //else
      Random.setSeed(seed)

      Random.shuffle(it)
    }, true).cache()

    //Print the combos 1 time
    if (debug == true) {
      println("Printing clauses after shuffle")
      myclauses.collect().foreach(println)
    }

    //TODO : Color 100 000 vertex first before doing the rest.
    //val combosNumbered = mycombos.zipWithIndex().cache()
    val clausesNumbered = phiway_numberclauses(myclauses, sc).cache()

    //We no longer need combos
    clauses.unpersist(false)

    //Print the combos 1 time
    if (debug == true) {
      println("Printing clauses after numbering")

      clausesNumbered.mapPartitionsWithIndex((index, iterator) => {
        var stringPartition = ""
        stringPartition += "Partition " + index + "\n"
        iterator.foreach(e => {
          stringPartition += e._2 + ". " + (e._1.toString) + "\n"
        })
        Iterator(stringPartition)
      }).collect().foreach(println)
      // combosNumbered.collect().foreach(e => print(e._2 + " ") + utils.print_helper(e._1))
    }

    //Before everything, we can color the first vertex with the color 1
    colors(0) = 1
    var maxColor = 1
    var i = 1 //the number of vertices we have colored

    //var step = 0 //calculated value
    var chunkNo = 0

    loop

    def loop(): Unit = {
      chunkNo += 1
      println(s"Now processing Chunk $chunkNo of the graph...")
      System.gc() //calling garbage collection here

      if (i >= count) return //if we have colored everything, we are good to go

      println(s"Currently working with a chunk of the graph with $chunkSize vertices.")

      //Filter the combos we color in the next OrderColoring iteration
      val someClauses = clausesNumbered.flatMap(elem => {
        if (elem._2 < i + chunkSize && elem._2 >= i) {
          Some(elem)
        }
        else None
      }).collect().sortBy(_._2)

      //Print the combos now that they are shuffled (Debug mode)
      if (debug == true) {
        someClauses.foreach(e => {
          print(e._2 + " ")
          print(e._1)
        })
      }

      //Generate the adjlists for every combo in that list
      // val r1 = genadjlist_roaring(i, step, combosNumbered, someCombos, sc).cache()
      val filteredCombos = filterBig(clausesNumbered, i, chunkSize)
      //filteredCombos.localCheckpoint()
      //val r1 = genadjlist2(filteredCombos, someClauses, sc).cache()
      val r1 = genadjlist(i, chunkSize, filteredCombos, someClauses, sc).cache()

      //Print the types of the roaring bitmaps
      if (debug == true) {
        import org.roaringbitmap.insights.BitmapAnalyser._
        r1.collect().foreach(elem => {
          val rr = analyse(elem._2)
          println(rr.toString)
        })
      }

      if (debug == true) {
        println("\n\n")
        var printed = r1.collect().sortBy(_._1)
        //Return the sorted adjacency list, to be used by the order colouring algorithm.
        println(s"Printing the adjmatrix for $i")
        printed.foreach(e => {
          print(e._1 + " ")

          print(e._2.toString()) //printing the roaring matrix
          // e._2.foreach(b => print(b))
          print("\n")
        })
      }

      //Use KP when the graph is sparse (not dense)
      val r2 =
        if (algorithm == "KP") {
          println("Calling the K&P algorithm using the compressed graph chunk...")
          roaringkp.roaringkp.progressiveKP(colors, r1, r1.count().toInt, maxColor, sc)
        }
        else { //algorithm = "OC". The single threaded needs a sorted matrix of adjlists
          println("Calling the Order Coloring algorithm using the compressed graph chunk...")
          ordercoloring(colors, r1.collect().sortBy(_._1), i, maxColor)
        }

      //Update the max color
      totalIterations += r2._1
      maxColor = r2._2
      r1.unpersist(true)
      i += chunkSize

      loop
    }

    val percent = ((totalIterations.toDouble / count.toDouble) * 100)
    val vertexPerIteration = count / totalIterations
    println(s"We did a total of $totalIterations iterations, which is $percent% of total")
    println(s"We also colored $vertexPerIteration vertices per iteration on average")

    //Create tests now
    val bcastcolors = sc.broadcast(colors)
    val properFormRDD: RDD[(Int, clause)] = clausesNumbered.map(elem => {
      val id = elem._2
      val color = bcastcolors.value(id.toInt)
      (color, elem._1)
    })

    //Transform into tests
    coloringToTests(properFormRDD)
  }

  /**
   * The distributed graph coloring algorithm with roaring bitmaps and phiway coverage
   * This function starts the whole process. It is called from the cmdline function
   *
   * @param n
   * @param t
   * @param v
   * @param sc
   * @return
   */
  def start_graphcoloring_phiway(clausesFile: String, t: Int = 0, sc: SparkContext,
                                 chunkSize: Int = 4000, algorithm: String = "OrderColoring"):
  Array[String] = {

    //Read the clauses and the domain sizes
    val clauses = readPhiWayClauses(clausesFile)
    val number = clauses.size
    var clausesRDD = sc.makeRDD(clauses)

    println("Phi-way testing using")
    println("Distributed Graph Coloring with Roaring bitmaps")
    println(s"Using a chunk size = $chunkSize vertices and algorithm = $algorithm")
    println(s"Using a set of phiway clauses instead of interaction strength")
    println(s"Problem: $clausesFile with $number clauses")

    //If t parameter is valid
    if (t > 1 && t < domainSizes.length) {
      println(s"Interaction strength t=$t clauses are being added using RDD.union right now...")
      clausesRDD = clausesRDD.union(fastGenClauses(t, sc))
    }

    import java.io._
    val pw = new PrintWriter(new FileOutputStream(filename, true))

    var t1 = System.nanoTime()

    val tests = phiway_coloring(clausesRDD, sc, chunkSize, algorithm)

    var t2 = System.nanoTime()
    var time_elapsed = (t2 - t1).toDouble / 1000000000

    pw.append(s"$clausesFile;GRAPHCOLORING_PHIWAY;$time_elapsed;${tests.size}\n")
    println(s"$clausesFile;GRAPHCOLORING_PHIWAY;$time_elapsed;${tests.size}\n")
    pw.flush()

    //If the option to save to a text file is activated
    import cmdlineparser.TSPARK.save
    if (save == true) {
      println(s"Saving the test suite to a file named ${clausesFile}results.txt")
      //Save the test suite to file
      saveTestSuite(s"${clausesFile}results.txt", tests)
    }

    //Return the test suite
    tests
  }

  /** Petits tests de phiway coloring */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("Phi-way graph coloring")
      .set("spark.driver.maxResultSize", "0")
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")

    val tests = start_graphcoloring_phiway("clauses2.txt", 0, sc, 6)
    tests.foreach(println)
  }


}
