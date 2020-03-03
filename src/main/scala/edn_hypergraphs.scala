package edn

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

/**
  * Edmond La Chance UQAC 2020
  * This file contains a singleton object that handles EDN hypergraphs, and applies the set cover algorithm on it.
  */

object edn_hypergraphs {

  /**
    * This is a version of the setcover function
    * It returns the number of tests needed to cover a hypergraph
    *
    * @param sc
    * @param v
    * @param rdd
    * @return
    */
  def hypergraph_edn(sc: SparkContext, rdd: RDD[Array[String]]): Int = {
    val randomGen = scala.util.Random
    var logEdgesChosen = ArrayBuffer[String]()
    var counter = 1
    val sizeRDD = rdd.count()
    var maxPicks = sizeRDD / 1000
    if (maxPicks < 500) maxPicks = 500

    var currentRDD = rdd

    def loop(): Unit = {
      while (true) {

        counter += 1
        //println("Iteration : " + counter)

        //Condition de fin, le RDD est vide
        if (currentRDD.isEmpty()) return

        //Initial aggregation with the hash tables
        val s1 = currentRDD.mapPartitions(partition => {
          var hashmappp = scala.collection.mutable.HashMap.empty[String, Int]
          partition.foreach(arr => {
            arr.foreach(elem => {
              val key = elem
              if (hashmappp.get(key).isEmpty) //if entry is empty
                hashmappp.put(key, 1)
              else
                hashmappp(key) += 1
            })
          })
          Iterator(hashmappp)
        }
        )

        //Shuffle the data to the right reducer (network transfers), and perform final aggregation with ReduceByKey
        var res = s1.flatMap(hash => hash.toSeq).reduceByKey((a, b) => a + b).collect()

        case class bt(var test: String = "", var count: Int = 0)
        var bestCount = 0
        var MTests = new ArrayBuffer[String]()
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
            val toKeep = elem._1
            MTests += toKeep
            Mpicks += 1
          }
        })

        //println("Picking a random test in the collection")
        val chosenTest = randomPicker(MTests)

        //Filter the hyperedges using this vertex
        currentRDD = currentRDD.flatMap(he => {
          def loop(): Boolean = {
            he.foreach(e => {
              if (e == chosenTest) return true
            })
            return false
          }

          val answer = loop()
          if (answer == false) Some(he)
          else None
        })

        //Add the test
        logEdgesChosen += chosenTest

        if (counter % 3 == 0)
          currentRDD.localCheckpoint()

      } //fin du while
    } //fin fct loop

    loop()
    // CustomLogger.logger.info(s"ITERATION NUMBER : $counter")
    logEdgesChosen.size
  }

  def randomPicker(choices: ArrayBuffer[String]) = {
    val choice = scala.util.Random.nextInt(choices.size)
    choices(choice)
  }

}
