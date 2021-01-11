//package Totem
//
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//import phiway.phiway.clause
//import phiway_hypergraph.phiway_hypergraph.{clauseToTests, greedyPicker, progressive_filter}
//import scala.collection.mutable.ArrayBuffer
//
//import phiway.phiway.domainSizes
//
//object Totem extends Serializable
//{
//
//
//  /**
//    * Create a new totem with n parameters, and domainSizes(i) values
//    * @param n
//    */
//  def newTotem(n: Int)= {
//    var totem: Array[Array[Short]] = new Array[ Array[Short]](n)
//    //Init totem
//    for (jj <- 0 until totem.length) {
//      totem(jj) = new Array[Short](domainSizes(jj))
//    }
//    totem
//  }
//
//
//  /**
//    *  This function mutates totem directly
//    *  Test in string form is like ;value;value;value
//    *  Il faudrait faire une fonction de génération qui retourne juste des valeurs
//    * @param test
//    */
//  def placeTestInTotem(test: String, totem: Array[Array[Short]]): Unit =
//  {
//    var i = 0
//    val values = test.split(";").map(_.toShort)
//
//      //For each value in test
//      values.foreach( v => {
//        totem(i)(v) += 1
//        i+=1
//      })
//  }
//
//  /**
//    * Create the best test from the totem
//    * @param totem
//    */
//  def testFromTotem(totem : Array[Array[Short]],
//                    limit : Int): Unit = {
//
//
//  }
//
//
//  /**
//    * Greedy algorithm for hypergraph covering
//    * Every hyperedge is a phi-way clause.
//    * Every phiway clause votes for tests, every iteration
//    * The best tests are picked using an algorithm called greedypicker.
//    * The greedypicker algorithm picks a diverse set of tests.
//    *
//    * @param sc
//    * @param v
//    * @param rdd
//    * @return
//    */
//  def greedy_totem(sc: SparkContext,
//                      rdd: RDD[clause],
//                   n : Int) = {
//    //sc.setCheckpointDir(".") //not used when using local checkpoint
//    val randomGen = scala.util.Random
//    //.cache()
//    var logEdgesChosen = ArrayBuffer[String]()
//    var counter = 1
//    var currentRDD = rdd
//    val sizeRDD = rdd.count()
//
//    //Get the number of partitions
//    val num_partitions = rdd.getNumPartitions
//    println("Number of partitions : " + num_partitions)
//
//    //Max picks = 1 percent of the size
//    var maxPicks = sizeRDD / 100
//    // if (maxPicks < 500) maxPicks = 500
//    if (maxPicks < 5) maxPicks = 2
//
//    println("Repartitioning...")
//    currentRDD = currentRDD.repartition(num_partitions * 2)
//    println("Done")
//
//    def loop(): Unit = {
//      while (true) {
//
//        counter += 1
//        println("Iteration : " + counter)
//
//        //Condition de fin, le RDD est vide
//        if (currentRDD.isEmpty()) return
//
//        //Trouver le sommet S qui est présent dans le plus de tTests (Transformation)
//        val s1: RDD[Array[Short]] = currentRDD.mapPartitions(partition =>
//        {
//
//          var totem: Array[Array[Short]] = new Array[ Array[Short]](n)
//          //Init totem
//          for (jj <- 0 until totem.length) {
//            totem(jj) = new Array[Short](domainSizes(jj))
//          }
//
//          partition.foreach(clause =>
//          {
//            val list = clauseToTests(clause) //faire une version qui retourne Array[Short]
//            list.foreach(elem => {
//
//              placeTestInTotem(elem, totem)
//
//            })
//          })
//          totem.toIterator
//        }
//        )
//
//
//        val s2 = s1.reduce( (a,b) => {
//          val ttt = for {
//             val1 <- a
//             val2 <- b
//          } yield (val1+val2).toShort
//          ttt
//        })
//
//        var res = s1.reduceByKey((a, b) => a + b).collect()
//
//        case class bt(var test: String = "", var count: Int = 0)
//        var bestCount = 0
//        var MTests = new ArrayBuffer[String]()
//        var Mpicks = 0
//
//        //First we find the best count
//        //https://docs.scala-lang.org/overviews/parallel-collections/overview.html
//        res.foreach(elem => {
//          if (elem._2 > bestCount) {
//            bestCount = elem._2
//          }
//        })
//
//        //   Then we can pick as much as M such counts
//        res.foreach(elem => {
//          if (elem._2 == bestCount && Mpicks != maxPicks) {
//            //println(s"We pick a test here. The best count is $bestCount")
//            val toKeep = (elem._1)
//            MTests += toKeep
//            Mpicks += 1
//          }
//        })
//
//        println("Picking the tests")
//        val chosenTests = greedyPicker(MTests)
//        println(s"We have chosen " + chosenTests.size + " tests in this iteration using the greedy picker algorithm")
//
//        //Quand on enleve 1000 tests a la fois, c'est trop long...
//        //Il faut en enlever moins a la fois.
//        currentRDD = progressive_filter(chosenTests, currentRDD, sc)
//
//        //Add the M tests to logEdgesChosen
//        chosenTests.foreach(test => {
//          logEdgesChosen += test
//        })
//
//        //Checkpoint every 3 iterations
//        currentRDD = currentRDD.localCheckpoint()
//
//      } //fin du while
//    } //fin fct loop
//
//    loop()
//    // CustomLogger.logger.info(s"ITERATION NUMBER : $counter")
//    logEdgesChosen.toArray
//
//
//  } //fin greedy_totem
//
//
//} //Fin object totem
