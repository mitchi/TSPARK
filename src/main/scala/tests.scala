import org.apache.spark.{SparkConf, SparkContext}

import enumerator.distributed_enumerator._
import utils.utils._

object test2 extends App {
  val conf = new SparkConf().setMaster("local[*]").setAppName("Combination generator")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  generateParameterVectors(sc, 10, 3)
}

object test extends App {
  //e.setProblem(5, 2)
  val conf = new SparkConf().setMaster("local[*]").setAppName("Combination generator")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val t = 5
  val n = 10
  val v = 2

  val steps = generate_all_steps(n, t)

  println("Printing all the steps :\n")
  steps.foreach(s => print(s))

  println("\n\n")

  val r1 = sc.makeRDD(steps) //Parallelize the steps
  val r2 = r1.flatMap(step => generate_from_step(step, t)) //Generate all the parameter vectors

  //Print the parameter vectors
  println("Printing the parameter vectors :\n")
  r2.collect.foreach(arr => print_helper(arr))

  val r3 = r2.flatMap(pv => generate_vc(pv, t, v))

  //Aggregate a little something to test the vectors
  val r4 = r3.aggregate(0L)(

    (acc, value) => (acc + value.length),
    (acc1, acc2) => {
      acc1 + acc2
    }
  )
  println(r4)
}

