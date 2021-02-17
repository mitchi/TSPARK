package withoutSpark

import org.apache.spark.{SparkConf, SparkContext}

object testNoSparkv2 extends App {

  import NoSparkv2.withoutSpark

  val conf = new SparkConf().setMaster("local[*]").
    setAppName("Without Spark v2")
    .set("spark.driver.maxResultSize", "10g")
  val sc = new SparkContext(conf)
  sc.setLogLevel("OFF")
  println(s"Printing sc.appname : ${sc.appName}")
  println(s"Printing default partitions : ${sc.defaultMinPartitions}")
  println(s"Printing sc.master : ${sc.master}")
  println(s"Printing sc.sparkUser : ${sc.sparkUser}")
  println(s"Printing sc.resources : ${sc.resources}")
  println(s"Printing sc.deploymode : ${sc.deployMode}")
  println(s"Printing sc.defaultParallelism : ${sc.defaultParallelism}")
  println(s"Printing spark.driver.maxResultSize : ${sc.getConf.getOption("spark.driver.maxResultSize")}")
  println(s"Printing spark.driver.memory : ${sc.getConf.getOption("spark.driver.memory")}")
  println(s"Printing spark.executor.memory : ${sc.getConf.getOption("spark.executor.memory")}")
  println(s"Printing spark.serializer : ${sc.getConf.getOption("spark.serializer")}")
  println(s"Printing sc.conf : ${sc.getConf}")
  println(s"Printing boolean sc.islocal : ${sc.isLocal}")
  println("Without Spark Version 2")

  var n = 8
  var t = 7
  var v = 4

  import cmdlineparser.TSPARK.compressRuns

  compressRuns = false
  val maxColor = withoutSpark(n, t, v, sc, 100000, "OC") //4000 pour 100 2 2
  println("We have " + maxColor + " tests")

}
