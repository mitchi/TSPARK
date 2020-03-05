package cmdline

import org.apache.spark.{SparkConf, SparkContext}
import enumerator.distributed_enumerator._
import ipog.d_ipog_coloring._

case class resume_info(var tests: Array[Array[Char]], var param: Int)

object MainConsole {

  import generator.gen._
  import utils.utils._

  val SPARKVERSION = "2.4.4"
  val VERSION = "1.0.0"
  val HEADER = s"TSPARK $VERSION (c) 2019 Edmond LA CHANCE. Developped using Spark Version : $SPARKVERSION"
  val FOOTER = "See results.txt file. For all other tricks, consult the documentation"

  def readSeeding(s: String): resume_info = {
    var res = s.split(",")
    var targetParam = res(0).toInt
    var filename = res(1)
    resume_info(readTestSuite(filename), targetParam)
  }

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      showConsoleHelp()
      System.exit(0)
    }

    if (args.contains("--help")) {
      showConsoleHelp()
      System.exit(0)
    }

    else if (args.contains("--version")) {
      showVersion()
      System.exit(0)
    }

    val map_parameters = args2maps(args)

    //Support for covering EDN hypergraphs
    if (args.contains("--edn")) {
      tryEDN(map_parameters)
      System.exit(0)
    }

    //Support for coloring GraphViz graphs (WIP)
    if (args.contains("--graphviz")) {
      tryGraphVizColoring(map_parameters)
      System.exit(0)
    }

    if (args.contains("--verify")) {
      showVersion()
      System.exit(0)
    }

    val algo_type = map_parameters("type")

    algo_type match {
      case "pv" => tryGeneratePV(map_parameters)
      case "tway" => tryGenerateVC(map_parameters)
      case "combos" => printCombosInOrder(map_parameters)
      case "hypergraphcover" => tryGenerateSETCOVER(map_parameters)
      case "graphcoloring" => tryGenerateColoring(map_parameters) //Distributed Graph Coloring
      case "ordercoloring" => tryGenerateSIMPLECOLORING(map_parameters)
      case "distributed_ipog_coloring" => handle_distributed_ipog_coloring(map_parameters)
      case "distributed_ipog_hypergraph" => handle_distributed_ipog_hypergraph(map_parameters)
      case _ => println("Output type unknown, please check --type parameter")
    }
  }


  //Offrir également une fonctionnalité de Graph coloring? Peut etre c'est mieux de juste faire un autre programme.
  def showConsoleHelp(): Unit = {
    println(HEADER)
    println(
      """Help :
        |
        |--type pv|tway pv outputs parameter vectors (t and n only) and tway outputs value combinations
        |--type combos outputs the combos for each parameter, in order
        |
        |Distributed algorithms:
        |--type distributed_ipog_coloring|distributed_ipog_hypergraph|graphcoloring|hypergraphcover
        |
        |Algorithm selection for the graph coloring algorithms in IPOG or elsewhere (KP or OC)
        |--coloring KP
        |
        |How much memory for adjlists (required for graph coloring algorithms)
        |--memory XXXXX
        |
        |Single-threaded algorithms (does not use the memory parameter):
        |--type ordercoloring
        |--numbercolorings N  number of graph colorings to perform in distributed. Default is 6. Used with Order Coloring.
        |
        |Traditional graph coloring (From a graph detailed in a graphviz file)
        |Default memory is 500 mb
        |--graphviz filename
        |
        |Set the covering speed for the horizontal growth of IPOG here (Horizontal Growth). Default speed is 1/100th of the test size.
        |--hstep X
        |Set the covering speed for the Set Cover algorithm here. 1 is slower but gives bette results.  (Vertical Growth)
        |--vstep X
        |
        |--verify filename       (Use this to verify a test suite. Specify the t,n and v parameters beforehand)
        |--resume parameter,filename
        |--save true|false (default false) to save the test suite at every parameter to a file.
        |--localdir path   (Set the path for shuffle files. Choose a big disk for this!
        |
        |--t N    interaction strength. Default is 2
        |--n N    number of variables. Default is 3
        |--v N    number of values per variable. Default is 2

        |--print  true|false. default is true
        |--file filename (appends the output to filename.txt)
        |--checkpointdir [dir]  (Very optional, used in kpcoloring for big problems)
        |
        |--ts N/T/V/N/T/V  to run a test suite. First series is initialValues, second series is maxValues. Specify the algorithm with --type
        |--all N/T/V/N/T/V run the best algorithms with the parameter range
        |
        |Tip :
        |To increase the memory available for spark put the java parameter -Xmx before any other parameter
        |For exemple to use 4Go of RAM, type : java -Xmx4G -jar <jar path> ...
        |
        |For some algorithms like Graph Coloring, Spark may use the hard drive to store checkpoint files (memory dumps).
        |You can select a better place for these checkpoint files with : --setcheckpointdir [path]
        |
      """.stripMargin)
    println(FOOTER)
  }

  def showVersion(): Unit = {
    println(HEADER)
    println(
      """Version : 1.0.0
      """.stripMargin
    )
  }

  //  def configFile2args(path: String): Option[Array[String]] = {
  //    if (File(Path(path)).exists) {
  //      val input = Source.fromFile(path).getLines()
  //      val res = input.flatMap(_.trim().split(" "))
  //      Some(res.toArray)
  //    }
  //    else {
  //      None
  //    }
  //  }

  def args2maps(args_array: Array[String]): Map[String, String] = {
    // Input array : [--<parameter name>, <value>, --<parameter name>, <value>,...]

    val args_sanitize = args_array.map { arg =>
      val arg_sanitize = arg.replaceAll("--", "")
      arg_sanitize.trim()
    }
    val args_pairs = args_sanitize.grouped(2)

    // Then we extract the key, value to create a map
    val toto = args_pairs.map { case Array(k, v) => k -> v }.toMap
    toto
  }

  /**
    * Try to run a graph coloring algorithm from a GraphViz file
    *
    * @param params
    */
  def tryGraphVizColoring(params: Map[String, String]) = {

    try {

      val filename = params("graphviz")
      val coloring_algorithm = if (params.contains("coloring"))
        params("coloring")
      else "OC"

      val memory = if (params.contains("memory"))
        params("memory").toInt
      else 500 //default is to use 500 megabytes to color the graph

      val conf = new SparkConf().setMaster("local[*]").setAppName("GraphViz graph coloring")
      val sc = SparkContext.getOrCreate(conf) //Create a new SparkContext or get the existing one (Spark Submit)
      sc.setLogLevel("OFF")
      import graphviz.graphviz_graphs._
      val a = graphcoloring_graphviz(filename, sc, memory, coloring_algorithm)
    }

    catch {
      case e: Exception => {
        println(s"Exception catched : ${e.getMessage}")
      }
    }

  }


  /**
    * Solve EDN hypergraph
    *
    * @param params
    */
  def tryEDN(params: Map[String, String]) = {

    try {

      val filename = params("edn")
      val conf = new SparkConf().setMaster("local[*]").setAppName("EDN Hypergraph vertex cover solver")
      val sc = SparkContext.getOrCreate(conf) //Create a new SparkContext or get the existing one (Spark Submit)
      sc.setLogLevel("OFF")
      solve_edn_hypergraph(filename, sc)
    }

    catch {
      case e: Exception => {
        println(s"Exception catched : ${e.getMessage}")
      }
    }

  }

  //Includes the error checking code as well
  def tryGeneratePV(params: Map[String, String]): Unit = {

    try {
      val t = params("t").toInt
      val n = params("n").toInt

      var file = ""

      var print = "true"

      if (params.contains("file")) {
        file = params("file")
      }

      if (params.contains("print")) {
        print = params("print")
      }


      val conf = new SparkConf().setMaster("local[*]").setAppName("Combination generator")
      val sc = SparkContext.getOrCreate(conf) //Create a new SparkContext or get the existing one (Spark Submit)
      sc.setLogLevel("OFF")

      generateParameterVectors(sc, n, t, file, true)

    }

    catch {
      case e: Exception => {
        println("Parsing error, please verify the command")
        println("Type --help for more info")
      }
    }


  } //end trygeneratepv

  def tryGenerateVC(params: Map[String, String]): Unit = {

    try {
      val t = params("t").toInt
      val n = params("n").toInt
      val v = params("v").toInt

      var file = ""

      var print = "true"

      if (params.contains("file")) {
        file = params("file")
      }

      if (params.contains("print")) {
        print = params("print")
      }


      val conf = new SparkConf().setMaster("local[*]").setAppName("Combination generator").set("spark.driver.maxResultSize", "0")
        .set("spark.checkpoint.compress", "true")
      val sc = SparkContext.getOrCreate(conf) //Create a new SparkContext or get the existing one (Spark Submit)
      sc.setLogLevel("OFF")

      generateValueCombinations(sc, n, t, v, file, true)

    }

    catch {
      case e: Exception => {
        println("Parsing error, please verify the command")
        println("Type --help for more info")
      }
    }

  } //end trygeneratevc


  /**
    * Read a graphviz file and color the graph
    *
    * @param params
    */
  def colorGraphViz(params: Map[String, String]): Unit = {
    var filename = ""
    var algorithm = ""

    if (params.contains("graphviz")) {
      filename = params("graphviz")
    }

    //Algorithm KP or OC
    if (params.contains("coloring")) {
      algorithm = params("coloring")
    }


    //Perform graphcoloring on the graphviz file


  }


  //Todo ajouter le support pour écrire dans un fichier etc.
  //todo ajouter support pour debug messages etc etc
  def tryGenerateSETCOVER(params: Map[String, String]): Unit = {

    try {
      val t = params("t").toInt
      val n = params("n").toInt
      val v = params("v").toInt

      var file = ""

      var print = "true"

      if (params.contains("file")) {
        file = params("file")
      }

      if (params.contains("print")) {
        print = params("print")
      }

      val conf = new SparkConf().setMaster("local[*]").setAppName("Hypergraph Set Cover T-WAY COVERING").set("spark.driver.maxResultSize", "0")
        .set("spark.checkpoint.compress", "true")
      val sc = SparkContext.getOrCreate(conf) //Create a new SparkContext or get the existing one (Spark Submit)
      sc.setLogLevel("OFF")

      var vstep = if (params.contains("vstep")) {
        params("vstep").toInt
      }
      else -1

      val tests = simple_setcover(n, t, v, sc, vstep)
      println(s"Printing the ${tests.size} tests")
      tests.foreach(print_helper(_))
    }

    catch {
      case e: Exception => {
        println("Parsing error, please verify the command")
        println("Type --help for more info")
      }
    }

  } //end trygeneratsetcover


  /**
    * Graph Coloring, in Chunks (2 algorithms can be chosen)
    * Coloring the graph by chunks.
    * Allows for the biggest parallelism.
    * There are two versions of this algorithm.
    * In the first version, adjacency data is generated, and is then colored by a single threaded algorithm.
    * In the second version, the Knights and Peasants algorithm is used to color the graph in parallel.
    */
  def tryGenerateColoring(params: Map[String, String]): Unit = {

    try {
      val t = params("t").toInt
      val n = params("n").toInt
      val v = params("v").toInt

      var file = ""

      var print = "true"

      if (params.contains("file")) {
        file = params("file")
      }

      if (params.contains("print")) {
        print = params("print")
      }

      val conf = new SparkConf().setMaster("local[*]").setAppName("TSPARK DISTRIBUTED GRAPH COLORING").set("spark.driver.maxResultSize", "0")
        .set("spark.checkpoint.compress", "true")
      val sc = SparkContext.getOrCreate(conf) //Create a new SparkContext or get the existing one (Spark Submit)
      sc.setLogLevel("OFF")

      var memory = if (params.contains("memory")) {
        params("memory").toInt
      }
      else -1

      var algorithm: String = if (params.contains("coloring")) {
        params("coloring")
      }
      else "KP"

      if (memory == -1) {
        println("Missing the memory parameter (in megabytes)")
        System.exit(1)
      }

      val tests = distributed_graphcoloring(n, t, v, sc, memory, algorithm)


      println(s"Printing the ${tests.size} tests")
      tests.foreach(print_helper(_))
    }

    catch {
      case e: Exception => {
        println("Parsing error, please verify the command")
        println("Type --help for more info")
      }
    }

  } //end trygeneratecoloring


  /**
    * Single thread coloring using the Order Coloring algorithm
    */
  def tryGenerateSIMPLECOLORING(params: Map[String, String]): Unit = {

    try {
      val t = params("t").toInt
      val n = params("n").toInt
      val v = params("v").toInt


      var checkpointdir = "/"

      var file = ""

      var print = "true"

      if (params.contains("checkpointdir")) {
        checkpointdir = params("checkpointdir")
      }

      if (params.contains("file")) {
        file = params("file")
      }

      if (params.contains("print")) {
        print = params("print")
      }

      var colorings = 6
      if (params.contains("numbercolorings")) {
        colorings = params("numbercolorings").toInt
      }

      val conf = new SparkConf().setMaster("local[*]").setAppName("Single thread coloring").set("spark.driver.maxResultSize", "0")
        .set("spark.checkpoint.compress", "true")
      val sc = SparkContext.getOrCreate(conf) //Create a new SparkContext or get the existing one (Spark Submit)
      sc.setLogLevel("OFF")

      val tests = singlethreadcoloring(n, t, v, sc, colorings)

      //Print the tests
      println(s"Printing the ${tests.size} tests:")
      tests.foreach(print_helper(_))

    }

    catch {
      case e: Exception => {
        println("Parsing for Coloring, please verify the command")
        println("Type --help for more info")
      }
    }
  }


  /*
  Distributed IPOG COLORING
   */
  def handle_distributed_ipog_coloring(params: Map[String, String]): Unit = {

    try {
      val t = params("t").toInt
      val n = params("n").toInt
      val v = params("v").toInt

      var checkpointdir = "/"

      var file = ""

      var print = "true"

      if (params.contains("checkpointdir")) {
        checkpointdir = params("checkpointdir")
      }

      if (params.contains("file")) {
        file = params("file")
      }

      if (params.contains("print")) {
        print = params("print")
      }

      var colorings = 6
      if (params.contains("numbercolorings")) {
        colorings = params("numbercolorings").toInt
      }

      //If the parameter is set to true
      if (params.contains("save")) {
        var t = params("save")
        if (t == "true")
          save = true
      }

      var hstep = -1

      //If the parameter mvalue is set to true, we set it here.
      if (params.contains("hstep")) {
        var t = params("hstep").toInt
        hstep = t
      }

      //Grab the info to resume the test suite
      var resume =
        if (params.contains("resume"))
          Some(readSeeding(params("resume")))
        else None

      val conf = new SparkConf().setMaster("local[*]").setAppName("Parallel IPOG").set("spark.driver.maxResultSize", "0")
        .set("spark.checkpoint.compress", "true")
      val sc = SparkContext.getOrCreate(conf) //Create a new SparkContext or get the existing one (Spark Submit)
      sc.setLogLevel("OFF")

      val tests = distributed_ipog_coloring(n, t, v, sc, colorings, hstep, resume)

      //Print the tests
      println(s"Printing the ${tests.size} tests for Parallel IPOG:")
      tests.foreach(print_helper(_))

    }

    catch {
      case e: Exception => {
        println(e)
        println("Parsing for Coloring, please verify the command")
        println("Type --help for more info")
      }
    }
  }


  /*
 Distributed IPOG HYPERGRAPH COVER
  */
  def handle_distributed_ipog_hypergraph(params: Map[String, String]): Unit = {

    try {
      val t = params("t").toInt
      val n = params("n").toInt
      val v = params("v").toInt

      var checkpointdir = "/"

      var file = ""

      var print = "true"

      if (params.contains("checkpointdir")) {
        checkpointdir = params("checkpointdir")
      }

      if (params.contains("file")) {
        file = params("file")
      }

      if (params.contains("print")) {
        print = params("print")
      }

      //If the parameter is set to true
      if (params.contains("save")) {
        var t = params("save")
        if (t == "true")
          save = true
      }

      var resume =
        if (params.contains("resume"))
          Some(readSeeding(params("resume")))
        else None

      //Grab the hstep and vstep parameters
      var hstep = if (params.contains("hstep")) {
        params("hstep").toInt
      }
      else -1

      //Grab vstep
      var vstep = if (params.contains("vstep")) {
        params("vstep").toInt
      }
      else -1


      val conf = new SparkConf().setMaster("local[*]").setAppName("Parallel IPOG Setcover").set("spark.driver.maxResultSize", "0")
        .set("spark.checkpoint.compress", "true")
      val sc = SparkContext.getOrCreate(conf) //Create a new SparkContext or get the existing one (Spark Submit)
      sc.setLogLevel("OFF")

      val tests = distributed_ipog_hypergraph(n, t, v, sc, hstep, vstep, resume)

      //Print the tests
      println(s"Printing the ${tests.size} tests for Parallel IPOG:")
      tests.foreach(print_helper(_))
    }

    catch {
      case e: Exception => {
        println(e)
        println("Parsing for Coloring, please verify the command")
        println("Type --help for more info")
      }
    }
  }


  /**
    * Just printing the parameters in order, in the way that IPOG covers them.
    *
    * @param params
    */
  def printCombosInOrder(params: Map[String, String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("t-way combos generator").set("spark.driver.maxResultSize", "0")
      .set("spark.checkpoint.compress", "true")
    val sc = SparkContext.getOrCreate(conf) //Create a new SparkContext or get the existing one (Spark Submit)
    sc.setLogLevel("OFF")

    val t = params("t").toInt
    val n = params("n").toInt
    val v = params("v").toInt

    print_combos_in_order(n, t, v, sc)
  }


  /** Parser the test suite data
    * N/T/V in order , and theres another 3 for the maximums
    * */
  def testSuiteParser(meat: String) = {
    var tokens = meat.split('/').map(_.toInt)
    Array(tokens(0), tokens(1), tokens(2), tokens(3), tokens(4), tokens(5))
  }

}
