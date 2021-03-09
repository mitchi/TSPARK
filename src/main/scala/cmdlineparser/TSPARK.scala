package cmdlineparser
import cmdline.resume_info
import enumerator.distributed_enumerator.{fastGenCombos, generateParameterVectors, generateValueCombinations}
import enumerator.enumerator.localGenCombos
import org.apache.spark.{SparkConf, SparkContext}
import org.backuity.clist.{Cli, Command, arg, opt}
import withoutSpark.NoSparkv5.fastVerifyTestSuite

import scala.Console.println

/**
  * On utilise la classe Scala CLIST pour générer l'inteface commandline sans trop se forcer
  * Par contre, je n'ai pas réussi a gérer les options globales :(
  */
object TSPARK {

  var useKryo = false
  var save = false //global variable. Other functions import this
  var logLevelError = true
  var compressRuns = false //global variable, imported and used by Distributed Coloring
  var resume: Option[resume_info] = None //Contains a file and a parameter. Used to resume a computation for IPOG

  //Support for colors in Windows with the ANSICON executable.
  //https://stackoverflow.com/questions/16755142/how-to-make-win32-console-recognize-ansi-vt100-escape-sequences
  //http://adoxa.altervista.org/ansicon/

  // this lets you be exhaustive when matching on the command return
  sealed trait CommonOpt {
    this: Command => // same as above
  }

  //https://docs.scala-lang.org/tour/self-types.html


  object Phiwayparser extends Command(name = "phiway", description = "Phi-way testing from a list of clauses") with CommonOpt {

    var filename = arg[String](name = "clauses", description = "filename for the list of clauses")
    var algo = opt[String](name = "algorithm", description = "algorithm to use: OC,KP or HC", default = "OC")
    //var help = opt[Boolean](name = "help", description = "Display help for phiway")
    var chunkSize = opt[Int](name = "chunksize", description = "Chunk of vertices to use for graph coloring. Default is 4000", default = 4000)
    var save = opt[Boolean](name = "save", description = "Save the test suite to a text file")
    var t = opt[Int](name = "t", description = "Generate and join additional clauses using interaction strength t", default = 0)
  }


  object ExperimentalIPOG extends Command(name = "exp",
    description = "Experimental IPOG coloring") with CommonOpt {

    var t = arg[Int](name = "t",
      description = "interaction strength")

    var n = arg[Int](name = "n",
      description = "number of parameters")

    var v = arg[Int](name = "v",
      description = "domain size of a parameter")

    var compressRuns = opt[Boolean](abbrev = "c", name = "compressRuns", description = "Activate run compression with Roaring Bitmaps", default = false)
    var chunkSize = opt[Int](name = "chunksize", description = "Chunk size in vertices for graph coloring. Default is 20k", default = 20000)
    var hstep = opt[Int](name = "hstep", description = "Number of horizontal covering iterations. Default is 100", default = -1)
    var verify = opt[Boolean](name = "verify", abbrev = "v", description = "verify the test suite")
    var algorithm = opt[String](name = "algorithm", description = "Which algorithm to use (KP or OC)", default = "OC")
    var save = opt[Boolean](name = "save", abbrev = "s", description = "Save the test suite to a file")
    var bitset = opt[Boolean](name = "bitset", abbrev = "b", description = "Use uncompressed bit set as the main data structure")
    var local = opt[Boolean](name = "local", abbrev = "l", description = "Compute without Apache Spark")
  }

  object LocalColoring extends Command(name = "localc",
    description = "No-Spark Graph Coloring using the Fast Graph construction algorithm, and RoaringBitmaps") with CommonOpt {

    var t = arg[Int](name = "t",
      description = "interaction strength")

    var n = arg[Int](name = "n",
      description = "number of parameters")

    var v = arg[Int](name = "v",
      description = "domain size of a parameter")

    var compressRuns = opt[Boolean](abbrev = "c", name = "compressRuns", description = "Activate run compression with Roaring Bitmaps", default = false)
    var chunkSize = opt[Int](name = "chunksize", description = "Chunk size, in vertices. Default is 20k", default = 20000)
    var verify = opt[Boolean](name = "verify", abbrev = "v", description = "verify the test suite")
    var algorithm = opt[String](name = "algorithm", description = "Which algorithm to use (KP or OC)", default = "OC")
    var save = opt[Boolean](name = "save", abbrev = "s", description = "Save the test suite to a file")
    var bitset = opt[Boolean](name = "bitset", abbrev = "b", description = "Use uncompressed bit set as the main data structure")

  }

  object FastColoring extends Command(name = "fastc", description = "Distributed Graph Coloring using the Fast Graph construction algorithm") with CommonOpt {

    var t = arg[Int](name = "t",
      description = "interaction strength")

    var n = arg[Int](name = "n",
      description = "number of parameters")

    var v = arg[Int](name = "v",
      description = "domain size of a parameter")

    var kryo = opt[Boolean](abbrev = "k", name = "kryo", description = "Use Kryo serialization instead")

    var compressRuns = opt[Boolean](abbrev = "c", name = "compressRuns", description = "Activate run compression with Roaring Bitmaps", default = false)
    var chunkSize = opt[Int](name = "chunksize", description = "Chunk size, in vertices. Default is 20k", default = 20000)
    var verify = opt[Boolean](name = "verify", abbrev = "v", description = "verify the test suite")
    var algorithm = opt[String](name = "algorithm", description = "Which algorithm to use (KP or OC)", default = "OC")
    var save = opt[Boolean](name = "save", abbrev = "s", description = "Save the test suite to a file")
  }


  /**
    * Distributed IPOG Coloring avec Roaring Bitmaps
    */
  object D_ipog_coloring_roaring extends Command(name = "dicr",
    description = "Distributed IPOG-Coloring using a compressed graph") with CommonOpt {

    var t = arg[Int](name = "t",
      description = "interaction strength")

    var n = arg[Int](name = "n",
      description = "number of parameters")

    var v = arg[Int](name = "v",
      description = "domain size")

    var hstep = opt[Int](name = "hstep", description = "Speed of horizontal growth", default = -1)
    var chunkSize = opt[Int](name = "chunksize", description = "Chunk size, in vertices. Default is 20k", default = 20000)

    var algorithm = opt[String](name = "algorithm", description = "The Graph Coloring algorithm (OC or KP)", default = "OC")
    var seeding = opt[String](name = "seeding", description = "Resume the algorithm at param,file", default = "")
    var save = opt[Boolean](name = "save", abbrev = "s", description = "Save the test suite after every parameter")
    var verify = opt[Boolean](name = "verify", abbrev = "v", description = "verify the test suite")
    var compressRuns = opt[Boolean](abbrev = "c", name = "compressRuns", description = "Activate run compression with Roaring Bitmaps")

  }


  //Distributed Graph Coloring with Roaring Bitmaps
  object ColoringRoaring extends Command(name = "dcoloring", description = "Distributed Graph Coloring") with CommonOpt {

    var t = arg[Int](name = "t",
      description = "interaction strength")

    var n = arg[Int](name = "n",
      description = "number of parameters")

    var v = arg[Int](name = "v",
      description = "domain size of a parameter")

    var compressRuns = opt[Boolean](abbrev = "c", name = "compressRuns", description = "Activate run compression with Roaring Bitmaps", default = false)
    var chunkSize = opt[Int](name = "chunksize", description = "Chunk size, in vertices. Default is 20k", default = 20000)
    var verify = opt[Boolean](name = "verify", abbrev = "v", description = "verify the test suite")
    var algorithm = opt[String](name = "algorithm", description = "Which algorithm to use (KP or OC)", default = "OC")

  }

  object Graphviz extends Command(name = "graphviz", description = "graph coloring from a GraphViz file") with CommonOpt {

    var st = opt[Boolean](name = "st", abbrev = "s", description = "use single threaded coloring")
    var colorings = opt[Int](name = "colorings", description = "Number of parallel graph colorings to run", default = 6)
    var filename = arg[String](name = "filename", description = "file name of the .dot file")
    var memory = opt[Int](name = "memory", description = "memory for the graph structure on the cluster in megabytes")
  }

  object edn extends Command(name = "edn", description = "hypergraph covering from a file (edn format)") {
    var filename = arg[String](name = "filename", description = "file name of the .dot file")
  }

  //Single threaded coloring with Order Coloring
  object Color extends Command(name = "color",
    description = "Single Threaded Graph Coloring") with CommonOpt {

    var t = arg[Int](name = "t",
      description = "interaction strength")

    var n = arg[Int](name = "n",
      description = "number of parameters")

    var v = arg[Int](name = "v",
      description = "domain size")

    var colorings = opt[Int](name = "colorings", description = "Number of parallel graph colorings to run", default = 6)
    var verify = opt[Boolean](name = "verify", abbrev = "v", description = "verify the test suite")
  }


  /**
    * CLassic Distributed IPOG Coloring. Parallel graph colorings, 1 processor per graph coloring.
    */
  object D_ipog_coloring extends Command(name = "dic",
    description = "Distributed Ipog-Coloring") with CommonOpt {

    var t = arg[Int](name = "t",
      description = "interaction strength")

    var n = arg[Int](name = "n",
      description = "number of parameters")

    var v = arg[Int](name = "v",
      description = "domain size")

    var hstep = opt[Int](name = "hstep", description = "Number of parameters of tests to extend in parallel", default = -1)
    var verify = opt[Boolean](name = "verify", abbrev = "v", description = "verify the test suite")
    var st = opt[Boolean](name = "singlethreaded", abbrev = "st", description = "use single threaded coloring")
    var colorings = opt[Int](name = "colorings", description = "Number of graph colorings to run", default = 6) //Default should be 1 graph coloring per partition
    var save = opt[Boolean](name = "save", abbrev = "v", description = "Save the test suite to a file")
    var seeding = opt[String](name = "seeding", description = "Seeding at param,file", default = "")
  }

  object D_ipog_hypergraph extends Command(name = "dih",
    description = "Distributed Ipog Hypergraph Cover") with CommonOpt {

    var t = arg[Int](name = "t",
      description = "interaction strength")

    var n = arg[Int](name = "n",
      description = "number of parameters")

    var v = arg[Int](name = "v",
      description = "domain size")

    var hstep = opt[Int](name = "hstep", description = "Number of parameters of tests to extend in parallel", default = -1)
    var verify = opt[Boolean](name = "verify", abbrev = "v", description = "verify the test suite", default = false)
    var vstep = opt[Int](name = "vstep", description = "Covering speed (optional)", default = -1)
  }

  //Distributed Graph Coloring
  object Coloring extends Command(name = "dcolor", description = "Distributed Graph Coloring") with CommonOpt {

    var t = arg[Int](name = "t",
      description = "interaction strength")

    var n = arg[Int](name = "n",
      description = "number of parameters")

    var v = arg[Int](name = "v",
      description = "domain size of a parameter")

    var memory = opt[Int](name = "memory", description = "memory for the graph structure on the cluster in megabytes", default = 500)
    var verify = opt[Boolean](name = "verify", abbrev = "v", description = "verify the test suite")
    var algorithm = opt[String](name = "algorithm", description = "Algorithm (KP or Order Coloring)", default = "OC")

  }


  object Hypergraphcover extends Command(name = "dhgraph", description = "distributed hypergraph covering") with CommonOpt {

    var t = arg[Int](name = "t",
      description = "interaction strength")

    var n = arg[Int](name = "n",
      description = "number of parameters")

    var v = arg[Int](name = "v",
      description = "domain size of a parameter")

    var vstep = opt[Int](name = "vstep", description = "Covering speed (optional)", default = -1)

    var verify = opt[Boolean](name = "verify", abbrev = "v", description = "verify the test suite")

  }


  object Tway extends Command(name = "tway", description = "enumerate t-way combos") with CommonOpt {

    var t = arg[Int](name = "t",
      description = "interaction strength")

    var n = arg[Int](name = "n",
      description = "number of parameters")

    var v = arg[Int](name = "v",
      description = "domain size of a parameter")

    var inOrder = opt[Boolean](name = "order", abbrev = "o", description = "enumerate the combos to cover in parameter order")
  }

  object Pv extends Command(name = "pv", description = "enumerate parameter vectors") with CommonOpt {
    var t = arg[Int](name = "t",
      description = "interaction strength")

    var n = arg[Int](name = "n",
      description = "number of parameters")
  }


  def main(args: Array[String]) {

    //A map for global options
    //val map_parameters = args2maps(args)

    val choice = Cli.parse(args)
      .version("1.0.0")
      .withProgramName("TSPARK")
      .withDescription("a distributed testing tool")
      .withCommands(Phiwayparser, Graphviz, edn, Color, ColoringRoaring, LocalColoring, ExperimentalIPOG, FastColoring, D_ipog_coloring_roaring, D_ipog_coloring, D_ipog_hypergraph, Hypergraphcover, Tway, Pv)

    //Create the Spark Context if it does not already exist
    //The options of Spark can be set using the params of the program
    //val conf = new SparkConf().setMaster("local[*]").setAppName("TSPARK")
    //val sc = SparkContext.getOrCreate(conf) //Create a new SparkContext or get the existing one (Spark Submit)
    //sc.setLogLevel("OFF")

    //https://cdn.vanderbilt.edu/vu-wp0/wp-content/uploads/sites/157/2017/10/26210455/GPU_Cluster4.pdf
    // val spark = SparkSession
    //    .builder
    //  .appName("SparkLR")
    //  .getOrCreate()

    var sc: SparkContext = null

    try {
      sc = SparkContext.getOrCreate()
      if (logLevelError == true)
        sc.setLogLevel("ERROR")
    }
    catch {
      case _ => println("Looks like the program is local, and not launched from a cluster. We will be instantiating a LOCAL[*] CLUSTER")
        val conf = new SparkConf().setMaster("local[*]").setAppName("TSPARK")
        sc = SparkContext.getOrCreate(conf) //Create a new SparkContext or get the existing one (Spark Submit)
        if (logLevelError == true)
          sc.setLogLevel("ERROR")
    }

    //A utiliser absolument pour OrderColoring
    println("Setting spark.driver.maxResultSize to 0")
    sc.getConf.set("spark.driver.maxResultSize", "10g")
    sc.getConf.set("spark.driver.memory", "10g")

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
    println(s"Printing spark.kryoserializer.buffer.max : ${sc.getConf.getOption("spark.kryoserializer.buffer.max")}")
    println(s"Printing spark.kryo.registrator : ${sc.getConf.getOption("spark.kryo.registrator")}")
    println(s"Printing spark.kryo.unsafe : ${sc.getConf.getOption("spark.kryo.unsafe")}")


    println(s"Printing sc.conf : ${sc.getConf}")
    //println(s"Printing spark.conf : ${spark.conf}")
    println(s"Printing boolean sc.islocal : ${sc.isLocal}")

    import central.gen.{distributed_graphcoloring, simple_hypergraphcover, singlethreadcoloring, verifyTS}
    import ipog.d_ipog._
    import ipog.d_ipog_roaring.distributed_ipog_coloring_roaring
    import phiway_hypergraph.phiway_hypergraph._
    import phiwaycoloring.phiway_coloring._
    import utils.utils.print_combos_in_order

    choice match {


      case Some(ExperimentalIPOG) => {

        import dipog.dipog_coloring2.start

        val n = ExperimentalIPOG.n
        val t = ExperimentalIPOG.t
        val v = ExperimentalIPOG.v
        save = ExperimentalIPOG.save
        val chunkSize = ExperimentalIPOG.chunkSize
        val hstep = ExperimentalIPOG.hstep
        val verify = ExperimentalIPOG.verify
        val algorithm = ExperimentalIPOG.algorithm //Default is OC, Order Coloring
        compressRuns = ExperimentalIPOG.compressRuns
        val bitset = ExperimentalIPOG.bitset
        val isLocal = ExperimentalIPOG.local

        val seed = System.nanoTime()
        val tests = start(n, t, v, sc, hstep, chunkSize, algorithm)

        //Verify the test suite (optional)
        if (verify == true) {
          val combos: Array[(Array[Char], Long)] = localGenCombos(n, t, v, seed)
          val answer = fastVerifyTestSuite(tests, n, v, combos)
          if (answer == true) println("Test suite is verified")
          else println("Test suite is not verified")
        }

      }

      //Implémentation de Fast Coloring
      case Some(LocalColoring) => {
        import withoutSpark.NoSparkv5._
        val n = LocalColoring.n
        val t = LocalColoring.t
        val v = LocalColoring.v
        save = LocalColoring.save
        val chunkSize = LocalColoring.chunkSize
        val verify = LocalColoring.verify
        val algorithm = LocalColoring.algorithm //Default is OC, Order Coloring
        compressRuns = LocalColoring.compressRuns
        val bitset = LocalColoring.bitset

        println("Stopping the SparkContext & calling garbage collection")
        sc.stop() //We don't need the SparkContext
        sc = null
        System.gc()

        val seed = System.nanoTime()
        val tests = start(n, t, v, chunkSize, algorithm, seed)

        //Verify the test suite (optional)
        if (verify == true) {
          val combos: Array[(Array[Char], Long)] = localGenCombos(n, t, v, seed)
          val answer = fastVerifyTestSuite(tests, n, v, combos)
          if (answer == true) println("Test suite is verified")
          else println("Test suite is not verified")
        }
      }

      //Implémentation de Fast Coloring
      case Some(FastColoring) => {

        import OXRoaring.RoaringOXColoring2._

        val n = FastColoring.n
        val t = FastColoring.t
        val v = FastColoring.v

        useKryo = FastColoring.kryo

        if (useKryo == true) {
          println("Using the Kryo Serializer...")
          println("Using a Custom registrator for Kryro for Roaring bitmaps")
          println("Using spark.kryoserializer.buffer.max=2047m")

          sc.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //Setting up to use Kryo serializer
          sc.getConf.set("spark.kryo.registrator", "com.acme.MyRegistrator")
          sc.getConf.set("spark.kryoserializer.buffer.max", "2047m")

          sc.getConf.set("spark.kryo.unsafe", "true") //default false
          sc.getConf.set("spark.broadcast.compress", "false")
          sc.getConf.set("spark.checkpoint.compress", "true")
        }
        else println("We are not using Kryo Serialization. Use the parameter --kryo in order to use it")

        save = FastColoring.save
        val chunkSize = FastColoring.chunkSize
        val verify = FastColoring.verify
        val algorithm = FastColoring.algorithm //Default is OC, Order Coloring
        compressRuns = FastColoring.compressRuns

        val tests = start(n, t, v, sc, chunkSize, algorithm)

        //Verify the test suite (optional)
        //        if (verify == true) {
        //          val combos = fastGenCombos(n, t, v, sc)
        //          val a = verifyTS(combos, tests, sc)
        //          if (a == true) println("Test suite is verified")
        //          else println("This test suite does not cover the combos")
        //        }

      }

      case Some(Phiwayparser) => {

        val file = Phiwayparser.filename
        val chunkSize = Phiwayparser.chunkSize
        val algorithm = Phiwayparser.algo
        val doSave = Phiwayparser.save
        if (doSave == true) save = true

        val t = Phiwayparser.t

        val tests = algorithm match {
          case "HC" => {
            println("Using the hypergraph covering algorithm")
            phiway_hypergraphcover(file, sc, t)
          }
          case "KP" => {
            println("Using the Knights and Peasants graph coloring algorithm")
            start_graphcoloring_phiway(file, t, sc, chunkSize, "KP")
          }
          case "OC" => {
            println("Using the Order Coloring graph coloring algorithm")
            start_graphcoloring_phiway(file, t, sc, chunkSize, "OC")
          }
          case _ => {
            println(s"Incorrect algorithm $algorithm")
            Array("")
          }
        }

        tests.foreach(println)

        //val tests = start_graphcoloring_phiway(file, sc, chunkSize, "OC")

      }

      /** Distributed IPOG Coloring using Roaring Bitmaps */
      case Some(D_ipog_coloring_roaring) => {
        import cmdline.MainConsole.readSeeding

        val n = D_ipog_coloring_roaring.n
        val t = D_ipog_coloring_roaring.t
        val v = D_ipog_coloring_roaring.v

        val hstep = D_ipog_coloring_roaring.hstep
        val verify = D_ipog_coloring_roaring.verify
        val chunkSize = D_ipog_coloring_roaring.chunkSize
        val algorithm = D_ipog_coloring_roaring.algorithm

        save = D_ipog_coloring_roaring.save
        compressRuns = D_ipog_coloring_roaring.compressRuns

        if (save == true) println("Save parameter: True. Saving the test suites to files")
        if (compressRuns == true) println("Compress runs activated. Better compression for dense graphs")

        val seeding = D_ipog_coloring_roaring.seeding

        //We set the global variable here
        resume = if (seeding != "") {
          Some(readSeeding(seeding))
        } else None

        val tests = distributed_ipog_coloring_roaring(n, t, v, sc, hstep, chunkSize, algorithm)

        //Verify the test suite (optional)
        if (verify == true) {
          val combos = fastGenCombos(n, t, v, sc)
          val a = verifyTS(combos, tests, sc)
          if (a == true) println("Test suite is verified")
          else println("This test suite does not cover the combos")
        }
      }

      //Distributed IPOG Coloring CLASSIC (with byte array graphs)
      case Some(D_ipog_coloring) => {

        val n = D_ipog_coloring.n
        val t = D_ipog_coloring.t
        val v = D_ipog_coloring.v

        var hstep = D_ipog_coloring.hstep
        var verify = D_ipog_coloring.verify
        var colorings = D_ipog_coloring.colorings

        save = D_ipog_coloring.save //Do we save the generated test suites? This is useful for resuming


        println("Setting number of colorings to the default parallelism number")
        colorings = sc.defaultParallelism

        val tests = distributed_ipog_coloring(n, t, v, sc)

        //Verify the test suite (optional)
        if (verify == true) {
          val combos = fastGenCombos(n, t, v, sc)
          val a = verifyTS(combos, tests, sc)
          if (a == true) println("Test suite is verified")
          else println("This test suite does not cover the combos")
        }
      }

      //Distributed IPOG Hypergraph
      case Some(D_ipog_hypergraph) => {

        val n = D_ipog_hypergraph.n
        val t = D_ipog_hypergraph.t
        val v = D_ipog_hypergraph.v

        var hstep = D_ipog_hypergraph.hstep
        var verify = D_ipog_hypergraph.verify
        var vstep = D_ipog_hypergraph.vstep

        val tests = distributed_ipog_hypergraph(n, t, v, sc, hstep, vstep)


        //Verify the test suite (optional)
        if (verify == true) {
          val combos = fastGenCombos(n, t, v, sc)
          val a = verifyTS(combos, tests, sc)
          if (a == true) println("Test suite is verified")
          else println("This test suite does not cover the combos")
        }

      }

      //Implémentation de ColoringRoaring
      case Some(ColoringRoaring) => {

        import central.gen.distributed_graphcoloring_roaring

        val n = ColoringRoaring.n
        val t = ColoringRoaring.t
        val v = ColoringRoaring.v

        val chunkSize = ColoringRoaring.chunkSize
        val verify = ColoringRoaring.verify
        val algorithm = ColoringRoaring.algorithm //Default is OC, Order Coloring

        compressRuns = ColoringRoaring.compressRuns

        val tests = distributed_graphcoloring_roaring(n, t, v, sc, chunkSize, algorithm)

        //Verify the test suite (optional)
        if (verify == true) {
          val combos = fastGenCombos(n, t, v, sc)
          val a = verifyTS(combos, tests, sc)
          if (a == true) println("Test suite is verified")
          else println("This test suite does not cover the combos")
        }

      }


      //Distributed Graph Coloring
      //Mise a jour avec roaring bitmaps ici
      case Some(Coloring) => {

        val n = Coloring.n
        val t = Coloring.t
        val v = Coloring.v

        val memory = Coloring.memory
        val verify = Coloring.verify
        val algorithm = Coloring.algorithm //Default is OC, Order Coloring

        val tests = distributed_graphcoloring(n, t, v, sc, memory, algorithm)

        //Verify the test suite (optional)
        if (verify == true) {
          val combos = fastGenCombos(n, t, v, sc)
          val a = verifyTS(combos, tests, sc)
          if (a == true) println("Test suite is verified")
          else println("This test suite does not cover the combos")
        }

      }

      //Hypergraph cover algorithm
      case Some(Hypergraphcover) => {
        val n = Hypergraphcover.n
        val t = Hypergraphcover.t
        val v = Hypergraphcover.v

        val vstep = Hypergraphcover.vstep
        val verify = Hypergraphcover.verify

        val tests = simple_hypergraphcover(n, t, v, sc, vstep)

        //Verify the test suite (optional)
        if (verify == true) {
          val combos = fastGenCombos(n, t, v, sc)
          val a = verifyTS(combos, tests, sc)
          if (a == true) println("Test suite is verified")
          else println("This test suite does not cover the combos")
        }


      }

      //Color a graphviz file
      case Some(Graphviz) => {
        import graphviz.graphviz_graphs.graphcoloring_graphviz

        val alg = if (Graphviz.st == true) "OrderColoring"
        else "KP"
        val maxColor = graphcoloring_graphviz(Graphviz.filename, sc, Graphviz.memory, alg)
      }

      //Single threaded coloring (Order Coloring)
      case Some(Color) => {

        val n = Color.n
        val t = Color.t
        val v = Color.v

        val tests = singlethreadcoloring(n, t, v, sc, Color.colorings)

        //Verify the test suite (optional)
        if (Color.verify == true) {
          val combos = fastGenCombos(n, t, v, sc)
          val a = verifyTS(combos, tests, sc)
          if (a == true) println("Test suite is verified")
          else println("This test suite does not cover the combos")
        }

      }

      //Generate value combinations
      case Some(Tway) => {
        val inOrder = Tway.inOrder
        if (inOrder)
          print_combos_in_order(Tway.n, Tway.t, Tway.v, sc)
        else
          generateValueCombinations(sc, Tway.n, Tway.t, Tway.v)
      }

      case Some(Pv) =>
        generateParameterVectors(sc, Pv.n, Pv.t)

      case None =>
        println("nothing done")
    }
  } //fin main function
}
