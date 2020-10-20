package cmdlineparser

import java.io.File
import java.nio.file.Paths

import cmdline.MainConsole.args2maps
import org.apache.spark.{SparkConf, SparkContext}
import org.backuity.clist._
import enumerator.distributed_enumerator._

import Console._

object TSPARK {

  //Support for colors in Windows with the ANSICON executable.
  //https://stackoverflow.com/questions/16755142/how-to-make-win32-console-recognize-ansi-vt100-escape-sequences
  //http://adoxa.altervista.org/ansicon/

  // this lets you be exhaustive when matching on the command return
  sealed trait CommonOpt {
    this: Command => // same as above
  }

  //https://docs.scala-lang.org/tour/self-types.html

  object Graphviz extends Command(name = "graphviz", description = "graph coloring from a GraphViz file") with CommonOpt {

    var st = opt[Boolean](name = "st", abbrev = "s", description = "use single threaded coloring")
    var colorings = opt[Int](name = "colorings", description = "Number of parallel graph colorings to run", default = 6)
    var filename = arg[String](name = "filename", description = "file name of the .dot file")
    var memory = opt[Int](name = "memory", description = "memory for the graph structure on the cluster in megabytes")
  }

  object edn extends Command(name = "edn", description = "hypergraph covering from a file (edn format)") {
    var filename = arg[String](name = "filename", description = "file name of the .dot file")
  }

  //Nouvel objet pour Distributed IPOG avec Roaring
  object D_ipog_coloring_roaring extends Command(name = "dicr",
    description = "Distributed Ipog-coloring") with CommonOpt {

    var t = arg[Int](name = "t",
      description = "interaction strength")

    var n = arg[Int](name = "n",
      description = "number of parameters")

    var v = arg[Int](name = "v",
      description = "domain size")

    var hstep = opt[Int](name = "hstep", description = "Number of parameters of tests to extend in parallel", default = -1)
    var verify = opt[Boolean](name = "verify", abbrev = "v", description = "verify the test suite")
    var algorithm = opt[String](name = "algorithm", description = "Which algorithm to use (KP or OC)", default = "OC")
    var chunkSize = opt[Int](name = "chunksize", description = "Chunk size, in vertices. Use a bigger chunk if you have more RAM", default = 20000)
    var colorings = opt[Int](name = "colorings", description = "Number of parallel graph colorings to run (when using OC)", default = 0)

    var seeding = opt[String](name = "seeding", description = "Seeding at param,file", default = "")

  }

 //Single threaded coloring with Order Coloring
  object Color extends Command(name = "color",
    description = "single threaded graph coloring") with CommonOpt {

    var t = arg[Int](name = "t",
      description = "interaction strength")

    var n = arg[Int](name = "n",
      description = "number of parameters")

    var v = arg[Int](name = "v",
      description = "domain size")

    var colorings = opt[Int](name = "colorings", description = "Number of parallel graph colorings to run", default = 6)
    var verify = opt[Boolean](name = "verify", abbrev = "v", description = "verify the test suite")
  }


  object D_ipog_coloring extends Command(name = "dic",
    description = "distributed ipog coloring") with CommonOpt {

    var t = arg[Int](name = "t",
      description = "interaction strength")

    var n = arg[Int](name = "n",
      description = "number of parameters")

    var v = arg[Int](name = "v",
      description = "domain size")

    var hstep = opt[Int](name = "hstep", description = "Number of parameters of tests to extend in parallel")
    var verify = opt[Boolean](name = "verify", abbrev = "v", description = "verify the test suite")
    var st = opt[Boolean](name = "st", abbrev = "s", description = "use single threaded coloring")
    var colorings = opt[Int](name = "colorings", description = "Number of parallel graph colorings to run", default = 6)
  }

  object D_ipog_hypergraph extends Command(name = "dih",
    description = "distributed ipog hypergraph cover") with CommonOpt {

    var t = arg[Int](name = "t",
      description = "interaction strength")

    var n = arg[Int](name = "n",
      description = "number of parameters")

    var v = arg[Int](name = "v",
      description = "domain size")

    var hstep = opt[Int](name = "hstep", description = "Number of parameters of tests to extend in parallel")
    var verify = opt[Boolean](name = "verify", abbrev = "v", description = "verify the test suite")
    var vstep = opt[Int](name = "vstep", description = "Covering speed (optional)", default = -1)
  }

  //Distributed Graph Coloring
  object Coloring extends Command(name = "dcolor", description = "distributed graph coloring") with CommonOpt {

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
      .withCommands(Graphviz, edn, Color, D_ipog_coloring_roaring, D_ipog_hypergraph, Coloring, Hypergraphcover, Tway, Pv)


    //Create the Spark Context if it does not already exist
    //The options of Spark can be set using the params of the program
    val conf = new SparkConf().setMaster("local[*]").setAppName("TSPARK")
    val sc = SparkContext.getOrCreate(conf) //Create a new SparkContext or get the existing one (Spark Submit)
    sc.setLogLevel("OFF")

    import central.gen.singlethreadcoloring
    import central.gen.verifyTS
    import central.gen.simple_hypergraphcover
    import central.gen.distributed_graphcoloring
    import ipog.d_ipog._
    import utils.utils.print_combos_in_order
    import ipog.d_ipog_roaring.distributed_ipog_coloring_roaring

    choice match {

      //Distributed IPOG Coloring roaring bitmaps
      case Some(D_ipog_coloring_roaring) => {

        val n = D_ipog_coloring_roaring.n
        val t = D_ipog_coloring_roaring.t
        val v = D_ipog_coloring_roaring.v

        var hstep = D_ipog_coloring_roaring.hstep
        var verify = D_ipog_coloring_roaring.verify
        val colorings = D_ipog_coloring_roaring.colorings
        var chunkSize = D_ipog_coloring_roaring.chunkSize
        var algorithm = D_ipog_coloring_roaring.algorithm


        import cmdline.MainConsole.readSeeding
        import cmdline.resume_info
        var seeding = D_ipog_coloring_roaring.seeding

        val resume = if (seeding != "") {
          Some(readSeeding(seeding))
        } else None

        val tests = distributed_ipog_coloring_roaring(n, t, v, sc, colorings, hstep, resume, chunkSize, algorithm)

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
        val colorings = D_ipog_coloring.colorings

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
} //fin object main




