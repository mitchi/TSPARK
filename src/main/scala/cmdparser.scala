package cmdlineparser

import org.apache.spark.{SparkConf, SparkContext}
import org.backuity.clist._

object TSPARK {

  //Support for colors in Windows with the ANSICON executable.
  //https://stackoverflow.com/questions/16755142/how-to-make-win32-console-recognize-ansi-vt100-escape-sequences
  //http://adoxa.altervista.org/ansicon/

  // this lets you be exhaustive when matching on the command return
  sealed trait Common {
    this: Command => // same as above
  }

  //https://docs.scala-lang.org/tour/self-types.html

  object Graphviz extends Command(name = "graphviz", description = "graph coloring from a GraphViz file") with Common {

    var st = opt[Boolean](name = "st", abbrev = "s", description = "use single threaded coloring")
    var colorings = opt[Int](name = "colorings", description = "Number of parallel graph colorings to run", default = 6)
    var filename = arg[String](name = "filename", description = "file name of the .dot file")
    var memory = opt[Int](name = "memory", description = "memory for the graph structure on the cluster in megabytes")
  }

  object edn extends Command(name = "edn", description = "hypergraph covering from a file (edn format)") {
    var filename = arg[String](name = "filename", description = "file name of the .dot file")
  }


  object Color extends Command(name = "color",
    description = "single threaded graph coloring") with Common {

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
    description = "distributed ipog coloring") with Common {

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

  object Coloring extends Command(name = "dcolor", description = "distributed graph coloring") with Common {

    var t = arg[Int](name = "t",
      description = "interaction strength")

    var n = arg[Int](name = "n",
      description = "number of parameters")

    var v = arg[Int](name = "v",
      description = "domain size of a parameter")


    var memory = opt[Int](name = "memory", description = "memory for the graph structure on the cluster in megabytes", default = 500)
    var verify = opt[Boolean](name = "verify", abbrev = "v", description = "verify the test suite")
  }


  object Hypergraphcover extends Command(name = "dhgraph", description = "distributed hypergraph covering") with Common {

    var t = arg[Int](name = "t",
      description = "interaction strength")

    var n = arg[Int](name = "n",
      description = "number of parameters")

    var v = arg[Int](name = "v",
      description = "domain size of a parameter")

    var vstep = opt[Int](name = "vstep", description = "Covering speed (optional)", default = -1)

    var verify = opt[Boolean](name = "verify", abbrev = "v", description = "verify the test suite")

  }


  object Tway extends Command(name = "tway", description = "enumerate t-way combos") with Common {

    var t = arg[Int](name = "t",
      description = "interaction strength")

    var n = arg[Int](name = "n",
      description = "number of parameters")

    var v = arg[Int](name = "v",
      description = "domain size of a parameter")

    var inOrder = opt[Boolean](name = "order", abbrev = "o", description = "enumerate the combos to cover in parameter order")
  }

  object Pv extends Command(name = "pv", description = "enumerate parameter vectors") with Common {
    var t = arg[Int](name = "t",
      description = "interaction strength")

    var n = arg[Int](name = "n",
      description = "number of parameters")
  }

  import enumerator.distributed_enumerator._
  import Console._
  import Console.RESET
  def main(args: Array[String]) {

    val choice = Cli.parse(args)
      .version("1.0.0")
      .withProgramName("TSPARK")
      .withDescription("a distributed testing tool")
      .withCommands(D_ipog_coloring, Coloring, Graphviz, edn, Hypergraphcover, Color, Tway, Pv)


    //Create the Spark Context for all apps
    val conf = new SparkConf().setMaster("local[*]").setAppName("TSPARK")
    val sc = SparkContext.getOrCreate(conf) //Create a new SparkContext or get the existing one (Spark Submit)
    sc.setLogLevel("OFF")

    import central.gen.singlethreadcoloring
    import central.gen.verifyTS

    choice match {


      case Some(Graphviz) => {
        import graphviz.graphviz_graphs.graphcoloring_graphviz

        val alg = if (Graphviz.st == true) "OrderColoring"
        else "KP"
        val maxColor = graphcoloring_graphviz(Graphviz.filename, sc, Graphviz.memory, alg)
      }

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

      case Some(Tway) => generateValueCombinations(sc, Tway.n, Tway.t, Tway.v)

      case Some(Pv) =>
        generateParameterVectors(sc, Pv.n, Pv.t)

      case None =>
        println("nothing done")
    }
  } //fin main function
} //fin object main


import java.io.File
import java.nio.file.Paths

import org.backuity.clist._

object ProgramInfoDemoTest {

  sealed trait CommonOpt {
    self: Command =>
    var debug = opt[Boolean](description = "debug mode")
    var timeout = opt[Long](default = 500, description = "max network timeout in millisecond")
    var port = opt[Option[Int]](description = "specify listening port for contact")
    var printCode = opt[Boolean](description = "print connection address as hex string.")

    var address = arg[Option[String]](required = false,
      description = "address or connection code, e.g. 127.0.0.1:8088")
  }

  private class Push extends Command(
    description = "publish a file and wait for pulling from client")
    with CommonOpt {
    var file = arg[File](required = true, description = "file to send")
  }

  private class Pull extends Command(
    description = "pull a published file from push node")
    with CommonOpt {
    var destDir =
      arg[File](required = false, default = Paths.get(".").toFile,
        description = "dest dir to save pulled file")
  }

  def main(args: Array[String]): Unit = {
    Cli.parse(args)
      .version("test-version")
      .withProgramName("eft")
      .withDescription("a file transfer tool.")
      .withCommands(new Push, new Pull)
      .foreach {
        printCmd
      }
  }

  private def printCmd(cmd: Command): Unit = {
    println(s"Push cmd executed with options:\n${cmd.options}\n args:\n${cmd.arguments}")
  }
}

