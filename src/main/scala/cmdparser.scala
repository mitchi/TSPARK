package cmdlineparser

import org.apache.spark.{SparkConf, SparkContext}
import org.backuity.clist._

object Main {

  // this lets you be exhaustive when matching on the command return
  sealed trait Common {
    this: Command => // same as above
  }

  object graphviz extends Command(name = "graphviz", description = "graph coloring from a GraphViz file") {

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

  import Console.UNDERLINED, Console.BLUE
  import Console.RESET
  def main(args: Array[String]) {
    println(s"$BLUE TSPARK v 1.0$RESET")

    println("")

    val choice = Cli.parse(args).version("1.0.0").withCommands(D_ipog_coloring, Coloring, graphviz, edn, Hypergraphcover, Color, Tway, Pv)


    //Create the Spark Context for all apps
    val conf = new SparkConf().setMaster("local[*]").setAppName("TSPARK")
    val sc = SparkContext.getOrCreate(conf) //Create a new SparkContext or get the existing one (Spark Submit)
    sc.setLogLevel("OFF")

    choice match {

      case Some(Pv) =>
        generateParameterVectors(sc, Pv.n, Pv.t)
      case None =>
        println("nothing done")
    }
  } //fin main function
} //fin object main

