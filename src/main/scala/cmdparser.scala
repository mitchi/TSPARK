package cmdlineparser
import org.backuity.clist._

object Main {

  // this lets you be exhaustive when matching on the command return
  sealed trait MyCommand extends Command

  object graphviz extends Command(name = "graphviz", description = "Graphviz graph coloring") {

    var colorings = opt[Int](name = "toto", description = "toto")
  }


  object d_ipog_coloring extends Command(name = "dic",
    description = "distributed ipog coloring") with MyCommand {

    var t = opt[Int](name = "t",
      description = "interaction strength")

    var n = opt[Int](name = "n",
      description = "number of parameters")

    var v = opt[Int](name = "n",
      description = "domain size")


    var hstep = opt[Int](name = "hstep", description = "Number of parameters of tests to extend in parallel")

  }

  object coloring extends Command(name = "coloring", description = "distributed graph coloring") with MyCommand {

    var t = arg[Int](name = "t",
      description = "interaction strength")

    var n = arg[Int](name = "n",
      description = "number of parameters")

    var v = arg[Int](name = "v",
      description = "domain size")

    var colorings = opt[Int](name = "colorings", description = "Number of parallel graph colorings to run")
  }


  trait GlobalOptions {
    this: Command =>
    var opt1 = opt[Boolean](name = "1",
      description = "This is a wonderful command")
    var opt2 = opt[String](description = "Man you should try this one",
      default = "haha")
  }

  def main(args: Array[String]) {

    Cli.parse(args).version("1.0.0").withCommands(d_ipog_coloring, coloring, graphviz) match {

      case Some(coloring) => println("coloring")

      case Some(d_ipog_coloring) => println("ipog")

      case None =>
        println("nothing done")
    }
  }
}