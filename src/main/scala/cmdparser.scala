package cmdlineparser

import org.backuity.clist.util.Read
import org.backuity.clist._

object Main {

  // this lets you be exhaustive when matching on the command return
  sealed trait MyCommand extends Command

  trait enumeratorOptions {
    this: Command =>
    var t = opt[Int](name = "t",
      description = "interaction strength")

    var n = opt[Int](name = "n",
      description = "number of parameters")

    var v = opt[Int](name = "n",
      description = "domain size")
  }

  object d_ipog_coloring extends Command(name = "dic",
    description = "distributed ipog coloring") with enumeratorOptions with MyCommand {

    var hstep = opt[Int](name = "hstep", description = "Number of parameters of tests to extend in parallel")

  }

  object coloring extends Command(name = "coloring", description = "distributed graph coloring") with MyCommand {
    var colorings = opt[Int](name = "colorings", description = "Number of parallel graph colorings to run")
  }


  trait GlobalOptions {
    this: Command =>
    var opt1 = opt[Boolean](name = "1",
      description = "This is a wonderful command")
    var opt2 = opt[String](description = "Man you should try this one",
      default = "haha")
  }

  trait SomeCategoryOptions extends GlobalOptions {
    this: Command =>
    var optA = opt[Int](name = "A", default = 1)
    var optB = opt[Boolean](description = "some flag")
  }

  object Run extends Command(description = "run run baby run") with SomeCategoryOptions with MyCommand {
    var target = arg[String]()
    var runSpecific = opt[Long](default = 123L)
  }

  object Show extends Command(name = "cho",
    description = "show the shit!") with GlobalOptions with MyCommand {
  }

  object Test extends Command with SomeCategoryOptions with MyCommand

  object Person extends MyCommand {
    var name = arg[Name]()
  }

  case class Name(firstName: String, lastName: String)

  implicit val nameRead: Read[Name] = Read.reads[Name]("a Name") { str =>
    val Array(first, last) = str.split("\\.")
    Name(first, last)
  }


  def main(args: Array[String]) {

    Cli.parse(args).version("1.0.0").withCommands(d_ipog_coloring, coloring) match {
      case Some(Run) =>
        println("Executed Run command:")
        println("\t- target : " + Run.target)
        println("\t- specific : " + Run.runSpecific)
        println("\t- opt1 : " + Run.opt2)

      case Some(Show) =>
        println("show")

      case Some(Test) =>
        println("test")

      case Some(Person) =>
        println(Person.name)

      case Some(coloring) =>

      case None =>
        println("nothing done")
    }
  }
}