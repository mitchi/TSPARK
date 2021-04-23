package phiway

import enumerator.distributed_enumerator.{generate_all_steps, generate_from_step}
import org.apache.spark.{SparkConf, SparkContext}
import org.roaringbitmap.RoaringBitmap

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * Phi-way testing supports graph coloring and hypergraph vertex covering.
 * We input the Phi-way clauses using a file (1)
 * Or by adding Phi-way clauses to clauses enumerated by interaction strength (2)
 *
 */

object phiway extends Serializable {

  //This array is initialized by the parseClause function
  var domainSizes = Array[Short]()

  sealed abstract class EnsembleOuValeur

  case class Ensemble(var ensemble: RoaringBitmap) extends EnsembleOuValeur

  case class Valeur(valeur: Short) extends EnsembleOuValeur

  case class Rien() extends EnsembleOuValeur

  //Clause class
  case class clauseEOV(var eovs: Array[EnsembleOuValeur]) {
    //Pour utiliser () pour adresser les conds directement
    def apply(i: Int) = eovs(i)
  }


  /** Save test suite, Phiway version
   *
   * @param filename the name of the file
   * @param tests    the test suite
   */
  def saveTestSuite(filename: String, tests: Array[String]): Unit = {
    //Open file for writing
    import java.io._
    val pw = new PrintWriter(new FileOutputStream(filename, false))

    //For all tests. Write them to the file
    for (test <- tests) {
      pw.append(s"$test\n")
      pw.flush()
    }
    //Close the file
    pw.close()
  }

  /**
   * Phi-way version.
   * We copy the combinations inside a pv object, then we clone it.
   *
   * @param combinations
   * @param indexes
   * @param pv
   */
  def copycombinations(combinations: Array[Short],
                       indexes: ArrayBuffer[Int],
                       pv: Array[Short]): Unit = {

    var i = 0

    def loop(): Unit = {

      if (i == combinations.length) return

      //Get the value of the combi
      val valueCombi = combinations(i)
      //get the index
      val index = indexes(i)
      //Replace inside pv
      pv(index) = valueCombi

      i += 1

      loop
    }

    loop
  }


  /**
   * Increment left. Phi-way version
   *
   * @param combinations
   * @return
   */
  def increment_left(combinations: Array[Short]): Boolean = {
    var i: Int = 0
    var theEnd = false //whether or not its over
    //var valueToAdd : Byte = 1

    def loop(): Unit = {

      //We have finished filling the table
      if (i == combinations.length) {
        theEnd = true
        return
      }

      //We can still increment here
      if (combinations(i) + 1 < domainSizes(i)) {
        combinations(i) = (combinations(i) + 1).toShort
        return
      }

      //We cannot increment
      else if (combinations(i) + 1 == domainSizes(i)) {
        combinations(i) = 0
        i += 1
      }

      loop
    }

    loop

    theEnd

  }


  /**
   * Create a new clause, from an array of shorts.
   * -1 indicates that there is no parameter. Everything is is the value combination.
   *
   * @param shorts
   * @return
   */
  def makeNew(shorts: Array[Short]) = {
    //We create a new clause object
    val tmp: Array[booleanCondition] = shorts.map(num => {
      if (num == -1) EmptyParam()
      else booleanCond(operator = '=', value = num)
    })
    clause(tmp)
  }


  /** Main code to generate the value combinations from a paramater vector
   *
   * Algorithm : First we generate a small array called combinations. This is where we enumerate value combinations.
   * Then, we iterate and store the indexes of the parameter vector.
   * We store the value combination in the parameter vector. We clone the parameter vector and store the object in a return array.
   * We generate the next value combination, and repeat this process until every value combo has been enumerated.
   * We return the t-way combos at the end.
   * */
  def generate_value_combinations(tab: Array[Char],
                                  t: Int): ArrayBuffer[clause] = {

    //1. Generate a small array to combine everything
    //Create initial vector
    var combinations = new Array[Short](t)
    for (i <- 0 until t) {
      combinations(i) = 0
    }

    //First we note the index of each parameter = 1. This way, we can access them directly afterwards
    var indexes = new ArrayBuffer[Int]()

    //Now we find all the 1s in the parameter vector. We put their index inside the indexes array
    var i = tab.length - 1
    while (i != -1) {
      if (tab(i) == '1') indexes += i
      i -= 1
    }

    var newtab = tab.map(e => (-1).toShort)
    var results = new ArrayBuffer[clause]()
    var end = false

    while (!end) {

      //Store this combination of values into the parameter vector
      copycombinations(combinations, indexes, newtab)
      results += makeNew(newtab)
      end = increment_left(combinations)
    }
    results
  }


  /**
   * This is the Phi-way version of this function.
   * This function generates Phi-way clauses using the interaction strength of a given problem.
   * We rely on the domainSizes global variable for enumeration.
   * These clauses can then be joined to the other clauses before solving a problem.
   *
   * @param n
   * @param t
   * @param v
   * @param sc SparkContext
   * @return RDD of combos
   */
  def fastGenClauses(t: Int, sc: SparkContext) = {

    val n = domainSizes.length

    //Step 1 : Cover t parameters
    val steps = generate_all_steps(n, t)
    val r1 = sc.makeRDD(steps) //Parallelize the steps
    val r2 = r1.flatMap(step => generate_from_step(step, t)) //Generate all the parameter vectors
    var clausesRDD = r2.flatMap(pv => generate_value_combinations(pv, t)) //removed cache here. Functions have to cache on their own
    clausesRDD
  }

  /**
   * We take a reduced EOV clause, and produce a test with it
   */
  def EOVtoTest(a: clauseEOV): String = {
    var test = ""
    //On separe le test avec des ;
    //Go through all the reduced conditions
    // var index = 0
    for (i <- a.eovs) {

      i match {
        //Si on a un ensemble, on choisit une valeur dans l'ensemble.
        //Ici, je prends toujours la première valeur.
        case Ensemble(ensemble) => {

          if (ensemble.isEmpty) {
            println("ERROR: Impossible boolean condition produces an empty set")
            System.exit(1)
          }

          val c = ensemble.first()
          test += s"$c;"
        }
        case Valeur(valeur) => test += s"$valeur;"
        case Rien() => {
          //Take the first value of the domain size, which is always 0
          test += "0;"
        }
      }
      //  index+=1
    }
    //Remove last ;
    test.substring(0, test.length - 1)
  }


  /**
   * Transform a clause to the new clause type
   * We use the global domainSize variable.
   *
   * @param a
   * @return
   */
  def transformClause(a: clause): clauseEOV = {
    var accumulator = new ArrayBuffer[EnsembleOuValeur]()

    var i = -1
    val result: Array[EnsembleOuValeur] = a.conds.map(cond => {
      i += 1
      condToEOV(cond, i)
    })
    clauseEOV(result)
  }

  /**
   * Merge every condition inside the clauses. Return the final clause
   *
   * @param a
   * @param b
   */
  def mergetwoclauses(a: clauseEOV, b: clauseEOV): clauseEOV = {

    val len = a.eovs.size
    val buffer = new ArrayBuffer[EnsembleOuValeur]()

    for (i <- 0 until len) {
      val c = intersect_two(a(i), b(i)) //merge two conditions on the same parameter
      buffer += c
    }
    clauseEOV(buffer.toArray)
  }


  /**
   * Merge two conditions.
   *
   * @param a
   * @param b
   * @return
   */
  def intersect_two(a: EnsembleOuValeur, b: EnsembleOuValeur): EnsembleOuValeur = {

    (a, b) match {
      case (a: Valeur, b: Ensemble) => {
        return a
      }
      case (a: Valeur, b: Rien) => {
        return a
      }

      case (a: Valeur, b: Valeur) => return a //peu importe, a ou b

      case (a: Rien, b: Valeur) => {
        return b
      }
      case (a: Rien, b: Ensemble) => {
        return b
      }

      case (a: Rien, b: Rien) => return a

      case (a: Ensemble, b: Rien) => {
        return a
      }

      case (a: Ensemble, b: Valeur) => {
        return b
      }

      case (a: Ensemble, b: Ensemble) => {
        a.ensemble.and(b.ensemble) //resultat dans a
        return a
      }
    }

  }

  /**
   * From a cond to an EnsembleOuValeur
   * We use the global domainSize variable
   *
   * @param a   the boolean condition
   * @param ith the ith parameter
   * @return
   */
  def condToEOV(aa: booleanCondition, ith: Int): EnsembleOuValeur = {
    if (aa.isInstanceOf[EmptyParam]) {
      return Rien()
    }

    val a = aa.asInstanceOf[booleanCond]

    if (a.operator == '=') {
      return Valeur(a.value)
    }

    var bitmap = new RoaringBitmap()

    //Grab the domain size of the parameter.
    //Here, we just grab "v"

    if (a.operator == '!') {
      for (i <- 0 until domainSizes(ith)) {
        if (i != a.value) bitmap.add(i)
      }
    }

    else if (a.operator == '<') {
      for (i <- 0 until a.value)
        bitmap.add(i)
    }

    //Else '>'
    else {
      for (i <- a.value + 1 until domainSizes(ith))
        bitmap.add(i)
    }

    Ensemble(bitmap)
  }

  sealed abstract class booleanCondition

  case class EmptyParam() extends booleanCondition {
    override def toString: String = return s"XX"
  }

  //Size : 32 bits (previously, 16 bits)
  case class booleanCond(var operator: Char = 'X', var value: Short = 0) extends booleanCondition {
    //X signifie qu'il n'y a rien. Le parametre est absent
    //16 bit domain size

    override def toString: String = {
      return s"${operator}$value"
    }
  }

  case class clause(conds: Array[booleanCondition]) {

    //Pour utiliser () pour adresser les conds directement
    def apply(i: Int) = conds(i)


    override def toString: String = {
      var output = ""
      for (i <- conds) output += i.toString + " "
      output
    }
  }


  /**
   * Parse a clause into a booleanCond
   *
   * @param clause
   */
  def parseClause(clause: String): booleanCondition = {


    //First we check if there is an operator like < > !. The operator = is default
    val cond = new booleanCond()

    //Empty-clause, means that there is no parameter in the clause. We use default values
    if (clause == "X") {
      return EmptyParam()
    }

    val c = clause(0)

    if (c >= '0' && c <= '9')
      cond.operator = '='
    else if (c == '!')
      cond.operator = '!'
    else if (c == '>')
      cond.operator = '>'
    else if (c == '<')
      cond.operator = '<'

    if (cond.operator == '=')
      cond.value = clause.toShort
    else cond.value = clause.substring(1).toShort

    cond
  }

  /**
   * The first line contains the domain sizes
   * The line has the following structure: d1-d2-d3-d4-d4.
   * With d1,d2 being domain sizes for parameters p1,p2 etc
   *
   * @param firstLine
   */
  def readDomainSizes(firstLine: String) = {
    val domainSizes = firstLine.split("-").map(_.toShort)
    domainSizes
  }

  /**
   * In Phiway testing, the notion of testing strength is replaced with a set of boolean conditions
   * on parameter values.
   *
   * @param filename
   */
  def readPhiWayClauses(filename: String): Array[clause] = {
    var clauses = new ArrayBuffer[clause]()

    //Comment inferer le return??
    def treatLine(line: String): Unit = {
      var conds = new ArrayBuffer[booleanCondition]()

      //Ligne commentaire ou ligne vide
      if (line == "" || line(0) == '#') {
        return
      }

      //les sommets commencent a 1
      val conditions = line.split(";") //Split with ; separator

      conditions.foreach(cond => {
        conds += parseClause(cond)
      })

      clauses += clause(conds.toArray)
    }

    var firstLine = true
    //For all the lines in the file
    for (line <- Source.fromFile(filename).getLines()) {

      if (firstLine == true) {
        //Is this a line with the domain sizes?
        if (line.contains("-") && !line.contains("#")) {
          firstLine = false
          domainSizes = readDomainSizes(line)
        }
      }
      else treatLine(line)
    }

    clauses.toArray
  }

  /**
   * Transform a t-way combo into a proper Phi-way object
   *
   * @param combo
   * @return
   */
  def combo_to_phiway(combo: Array[Char]): booleanCond = {
    val combo2 = new booleanCond()

    for (i <- combo) {
      if (i != '*') {
        combo2.operator = '='
        combo2.value = i.toShort
      }
    }
    combo2
  }


  /**
   * This function answers the question: Are these two boolean conditions compatible, meaning that they can cohabit
   * inside the same test.
   *
   * =5 and =5 is compatible
   * =4 and =5 is not compatible
   *
   * <3 and <5 is compatible
   * etc
   *
   * @param a
   * @param b
   */
  def compatible(c1: booleanCondition, c2: booleanCondition): Boolean = {
    //If one of the two conditions is the EmptyParam we return false
    if (c1.isInstanceOf[EmptyParam] || c2.isInstanceOf[EmptyParam]) return true

    val a = c1.asInstanceOf[booleanCond]
    val b = c2.asInstanceOf[booleanCond]


    //a<9 et a>9 -> 8>9    a<9 et a>5 ->  8>5
    //Plus grande valeur de l'ensemble < vs la condition
    if (a.operator == '<' && b.operator == '>') {
      if (a.value - 1 > b.value) return true
      else return false
    }

    if (a.operator == '<' && b.operator == '<') {
      return true
    }

    //Même chose de l'autre coté
    if (a.operator == '<' && b.operator == '=')
      return b.value < a.value

    //a < 1 et a !=0
    if (a.operator == '<' && b.operator == '!') {
      if (b.value == 0 && a.value == 1) return false //seul cas sans intersection je pense
      else return true
    }

    //a>9 et a<9 -> 8>9 faux
    //Plus grande valeur de l'ensemble < vs la condition
    if (a.operator == '>' && b.operator == '<') {
      if (b.value - 1 > a.value) return true
      else return false
    }

    if (a.operator == '>' && b.operator == '>') return true //2

    //a > 32766 && b != 32766
    if (a.operator == '>' && b.operator == '!') {
      if (b.value == 32767 && a.value == 32766) return false
      else return true
    }

    //a > 5 et b = 3
    if (a.operator == '>' && b.operator == '=') {
      return b.value > a.value
    }

    //4x!
    if (a.operator == '!' && b.operator == '<') {
      if (a.value == 0 && b.value == 1) return false //seul cas sans intersection je pense
      else return true
    }

    //a > 32766 && b != 32766
    if (a.operator == '!' && b.operator == '>') {
      if (a.value == 32767 && b.value == 32766) return false
      else return true
    }

    //!5  et !3 et !3 et !3
    if (a.operator == '!' && b.operator == '!') return true //4


    // !3 et =2. Il faut que les opérandes soit pareil. Exemple a != 3 et a = 3 -> compatible
    if (a.operator == '!' && b.operator == '=') { //5
      if (a.value == b.value) return false
      else return true
    }

    //On fait les 4 =
    // = avec <
    if (a.operator == '=' && b.operator == '<') //7
      return a.value < b.value

    //a =5  b>3
    if (a.operator == '=' && b.operator == '>') {
      return a.value > b.value
    }

    if (a.operator == '=' && b.operator == '!') { //6
      if (a.value == b.value) return false
      else return true
    }

    if (a.operator == '=' && b.operator == '=') {
      if (a.value == b.value) return true
      else return false
    }

    println("Erreur fonction intersection")
    return false

  }

  //https://docs.scala-lang.org/overviews/scala-book/match-expressions.html
  def main(args: Array[String]): Unit = {

    val clauses = readPhiWayClauses("clauses1.txt")

    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("Phi-way graph coloring")
      .set("spark.driver.maxResultSize", "0")
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")

    val clauses2 = fastGenClauses(2, sc).collect()
    clauses2.foreach(println)
  }

}
