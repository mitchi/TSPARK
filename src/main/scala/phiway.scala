package phiway

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


  /**
    * We take a reduced EOV clause, and produce a test with it
    */
  def EOVtoTest(a: clauseEOV): String = {
    var test = ""
    //On separe le test avec des ;
    //Go through all the reduced conditions
    for (i <- a.eovs) {

      i match {
        //Si on a un ensemble, on choisit une valeur dans l'ensemble.
        //Ici, je prends toujours la première valeur.
        case Ensemble(ensemble) => {
          val c = ensemble.first()
          test += s"$c;"
        }
        case Valeur(valeur) => test += s"$valeur;"
        case Rien() => test += ";"
      }
    }
    test
  }


  /**
    * Transform a clause to the new clause type
    *
    * @param a
    * @param v
    * @return
    */
  def transformClause(a: clause, v: Int): clauseEOV = {
    var accumulator = new ArrayBuffer[EnsembleOuValeur]()
    val result: Array[EnsembleOuValeur] = a.conds.map(cond => condToEOV(cond, v))
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
      val c = mergetwo(a(i), b(i)) //merge two conditions on the same parameter
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
  def mergetwo(a: EnsembleOuValeur, b: EnsembleOuValeur): EnsembleOuValeur = {

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
    *
    * @param a
    * @return
    */
  def condToEOV(aa: booleanCondition, v: Int): EnsembleOuValeur = {


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
      for (i <- 0 until v) {
        if (i != a.value) bitmap.add(i)
      }
    }

    else if (a.operator == '<') {
      for (i <- 0 until a.value)
        bitmap.add(i)
    }

    //Else '>'
    else {
      for (i <- a.value until v)
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

      //Ligne commentaire
      if (line(0) == '#') {
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
        if (line.contains("-")) {
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
    if (a.operator == '!' && b.operator == "=") { //5
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

    if (a.operator == '=' && b.operator == "!") { //6
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

  /**
    * Compare two boolean formulas and return whether or not they are adjacent in a graph
    * construction
    * If the two boolean formulas have domains that do not intersect, there is an edge in the graph
    * If the domains do intersect, no edge in the graph because it means that the tests are compatible
    *
    * @param a
    * @param b
    */
  def intersection2(a: booleanCond, b: booleanCond): Unit = {

    //Optimiser le nombre de comparaisons en regroupant les trucs.
    //Et en mettant la comparaison la plus fréquente en premier.

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
    if (a.operator == '!' && b.operator == "=") { //5
      if (a.value == b.value) return false
    }

    //On fait les 4 =
    // = avec <
    if (a.operator == '=' && b.operator == '<') //7
      return a.value < b.value

    //a =5  b>3
    if (a.operator == '=' && b.operator == '>') {
      return a.value > b.value
    }

    if (a.operator == '=' && b.operator == "!") { //6
      if (a.value == b.value) return false
    }

    //Premier cas: On a deux t-way combos essentiellement.
    //Il faut que la valeur soit différente
    if (a.operator == '=' && b.operator == '=') {
      if (a.value != b.value) return false
    }

  }

  //https://docs.scala-lang.org/overviews/scala-book/match-expressions.html
  def main(args: Array[String]): Unit = {
    val clauses = readPhiWayClauses("clauses.txt")
    clauses.foreach(println)
  }

} //fin object phiway


/*
On gère deux formes de formule Phi-way. On gère une forme classique, et une forme non-classique?
La forme classique : **01
La forme nouvelle : ,,=0,=1
D'autres versions de la forme nouvelle: ,,<5,<2
D'autres versions : ,,!3,=2
*/

/**
  * On transforme un combo en clause Phi-way
  * Le combo ressemble
  * [3;3;3;3]
  *
  *
  */
