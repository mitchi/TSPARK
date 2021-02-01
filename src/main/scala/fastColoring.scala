package fastcoloring

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.roaringbitmap.{RoaringArray, RoaringBitmap}
import utils.utils
import progressivecoloring.progressive_coloring.assign_numberClauses
import progressivecoloring.progressive_coloring.determineStep

import scala.util.Random
import ordercoloring.OrderColoring.isAdjacent
import ordercoloring.OrderColoring.coloringToTests
import roaringcoloring.roaring_coloring.{filterBig, generatelist_fastcoloring, ordercoloring}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Set

object fastColoring extends Serializable {

  var debug = true


  /**
    *
    */
  def fastcoloring(combos: RDD[Array[Char]],
                           sc: SparkContext,
                           chunkSize: Int,
                           n: Int, v : Int,
                           algorithm: String = "OC") = {

    val count = combos.count()
    //Toutes nos couleurs sont la
    val colors = new Array[Int](count.toInt)
    val seed = if (debug == true) 20 else System.nanoTime()
    var totalIterations = 0

    //Shuffle the combos before. Doing this ensures a different result every run.
    var mycombos = combos.mapPartitions(it => {
      Random.setSeed(seed)
      Random.shuffle(it)
    }, true)

    //Assign numbers to clauses using a custom algorithm
    val combosNumbered: RDD[(Array[Char], Long)] = assign_numberClauses(mycombos, sc).cache()
    println("Shuffling and numbering the clauses is complete")

    //We no longer need the not-numbered combos
    combos.unpersist(false)

    //Before everything, we can color the first vertex with the color 1
    var maxColor = 1
    var i = 1 //the number of vertices we have colored
    var chunkNo = 0

    var tableau = initTableau(n,v)
    var etoiles = initTableauEtoiles(n)

    //On ajoute l'info du premier combo
    val firstCombo: Array[(Array[Char], Long)] = combosNumbered.flatMap(e => {
      if (e._2 == 0) Some(e)
      else None
    }).collect()

    println("On ajoute l'info du premier combo dans le tableau, et tableau etoiles")
    tableau = addToTableau(tableau, firstCombo, n, v)
    etoiles = addTableauEtoiles(etoiles, firstCombo, n, v)

    loop

    def loop(): Unit = {

      if (i >= count) return //if we have colored everything, we are good to go

      chunkNo += 1
      println(s"Now processing Chunk $chunkNo of the graph...")
      println("Calling Garbage Collection...")
      System.gc() //calling garbage collection here

      //Filter the combos we color in the next OrderColoring iteration
      println("We are now  creating the chunk we need...")
      val someCombos: Array[(Array[Char], Long)] = combosNumbered.flatMap(elem => {
        if (elem._2 < i + chunkSize && elem._2 >= i) { //if chunksize = 10k, then  i < 10001 and 1 >=1. 2ème itération  < 20001 et >=10001
          Some(elem)
        }
        else None
      }).collect().sortBy(_._2)

      val sizeOfChunk = someCombos.size
      println(s"Currently working with a chunk of the graph with $sizeOfChunk vertices.")

      //val filteredCombos = filterBig(combosNumbered, i, sizeOfChunk)

      println("Adding the entries of the chunk to the tableau...")
      tableau = addToTableau(tableau, someCombos, n, v)
      etoiles = addTableauEtoiles(etoiles, someCombos, n, v)


      if (debug == true) {
        println("On imprime le tableau apres remplissage")
        for (i <- 0 until n){ //pour tous les paramètres
          for (j <- 0 until v) { //pour toutes les valeurs
            println(s"p$i=$j "+ tableau(i)(j).toString)
          }
        }

        println("On imprime le tableau etoiles")
        var tt = 0
        for (elem <- etoiles) {
          println(s"p$tt=*" + " " +elem)
          tt+=1
        }
      }

      val r1 = generateadjlist_fastcoloring(i, sizeOfChunk, combosNumbered, tableau, etoiles, sc).cache()
      r1.collect()

      //On debug ce qu'on a
      println("Debug de l'adj list")
      r1.collect().foreach(  e => println(s"${e._1} ${e._2.toString}"))

      //Use KP when the graph is sparse (not dense)
      val r2 =
        if (algorithm == "KP") {
          roaringkp.roaringkp.progressiveKP(colors, r1, r1.count().toInt, maxColor, sc)
        }
        else { //algorithm = "OC". The single threaded needs a sorted matrix of adjlists
          ordercoloring(colors, r1.collect().sortBy(_._1), i, maxColor)
        }

      //Update the max color
      totalIterations += r2._1
      maxColor = r2._2
      r1.unpersist(true)
      i += chunkSize //1 + 10000 = 10001

      loop
    }

    val percent = ((totalIterations.toDouble / count.toDouble) * 100)
    val vertexPerIteration = count / totalIterations
    println(s"We did a total of $totalIterations iterations, which is $percent% of total")
    println(s"We also colored $vertexPerIteration vertices per iteration on average")

    //Create tests now
    val bcastcolors = sc.broadcast(colors)
    val properFormRDD: RDD[(Int, Array[Char])] = combosNumbered.map(elem => {
      val id = elem._2
      val color = bcastcolors.value(id.toInt)
      (color, elem._1)
    })

    //Transform into tests
    coloringToTests(properFormRDD)
  }


  /**
    * Generate the list of impossible combos by "investing" the list of possible combos, and also using the list of combos with stars
    * @param id
    * @param list
    * @param etoiles
    * @return
    */
  def generateOtherList( id: Long,
                         list : RoaringBitmap,
                         etoiles: RoaringBitmap) = {
    //On construit une lookup table avec la liste des bons combos
    //La liste est exactement de la taille du id
    var lookuptable = new Array[Byte](id.toInt)
    lookuptable = lookuptable.map( e => 0.toByte)

    //We take the data from the tableau, and we add it to the possiblyValidGuys, if the comboId is smaller than our id of course!
    val buffer = new Array[Int](256)
    val ite = list.getBatchIterator
    while (ite.hasNext) {
      val batch = ite.nextBatch(buffer)
      for (i <- 0 until batch) {
        if (buffer(i) < id) lookuptable( buffer(i) ) = 1
      }
    }

  //On ajoute les etoiles également
  val buffer2 = new Array[Int](256)
    val ite2 = etoiles.getBatchIterator
    while (ite2.hasNext) {
      val batch = ite2.nextBatch(buffer2)
      for (i <- 0 until batch) {
        if (buffer2(i) < id) lookuptable( buffer2(i) ) = 1
      }
    }

    //On inverse la lsite maintenant
    var adjlist = new RoaringBitmap()
    var index = 0
    for (i <- 0 until id.toInt) {
      if (lookuptable(i) == 0) {
        adjlist.add(i)
      }
    }

    println("List of combos with valid params " + list.toString)
    println("List of combos with stars " + etoiles.toString)
    println("the certified invalid combos "+ adjlist.toString)

    adjlist
  }



  /**
    *
    * @param id
    * @param combo
    * @param tableau
    * @param etoiles
    * @return
    */
  def comboToADJ(id: Long,
                 combo: Array[Char],
                 tableau: Array[Array[RoaringBitmap]],
                 etoiles: Array[RoaringBitmap]) = {

    var i = 0 //quel paramètre?
    var certifiedInvalidGuys = new RoaringBitmap()

    println("Combo is " + utils.print_helper(combo) + s" id is $id")

    //On crée le set des validguys a partir de notre tableau rempli
    for (it <- combo) {

      println(s"i=$i, value is $it")
      if (it == '*') println("*, we skip")

      if (it != '*') {
        val paramVal = it - '0'
        val list = tableau(i)(paramVal) //on prend tous les combos qui ont cette valeur. (Liste complète)
        val listEtoiles = etoiles(i) //on va prendre tous les combos qui ont des etoiles pour ce parametre (Liste complète)
        val invalids = generateOtherList(id, list, listEtoiles)

        //On ajoute dans la grosse liste des invalides
        certifiedInvalidGuys.or(invalids)

      }
      //On va chercher la liste des combos qui ont ce paramètre-valeur
      i+=1
    }

    println(s"list of all certified invalid guys" + certifiedInvalidGuys.toString)
    println("\n\n")

    //On retourne cette liste, qui contient au maximum chunkSize éléments
    //Il faut ajuster la valeur des éléments de cette liste pour les id du chunk
    certifiedInvalidGuys

  }


  /**
    * Input: RDD de combos, tableau, etoiles. Output: un RDD de (id, adjlist) pour colorier le graphew
    * @param i
    * @param step
    * @param combos
    * @param tableau
    * @param etoiles
    * @param sc
    * @return
    */
  def generateadjlist_fastcoloring(i : Long, step: Long,
                                combos: RDD[(Array[Char], Long)],
                                tableau: Array[Array[RoaringBitmap]],
                                etoiles: Array[RoaringBitmap],
                                sc : SparkContext) =
  {

    //Print the number of partitions we are using
    val partitions = combos.getNumPartitions
    println("Generating the adjacency lists using the fast graph construction algorithm...")
    println(s"Currently using $partitions partitions")

    val r1 = combos.flatMap( combo => {
      //le id du combat. On n'a pas besoin de mettre des sommets plus gros que lui dans sa liste d'adjacence
      val id = combo._2
      if (id != 0 && id < i+step) { //Discard first combo, it is already colored. We don't need to compute its adjlist
        //Pour chaque combo du RDD, on va aller chercher la liste de tous les combos dans le chunk qui sont OK
        val adjlist = comboToADJ(id, combo._1, tableau, etoiles)
        Some(combo._2, adjlist)
      }
      else {
        //println("Not calculing adjlist for first combo")
        None
      }
    })

    r1
  }


  /**
    * On filtre les combos du RDD, comme ça on travaille pas avec les combos qui sont pas dans le chunk
    *
    * @param combos
    * @param i
    * @param step
    * @return
    */
  def filterBig(combos: RDD[(Array[Char], Long)], i: Int, step: Int):
  RDD[(Array[Char], Long)] = {
    combos.flatMap(e => {
      if (e._2 > i + step) None
      else Some(e)
    })
  }


  /**
    * On crée le tableau qu'on va utiliser.
    * On skip les * lorsqu'on remplit ce tableau avec les valeurs
    * Ce tableau se fait remplir avec chaque traitement de chunk.
    *
    *  */
  def initTableau(n: Int, v : Int) =
  {
    var tableau = new Array[Array[RoaringBitmap]](n)

    //On met très exactement n paramètres, chacun avec v valeurs. On ne gère pas les *
    for (i <- 0 until n ) {
      tableau(i) = new Array[RoaringBitmap](v)
      for (v <- 0 until v) {
        tableau(i)(v) = new RoaringBitmap()
      }
    }
    tableau
  }

  /**
    * On crée un tableau pour gérer seulement les étoiles
    * Roaring Bitmap dans le tableau
    */
  def initTableauEtoiles(n: Int) =
  {
    var tableauEtoiles = new Array[RoaringBitmap](n)

    for (i <- 0 until n) {
      tableauEtoiles(i) = new RoaringBitmap()
    }

    tableauEtoiles
  }


  def addTableauEtoiles(etoiles: Array[RoaringBitmap],
                        chunk: Array[(Array[Char], Long)], n : Int, v : Int ) = {

    //On remplit cette structure avec notre chunk
    for (combo <- chunk) { //pour chaque combo
      for (i <- 0 until n) { //pour chaque paramètre
        val cc = combo._1(i)
        if (cc == '*') {
          etoiles(i).add(combo._2.toInt)  //on ajoute dans le ArrayBuffer . On pourrait mettre l'index global aussi.mmm
        }
      }
    }
    etoiles
  }


  /**
  On recoit un tableau, et on ajoute l'information avec le chunk de combos
     On ajoute sans cesse dans le tableau
    * @param chunk
    * @param n
    * @param v
    */
  def addToTableau(tableau: Array[Array[RoaringBitmap]],
                   chunk: Array[(Array[Char], Long)], n : Int, v : Int) = {
    //On pourrait également faire Array[Array[Int]] et faire un indexage plus compliqué

    println("On print les combos du chunk")
    for (c <- chunk) {
      print(c._2 + " ") ; utils.print_helper(c._1)
    }

    var indexCombo = 0
    //On remplit cette structure avec notre chunk
    for (combo <- chunk) { //pour chaque combo
      for (i <- 0 until n) { //pour chaque paramètre
        val cc = combo._1(i)
        if (cc != '*') {
          val vv = combo._1(i) - '0' //on va chercher la valeur
          tableau(i)(vv) add combo._2.toInt //on ajoute dans le ArrayBuffer . On pourrait mettre l'index global aussi.mmm
          //tableau(i)(vv) += indexCombo
        }
      }
      indexCombo +=1
    }

    //On retourne notre travail
    tableau
  }



}
