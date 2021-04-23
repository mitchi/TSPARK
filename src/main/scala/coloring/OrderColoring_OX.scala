package coloring

import cmdlineparser.TSPARK.compressRuns
import com.acme.BitSet
import enumerator.enumerator.{localGenCombos2, verify}
import ordercoloring.OrderColoring.mergeTests
import org.apache.spark.{SparkConf, SparkContext}
import org.roaringbitmap.RoaringBitmap
import utils.utils


/**
  * Dans cette classe, on implémente un Graph Coloring "sur place"
  * Si on a n sommets de graphe, on fait n itérations de coloration
  * Pour chaque itération de coloration, on utilise l'algorithme OX pour calculer le voisinage de chaque sommet.
  * On colorie ensuite comme d'habitude.
 *
 * Non terminé
  */

object OrderColoring_OX extends Serializable {

  var debug = false
  var filename = "results.txt"


  def generateOtherListDelete(list: RoaringBitmap,
                              etoiles: RoaringBitmap, nTests: Int) = {

    val possiblyValidGuys = list.clone()
    possiblyValidGuys.or(etoiles)
    possiblyValidGuys.flip(0.toLong
      , nTests)
    possiblyValidGuys
  }

  /**
    * On vérifie la suite de tests avec l'algorithme OX
    *
    * @param testSuite
    * @param combos
    * @return true if the test suite validates
    */
  def fastVerifyTestSuite(testSuite: Array[Array[Char]], n: Int, v: Int,
                          combos: Array[(Array[Char], Long)]): Boolean = {

    val nTests = testSuite.size
    val tableau = initTableau(n, v)
    val etoiles = initTableauEtoiles(n)
    //Le id du test, on peut le générer ici sans problème

    var i = -1
    val a = testSuite.map(test => {
      i += 1
      (test, i.toLong)
    })

    addTableauEtoiles(etoiles, a, n, v)
    addToTableau(tableau, a, n, v)

    //Pour tous les combos
    val r1 = combos.flatMap(combo => {
      var i = 0 //quel paramètre?
      var certifiedInvalidGuys = new RoaringBitmap()
      //On crée le set des validguys a partir de notre tableau rempli
      for (it <- combo._1) {
        if (it != '*') {
          val paramVal = it - '0'
          val list = tableau(i)(paramVal) //on prend tous les combos qui ont cette valeur. (Liste complète)
          val listEtoiles = etoiles(i) //on va prendre tous les combos qui ont des etoiles pour ce parametre (Liste complète)
          val invalids = generateOtherListDelete(list, listEtoiles, nTests)

          //On ajoute dans la grosse liste des invalides
          certifiedInvalidGuys or invalids

        }
        //On va chercher la liste des combos qui ont ce paramètre-valeur
        i += 1
      }

      //CertifiedInvalidGuys contient la liste de tous les tests qui ne fonctionnent pas.
      //On inverse la liste pour obtenir les tests qui fonctionnent
      //Si cette liste n'est pas vide, il existe un test qui détruit ce combo
      //Sinon, le combo reste

      certifiedInvalidGuys.flip(0.toLong
        , nTests)

      val it = certifiedInvalidGuys.getBatchIterator
      if (it.hasNext == true) //S'il y a des
        None
      else Some(combo)
    })

    //Si la collection au complète est détruire, c'est bon
    //Sinon, il reste du travail a faire
    if (r1.isEmpty == true)
      true
    else
      false

  }

  /**
    *
    */

  /**
    * From a collection of colored combos, we create the tests
    *
    * @param coloredCombos
    */
  def transformToTests(colors: Array[Int], someCombos: Array[(Array[Char], Long)]) = {
    //Create tests now
    val properForm: Array[(Int, Array[Char])] = someCombos.map(elem => {
      val id = elem._2
      val color = colors(id.toInt)
      (color, elem._1)
    })

    val hashtable = scala.collection.mutable.HashMap.empty[Int, Array[Char]]

    properForm.foreach(elem => {
      val color = elem._1
      if (hashtable.contains(color)) { //fusion
        hashtable(color) = mergeTests(hashtable(color), elem._2).get
      }
      else hashtable(color) = elem._2
    })
    hashtable.toArray.map(elem => elem._2)
  }


  /**
    * On utilise cette fonction pour calculer un seul vecteur d'adjacence.
    * Ce dernier est ensuite utilisé par ordercoloring
    *
    * @param combo
    * @param tableau
    * @param etoiles
    */
  def calculate_adjvector(combo : Array[Char], tableau: Array[Array[RoaringBitmap]], etoiles: Array[RoaringBitmap]) = {

    var i = 0 //quel paramètre?
    var certifiedInvalidGuys = new RoaringBitmap()
    //if (debug == true) println("Combo is " + utils.print_helper(combo) + s" id is $id")
    //On crée le set des validguys a partir de notre tableau rempli
    for (it <- combo) {
      if (it != '*') {
        val paramVal = it - '0'
        val list = tableau(i)(paramVal) //on prend tous les combos qui ont cette valeur. (Liste complète)
        val listEtoiles = etoiles(i) //on va prendre tous les combos qui ont des etoiles pour ce parametre (Liste complète)
        val invalids = generateOtherList(list, listEtoiles)
        //On ajoute dans la grosse liste des invalides
        certifiedInvalidGuys or invalids
      }
      //On va chercher la liste des combos qui ont ce paramètre-valeur
      i += 1
    }
    certifiedInvalidGuys
  }

  /**
    * On numérote les combos
    * @param combos
    */
  def numberCombos(combos : Array[Array[Char]]) = {
    var numero = -1
    combos.map( c => {
      numero += 1
      (c, numero)
    })
  }


  /**
    * Dans cette fonction, on génère la liste des couleurs possédées par les voisins de notre sommet
    * @param list
    * @param colors
    */
  def gen_neighbor_color_list(id: Int, list: RoaringBitmap, colors : Array[Int], currentMaxColor: Int): BitSet = {

    //1. Batch iteration sur la RoaringBitmap
    //Ceci nous fait utiliser le Garbage collector pas mal. Est-ce que ça vaut la peine d'avoir un tableau global de
    // taille fixe qu'on efface nous même?
    val neighborcolors = new BitSet(currentMaxColor + 2)
    neighborcolors.setUntil(currentMaxColor + 2) //On remplit le bitset avec des 1
    neighborcolors.unset(0) //peut etre pas nécessaire

    //Batch iteration through the neighbors of this guy
    val buffer = new Array[Int](256)
    val it = list.getBatchIterator

    while (it.hasNext) {
      val batch = it.nextBatch(buffer)
      earlyexit();
      def earlyexit(): Unit = {
        for (i <- 0 until batch) {
          val neighbor = buffer(i)
          if (neighbor >= id) return
          val neighborColor = colors(neighbor)
          neighborcolors.unset(neighborColor)
        }
      }
    }
    neighborcolors
  }


  /**
    * La fonction qui construit le graphe + colorie en même temps.
    * Single threaded
    *
    * La fonction est appelée une fois
    * Le premier sommet est déja colorié, il faut donc commencer au sommet #1.
    *
    * Plutôt propre la fonction maintenant!
    *
    * @param combos
    * @param colors
    * @param tableau
    * @param etoiles
    */
  def ordercoloring(combos : Array[Array[Char]], colors: Array[Int], tableau: Array[Array[RoaringBitmap]],
                    etoiles: Array[RoaringBitmap]): Unit = {

    val limit = combos.size //the number of iterations we do. -1 car on a déja colorié le premier
    var currentMaxColor = 1

    //Pour tous les combos, a partir du deuxième
    for (j <- 1 until combos.size) {
      val leCombo = combos(j) //on prend le combo
      val list =  calculate_adjvector(leCombo, tableau, etoiles)                       //On calcule sa liste d'adjacence
      val neighborcolors = gen_neighbor_color_list(j, list, colors, currentMaxColor) //couleurs des voisins
      val foundColor = colorBitSet(neighborcolors) //on trouve la meilleure couleur
      colors(j) = foundColor //on écrit la couleur
      if (foundColor > currentMaxColor) currentMaxColor = foundColor
    }

  }

  /**
    * Version du Graph Coloring qui fonctionne sans créer de Chunks. Approprié pour utiliser avec IPOG je suppose
    *
    * @param combos
    * @param n
    * @param v
    * @return
    */
  def graphcoloring(combos: Array[Array[Char]], v: Int) = {

    val count = combos.size
    val colors = new Array[Int](count)
    val n = combos(0).size

    //Before everything, we can color the first vertex with the color 1
    var maxColor = 1
    colors(0) = 1
    var i = 1 //the number of vertices we have colored

    //On numérote tous les combos pour le graph coloring
    var numero = -1
    var someCombos = combos.map(test => {
      numero += 1
      (test, numero.toLong)
    })

    var tableau = initTableau(n, v)
    var etoiles = initTableauEtoiles(n)

    //On ajoute l'info du premier combo
    val firstCombo = Array(someCombos(0))

    println("On ajoute l'info du premier combo dans le tableau, et tableau etoiles")
    tableau = addToTableau(tableau, firstCombo, n, v)
    etoiles = addTableauEtoiles(etoiles, firstCombo, n, v)

    val startTime = System.nanoTime()
    val afterGrow = System.nanoTime()
    tableau = addToTableau(tableau, someCombos, n, v)
    etoiles = addTableauEtoiles(etoiles, someCombos, n, v)
    val afterAddToTableaux = System.nanoTime()

    val growTablesTime = (afterGrow - startTime).toDouble / 1000000000
    val addTableauxTime = (afterAddToTableaux - afterGrow).toDouble / 1000000000
    println(s"Add tableaux time : $addTableauxTime seconds")

    val r1 = generateadjlist_fastcoloringFutures(i, count - 1, someCombos, tableau, etoiles)

    println("Using the Order Coloring algorithm to color the graph...")
    val t1 = System.nanoTime()
    val a: (Int, Int) = ordercoloringRoaring(colors, r1.toArray, i, maxColor)
    val t2 = System.nanoTime()
    val time_elapsed = (t2 - t1).toDouble / 1000000000
    println(s"Time elapsed for Order Coloring: $time_elapsed seconds")

    maxColor = a._2
    println(s"max color is now $maxColor")
    val tests = transformToTests(colors, someCombos)

    (tests, maxColor)
  }


  /**
    * Generate the list of impossible combos by "investing" the list of possible combos,
    * and also using the list of combos with stars
    *
    * On génère la liste des combos qui seront adjacents dans le graphe, en prenant la liste des étoiles, et la liste des
    * possiblement valides.
    *
    * @param id
    * @param list
    * @param etoiles
    * @return
    */
  def generateOtherList(list: RoaringBitmap,
                        etoiles: RoaringBitmap) = {

    val possiblyValidGuys = list.clone()
    possiblyValidGuys.or(etoiles)
    possiblyValidGuys.flip(0.toLong
      , possiblyValidGuys.last())
    possiblyValidGuys
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

    if (debug == true) println("Combo is " + utils.print_helper(combo) + s" id is $id")

    //On crée le set des validguys a partir de notre tableau rempli
    for (it <- combo) {

      if (it != '*') {
        val paramVal = it - '0'
        val list = tableau(i)(paramVal) //on prend tous les combos qui ont cette valeur. (Liste complète)
        val listEtoiles = etoiles(i) //on va prendre tous les combos qui ont des etoiles pour ce parametre (Liste complète)
        val invalids = generateOtherList(list, listEtoiles)
        //On ajoute dans la grosse liste des invalides
        certifiedInvalidGuys or invalids

      }
      //On va chercher la liste des combos qui ont ce paramètre-valeur
      i += 1
    }

    //On retourne cette liste, qui contient au maximum chunkSize éléments
    //Il faut ajuster la valeur des éléments de cette liste pour les id du chunk
    //println(s"F: $certifiedInvalidGuys ")
    certifiedInvalidGuys

  }

  /**
    * Input: a Bitset, output: A Roaring Bitmap
    *
    * @param bitset
    */
  def bitSetToRoaringBitmap(bitset: BitSet) = {
    val r = new RoaringBitmap()
    //r.addN()
    for (elem <- bitset.iterator) {
      r.add(elem)
    }
    r
  }

  /**
    * Input: RDD de combos, tableau, etoiles. Output: un RDD de (id, adjlist) pour colorier le graphew
    * Cette version n'utilise pas Apache Spark
    *
    * @param i
    * @param step
    * @param combos
    * @param tableau
    * @param etoiles
    * @param sc
    * @return
    */
  def generateadjlist_fastcoloringFutures(i: Long, step: Long,
                                          combos: Array[(Array[Char], Long)],
                                          tableau: Array[Array[RoaringBitmap]],
                                          etoiles: Array[RoaringBitmap]) = {


    println("Generating the adjacency lists using the fast graph construction algorithm...")
    println("Here, we are using Scala Futures")
    println("Run compression : " + compressRuns)

    val t1 = System.nanoTime()

    var totalTimeConvert = 0.0

    val r1 = combos.flatMap(combo => { //.par
      val id = combo._2
      if (id != 0 && id < i + step && id >= i) { //Discard first combo, it is already colored. We don't need to compute its adjlist
        //Pour chaque combo du RDD, on va aller chercher la liste de tous les combos dans le chunk qui sont OK
        val adj = comboToADJ(id, combo._1, tableau, etoiles)

        //On compresse la liste en runs
        adj.runOptimize()

        //La liste est de taille minimum, 64 bits
        Some(combo._2, adj)
      }
      else {
        //println("Not calculing adjlist for first combo")
        None
      }
    })

    val t2 = System.nanoTime()
    val time_elapsed = (t2 - t1).toDouble / 1000000000
    val timeToConvertPrint = totalTimeConvert.toDouble / 1000000000
    println(s"Time to create all adj lists: $time_elapsed seconds")
    println(s"Total time to convert: $timeToConvertPrint seconds")

    r1
  }


  /**
    * On crée le tableau qu'on va utiliser.
    * On skip les * lorsqu'on remplit ce tableau avec les valeurs
    * Ce tableau se fait remplir avec chaque traitement de chunk.
    *
    * */
  def initTableau(n: Int, v: Int) = {
    var tableau = new Array[Array[RoaringBitmap]](n)

    //On met très exactement n paramètres, chacun avec v valeurs. On ne gère pas les *
    for (i <- 0 until n) {
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
  def initTableauEtoiles(n: Int) = {
    var tableauEtoiles = new Array[RoaringBitmap](n)

    for (i <- 0 until n) {
      tableauEtoiles(i) = new RoaringBitmap()
    }

    tableauEtoiles
  }


  /**
    * On recoit un tableau, et on ajoute l'information avec le chunk de combos
    * On ajoute sans cesse dans le tableau
    *
    * @param chunk
    * @param n
    * @param v
    */
  def addToTableau(tableau: Array[Array[RoaringBitmap]],
                   chunk: Array[(Array[Char], Long)], n: Int, v: Int) = {

    //On remplit cette structure avec notre chunk
    for (combo <- chunk) { //pour chaque combo
      for (i <- 0 until n) { //pour chaque paramètre
        val cc = combo._1(i)
        if (cc != '*') {
          val vv = combo._1(i) - '0' //on va chercher la valeur
          tableau(i)(vv) add combo._2.toInt //on ajoute dans le ArrayBuffer . On pourrait mettre l'index global aussi.mmm
        }
      }
    }

    //On retourne notre travail
    tableau
  }

  def addTableauEtoiles(etoiles: Array[RoaringBitmap],
                        chunk: Array[(Array[Char], Long)], n: Int, v: Int) = {

    //On remplit cette structure avec notre chunk
    for (combo <- chunk) { //pour chaque combo
      for (i <- 0 until n) { //pour chaque paramètre
        val cc = combo._1(i)
        if (cc == '*') {
          etoiles(i) add (combo._2.toInt) //on ajoute dans le ArrayBuffer . On pourrait mettre l'index global aussi.mmm
        }
      }
    }
    etoiles
  }

  /**
    * Color N vertices at a time using the Order Coloring algorithm.
    * The adjvectors are precalculated
    *
    * @return
    */
  def ordercoloringRoaring(colors: Array[Int], adjMatrix: Array[(Long, RoaringBitmap)], i: Int,
                           maxColor: Int) = {

    val limit = adjMatrix.size //the number of iterations we do
    var j = 0 //start coloring using the first adjvector of the adjmatrix
    var currentMaxColor = maxColor

    loop

    def loop(): Unit = {

      //Only color n vertices in this loop
      if (j == limit) return

      //Build the neighborcolors data structure for this vertex (j)
      //We iterate through the adjvector to find every colored vertex he's connected to and take their color.
      val neighborcolors = new BitSet(currentMaxColor + 2)
      neighborcolors.setUntil(currentMaxColor + 2) //On remplit le bitset avec des 1
      neighborcolors.unset(0) //peut etre pas nécessaire

      //Batch iteration through the neighbors of this guy
      val buffer = new Array[Int](256)
      val it = adjMatrix(j)._2.getBatchIterator
      val id = adjMatrix(j)._1

      while (it.hasNext) {
        val batch = it.nextBatch(buffer)
        earlyexit();
        def earlyexit(): Unit = {
          for (i <- 0 until batch) {
            val neighbor = buffer(i)
            if (neighbor >= id) return
            val neighborColor = colors(neighbor)
            neighborcolors.unset(neighborColor)
          }
        }

      }

      val foundColor = colorBitSet(neighborcolors)
      colors(i + j) = foundColor

      if (foundColor > currentMaxColor) currentMaxColor = foundColor
      j += 1 //we color the next vertex
      loop
    }

    //Return the number of iterations we did, and the maxColor
    (limit, currentMaxColor)
  }


  /**
    * MUTABLE.
    * This function works with a lookup table of colors ( color not here = 0, here = 1)
    * Color using this function and good parameters
    * Use this function the vertex is adjacent to the other, and we want to know what the best color is
    *
    * @param vertices
    * @param vertex
    * @return THE COLOR THAT WE CAN TAKE
    */
  def color(neighbor_colors: Array[Int]): Int = {
    for (i <- 1 to neighbor_colors.length) {
      if (neighbor_colors(i) != 1) //if this color is not present
        return i //we choose it
    }
    return 1 //the case when the color lookuo table is empty
  }

  /**
    * On itere sur le bitset des couleurs adjacentes. Dès qu'un bit du bitset est égal a zéro, on peut prendre cette couleur.
    *
    * @param vertices
    * @param vertex
    * @return THE COLOR THAT WE CAN TAKE
    */
  def colorBitSet(neighbor_colors: BitSet) = {
    val res = neighbor_colors.nextSetBit(1)
    res
  }

  /**
    * The distributed graph coloring algorithm with the fast coloring construction algorithm
    *
    * @param n
    * @param t
    * @param v
    * @param sc
    * @return
    */
  def start(n: Int, t: Int, v: Int, seed: Long) = {
    val expected = utils.numberTWAYCombos(n, t, v)

    println("Single threaded Local Graph Coloring using Order Coloring + OX Algorithm")
    println("Using BitSets...")
    println(s"Problem : n=$n,t=$t,v=$v")
    println(s"Expected number of combinations is : $expected ")
    println(s"Formula is C($n,$t) * $v^$t")

    import java.io._
    val pw = new PrintWriter(new FileOutputStream(filename, true))
    val t1 = System.nanoTime()

    //Roaring Bitmap
    val result = graphcoloring(localGenCombos2(n, t, v, seed), v)

    val t2 = System.nanoTime()
    val time_elapsed = (t2 - t1).toDouble / 1000000000
    val maxColor = result._2

    pw.append(s"$t;$n;$v;LOCAL_COLORING;$time_elapsed;$maxColor\n")
    println(s"$t;$n;$v;LOCAL_COLORING;$time_elapsed;$maxColor\n")
    pw.flush()

    //Return the test suite
    result._1
  }
}


object test_OrderColoring_OX extends App {
  var debug = false

  var n = 3
  var t = 2
  var v = 2

  val seed = if (debug == true) 20
  else System.nanoTime()
  import OrderColoring_OX.start

  val tests = start(n,t,v, seed)

 // println("We have colored the graph using " + maxColor + " colors")
  println("Printing the tests....")
  tests foreach (utils.print_helper(_))

  println("\n\nVerifying test suite ... ")
  val answer = verify(tests, n, v, localGenCombos2(n,t,v, seed))
  if (answer == true) println("Test suite is verified")
  else println("Test suite is not verified")

}