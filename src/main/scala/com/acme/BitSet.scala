package com.acme

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Arrays

/**
  * A simple, fixed-size bit set implementation. This implementation is fast because it avoids
  * safety/bound checking.
  *
  * Notes Edmond:
  * Taille minimum = 64 bits (bien entendu!)
  *
  */
class BitSet(numBits: Int) extends Serializable {

   val words = new Array[Long](bit2words(numBits))
   val numWords = words.length


//  /**
//    * Printing the bitset in compact form
//    * Marche parfaitement maintenant
//    * @return
//    */
//  override def toString: String =
//  {
//    var output = "    "
//    var j = 0
//
//    val it = this.iterator
//
//    for (i <- it) {
//      while (j != i) {
//        output += "0 "
//        j+=1
//      }
//      if (j == i) {
//        output+="1 "
//        j+=1
//      }
//
//    }
//
//    output += s" capacity:$capacity"
//    output
//  }


  /**
    * Printing the bitset in compact form
    * @return
    */
   override def toString: String =
  {
      var output = ""
      val it = this.iterator
      for (i <- it) {
        output += i +" "
      }
      output
  }

  /**
    * Printing the bitset in compact form
    * @return
    */
  def toString2: String =
  {
      var output = "       "
      var i = this.nextSetBit(0)
      loop; def loop(): Unit = {
          if (i == -1) return

        output+= i+" "
        i = this.nextSetBit(i+1)
      loop
     }

    output
  }


//  /**
//    * Printing the array in "bits" form 0 1 0 1 0 1 etc...
//    * @return
//    */
//  override def toString: String =
//  {
//    var output = "       "
//    var j = 0
//    var i = this.nextSetBit(0)
//
//    def printzeroes(): Unit = {
//      while(j != i) {
//        output += "0 "
//        j +=1
//      }
//    }
//
//    loop; def loop(): Unit = {
//    if (i == -1) return
//    printzeroes()
//    output+= "1 "
//    i = this.nextSetBit(i+1)
//    loop
//  }
//
//    output += s" capacity:$capacity"
//    output
//  }

  /**
    * Compute the capacity (number of bits) that can be represented
    * by this bitset.
    */
  def capacity: Int = numWords * 64

  /**
    * Clear all set bits.
    */
  def clear(): Unit = Arrays.fill(words, 0)

  /** Apply XOR 255 to everything */
  def xor1(): Unit = {
    for (i <-0 until words.length) {
      words(i) = words(i) ^ -1
    }
  }

  /** Same behavior as the xor1 function. Maybe its faster? */
  def notEverything(): Unit = {
    for (i <- 0 until words.length) {
      words(i) = ~words(i)
    }
  }

  /**
    * Compute OR in place
    * Both bitsets have the same size, always
    * Fonction de Edmond
    * @param other
    */
  def or(other: BitSet): Unit = {
    for (i <- 0 until other.words.length) {
      words(i) = words(i) | other.words(i)
    }
  }


  /**
    * Set all the bits up to a given index
    */
  def setUntil(bitIndex: Int): Unit = {
    val wordIndex = bitIndex >> 6 // divide by 64
    Arrays.fill(words, 0, wordIndex, -1)
    if(wordIndex < words.length) {
      // Set the remaining bits (note that the mask could still be zero)
      val mask = ~(-1L << (bitIndex & 0x3f))
      words(wordIndex) |= mask
    }
  }

  /**
    * Clear all the bits up to a given index
    */
  def clearUntil(bitIndex: Int): Unit = {
    val wordIndex = bitIndex >> 6 // divide by 64
    Arrays.fill(words, 0, wordIndex, 0)
    if(wordIndex < words.length) {
      // Clear the remaining bits
      val mask = -1L << (bitIndex & 0x3f)
      words(wordIndex) &= mask
    }
  }

  /**
    * Compute the bit-wise AND of the two sets returning the
    * result.
    */
  def &(other: BitSet): BitSet = {
    val newBS = new BitSet(math.max(capacity, other.capacity))
    val smaller = math.min(numWords, other.numWords)
    assert(newBS.numWords >= numWords)
    assert(newBS.numWords >= other.numWords)
    var ind = 0
    while( ind < smaller ) {
      newBS.words(ind) = words(ind) & other.words(ind)
      ind += 1
    }
    newBS
  }

  /**
    * Compute the bit-wise OR of the two sets returning the
    * result.
    */
  def |(other: BitSet): BitSet = {
    val newBS = new BitSet(math.max(capacity, other.capacity))
    assert(newBS.numWords >= numWords)
    assert(newBS.numWords >= other.numWords)
    val smaller = math.min(numWords, other.numWords)
    var ind = 0
    while( ind < smaller ) {
      newBS.words(ind) = words(ind) | other.words(ind)
      ind += 1
    }
    while( ind < numWords ) {
      newBS.words(ind) = words(ind)
      ind += 1
    }
    while( ind < other.numWords ) {
      newBS.words(ind) = other.words(ind)
      ind += 1
    }
    newBS
  }



  /**
    * Compute the symmetric difference by performing bit-wise XOR of the two sets returning the
    * result.
    */
  def ^(other: BitSet): BitSet = {
    val newBS = new BitSet(math.max(capacity, other.capacity))
    val smaller = math.min(numWords, other.numWords)
    var ind = 0
    while (ind < smaller) {
      newBS.words(ind) = words(ind) ^ other.words(ind)
      ind += 1
    }
    if (ind < numWords) {
      Array.copy( words, ind, newBS.words, ind, numWords - ind )
    }
    if (ind < other.numWords) {
      Array.copy( other.words, ind, newBS.words, ind, other.numWords - ind )
    }
    newBS
  }

  /**
    * Compute the difference of the two sets by performing bit-wise AND-NOT returning the
    * result.
    */
  def andNot(other: BitSet): BitSet = {
    val newBS = new BitSet(capacity)
    val smaller = math.min(numWords, other.numWords)
    var ind = 0
    while (ind < smaller) {
      newBS.words(ind) = words(ind) & ~other.words(ind)
      ind += 1
    }
    if (ind < numWords) {
      Array.copy( words, ind, newBS.words, ind, numWords - ind )
    }
    newBS
  }

  /**
    * Sets the bit at the specified index to true.
    * @param index the bit index
    */
  def set(index: Int): Unit = {
    val bitmask = 1L << (index & 0x3f)  // mod 64 and shift
    words(index >> 6) |= bitmask        // div by 64 and mask
  }

  def unset(index: Int): Unit = {
    val bitmask = 1L << (index & 0x3f)  // mod 64 and shift
    words(index >> 6) &= ~bitmask        // div by 64 and mask
  }

  /**
    * Return the value of the bit with the specified index. The value is true if the bit with
    * the index is currently set in this BitSet; otherwise, the result is false.
    *
    * @param index the bit index
    * @return the value of the bit with the specified index
    */
  def get(index: Int): Boolean = {
    val bitmask = 1L << (index & 0x3f)   // mod 64 and shift
    (words(index >> 6) & bitmask) != 0  // div by 64 and mask
  }


  /**
    * Get an iterator over the set bits.
    * This is an iterator for parallel work
    * Il faut que les partitions commencent a zéro
    */
  def iterator2(p: Int, numberPartitions : Int): Iterator[Int] = new Iterator[Int] {
    var partSize = numBits / numberPartitions
    var start = if (p == 0) 0
                else partSize * p
    var ind = nextSetBit(start)
    var bound = start + partSize

//    println("Numbits is " + numBits)
//    println("Partition Size is " + partSize)
//    println("Partition is " + p)
//    println("Start is " + start)
//    println("End is " + bound)

    override def hasNext: Boolean = ind >= 0 && ind <= bound
    override def next(): Int = {
      val tmp = ind
      ind = nextSetBit(ind + 1)
      tmp
    }
  }

  /**
    * Get an iterator over the set bits.
    */
  def iterator: Iterator[Int] = new Iterator[Int] {
    var ind = nextSetBit(0)
    override def hasNext: Boolean = ind >= 0
    override def next(): Int = {
      val tmp = ind
      ind = nextSetBit(ind + 1)
      tmp
    }
  }

  /** Return the number of bits set to true in this BitSet. */
  def cardinality(): Int = {
    var sum = 0
    var i = 0
    while (i < numWords) {
      sum += java.lang.Long.bitCount(words(i))
      i += 1
    }
    sum
  }

  /**
    * Returns the index of the first bit that is set to true that occurs on or after the
    * specified starting index. If no such bit exists then -1 is returned.
    *
    * To iterate over the true bits in a BitSet, use the following loop:
    *
    *  for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
    *    // operate on index i here
    *  }
    *
    * @param fromIndex the index to start checking from (inclusive)
    * @return the index of the next set bit, or -1 if there is no such bit
    */
  def nextSetBit(fromIndex: Int): Int = {
    var wordIndex = fromIndex >> 6
    if (wordIndex >= numWords) {
      return -1
    }

    // Try to find the next set bit in the current word
    val subIndex = fromIndex & 0x3f
    var word = words(wordIndex) >> subIndex
    if (word != 0) {
      return (wordIndex << 6) + subIndex + java.lang.Long.numberOfTrailingZeros(word)
    }

    // Find the next set bit in the rest of the words
    wordIndex += 1
    while (wordIndex < numWords) {
      word = words(wordIndex)
      if (word != 0) {
        return (wordIndex << 6) + java.lang.Long.numberOfTrailingZeros(word)
      }
      wordIndex += 1
    }

    -1
  }

  /**
    * Ma petite implémentation de la méthode clone
    * @return
    */
  override def clone(): BitSet = {
    val bb = new BitSet(numBits)
    Array.copy(this.words, 0, bb.words, 0, bb.words.length)
    bb
  }

  /** Return the number of longs it would take to hold numBits. */
  private def bit2words(numBits: Int) = ((numBits - 1) >> 6) + 1
}

/**
  * Companion object
  */
object BitSet {

  def apply(numBits: Int) = {
    new BitSet(numBits)
  }

  //Expand the current Bitset
  def expandBitSet(a: BitSet, newBits : Int) = {
    val bb = new BitSet(newBits+64)
    Array.copy(a.words, 0, bb.words, 0, a.words.length)
    bb
  }

}
