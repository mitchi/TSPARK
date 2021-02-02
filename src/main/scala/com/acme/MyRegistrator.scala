package com.acme

import com.esotericsoftware.kryo.Kryo
import fastColoringKryo.RoaringSerializer
import org.apache.spark.serializer.KryoRegistrator
import org.roaringbitmap.RoaringBitmap

class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    //System.err.println("REGISTRATOR REGISTRATORREGISTRATORREGISTRATORREGISTRATORREGISTRATORREGISTRATOR")
    kryo register(classOf[RoaringBitmap], new RoaringSerializer())
  }
}