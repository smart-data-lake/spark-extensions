package org.apache.spark.util

import org.json4s.jackson.Serialization
import org.json4s.{CustomKeySerializer, CustomSerializer, Formats, JValue, ShortTypeHints}

import scala.reflect.ClassTag

/**
 * This class implements functions to use different Spark/Json4s versions with the same interface
 * Spark 3.1 uses Json4s 3.7-M5
 * Spark 3.2 uses Json4s 3.7-M11
 */
object Json4sCompat {

  /**
   * Json4s up to 3.7-M5 needs Manifest, later versions need ClassTag
   */
  def getCustomSerializer[A: ClassTag: Manifest](ser: Formats => (PartialFunction[JValue, A], PartialFunction[Any, JValue])): CustomSerializer[A] = {
    new CustomSerializer[A](ser)
  }

  /**
   * Json4s up to 3.7-M5 needs Manifest, later versions need ClassTag
   */
  def getCustomKeySerializer[A: ClassTag: Manifest](ser: Formats => (PartialFunction[String, A], PartialFunction[Any, String])): CustomKeySerializer[A] = {
    new CustomKeySerializer[A](ser)
  }

  /**
   * Json4s formats.withStrictMapExtraction does not yet exists in 3.7-M5
   */
  def getStrictSerializationFormat(typeHints: ShortTypeHints): Formats = {
    Serialization.formats(typeHints).withStrictArrayExtraction.withStrictMapExtraction.withStrictOptionParsing
  }
}
