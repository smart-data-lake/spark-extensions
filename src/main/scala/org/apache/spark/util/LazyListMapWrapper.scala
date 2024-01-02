package org.apache.spark.util

import java.io.Serializable
import scala.collection.immutable
import scala.collection.immutable.ListMap

/**
 * LazyListMapWrapper can be used to break child dependencies in recursive algorithms
 * This is used in sdl-lang.
 *
 * As collection implementations are different for different Scala minor versions, this needs to be implemented per Scala minor version.
 * That's why it is placed in spark-extensions.
 */
class LazyListMapWrapper[A,B](createFn: () => ListMap[A,B]) extends immutable.AbstractMap[A,B] with Serializable {
  private lazy val wrappedList: ListMap[A,B] = createFn()
  override def size: Int = wrappedList.size
  def get(key: A): Option[B] = wrappedList.get(key) // removed in 2.9: orElse Some(default(key))
  def iterator: Iterator[(A, B)] = wrappedList.iterator
  override def +[V1 >: B](e: (A,V1)): Map[A, V1] = wrappedList + e
  override def -(k: A): Map[A, B] = wrappedList - k
}