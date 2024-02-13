package org.apache.spark.util

import scala.collection.{Map, AbstractMap}
import scala.collection.immutable.ListMap

/**
 * LazyListMapWrapper can be used to break child dependencies in recursive algorithms
 * This is used in sdl-lang.
 *
 * As collection implementations are different for different Scala minor versions, this needs to be implemented per Scala minor version.
 * That's why it is placed in spark-extensions.
 * Note that its difficult to find an implementation that compiles for Scala 2.12 and 2.13.
 * It's possible with collection.AbstractMap, but not collection.immutable.AbstractMap...
 */
class LazyListMapWrapper[A,B](createFn: () => ListMap[A,B]) extends AbstractMap[A,B] with Serializable {
  private lazy val wrappedList: ListMap[A,B] = createFn()
  override def size: Int = wrappedList.size
  def get(key: A): Option[B] = wrappedList.get(key) // removed in 2.9: orElse Some(default(key))
  def iterator: Iterator[(A, B)] = wrappedList.iterator
  override def -(key: A): Map[A, B] = wrappedList - key
  override def -(key1: A, key2: A, keys: A*): Map[A, B] = wrappedList -(key1,key2,keys:_*)
  override def +[V1 >: B](kv: (A, V1)): Map[A, V1] = wrappedList + kv
}