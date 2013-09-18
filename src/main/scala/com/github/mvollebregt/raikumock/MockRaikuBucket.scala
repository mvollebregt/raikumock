package com.github.mvollebregt.raikumock

import akka.actor.ActorSystem
import scala.collection.immutable
import scala.collection.mutable._
import scalaz._
import Scalaz._

import nl.gideondk.raiku._
import actors.{ RaikuConfig, RaikuHost }
import commands._
import monads.ValidatedFutureIO

object MockRaikuClient extends RaikuClient(RaikuConfig(RaikuHost("", 0), 0, 0))(ActorSystem("test"))

object MockRaikuBucket {
  def apply[T](implicit converter: RaikuConverter[T]) = new MockRaikuBucket[T]
}

/** In-memory implementation of RaikuBucket - used for fast unit testing.
 */
class MockRaikuBucket[T](implicit converter: RaikuConverter[T]) extends RaikuBucket[T]("mock", MockRaikuClient) {

  private val map = Map[String, T]()
  private val mapBinIndex = Map[String, Map[String, ArrayBuffer[String]]]()
  private val mapIntIndex = Map[String, Map[Int, ArrayBuffer[String]]]()

  /** Clears all values from the bucket
   */
  def clear() {
    map.clear()
    mapBinIndex.clear()
    mapIntIndex.clear()
  }

  /** Adds one or more objects to the bucket. Overrides existing values if their keys correspond.
   */
  def add(objects: T*) {
    objects.foreach { obj ⇒
      {
        val converted = converter.write(bucketName, obj)
        val previous = map.put(converted.key, obj)
        if (!previous.isEmpty) {
          val previousConverted = converter.write(bucketName, previous.get)
          deleteFromIndexes(mapBinIndex, previousConverted.binIndexes, previousConverted.key)
          deleteFromIndexes(mapIntIndex, previousConverted.intIndexes, previousConverted.key)

        }
        converted.binIndexes.foreach {
          case (idxk, idxvs) ⇒
            idxvs.foreach(idxv ⇒ put(mapBinIndex, idxk, idxv, converted.key))
        }
        converted.intIndexes.foreach {
          case (idxk, idxvs) ⇒ {
            idxvs.foreach(idxv ⇒ put(mapIntIndex, idxk, idxv, converted.key))
          }
        }
      }
    }
  }

  /** Clears all values from the bucket and adds one or more objects to the empty bucket
   */
  def init(objects: T*) {
    clear()
    add(objects: _*)
  }

  /** Checks that the bucket contains the exact objects.
   *  Returns true if the objects under the object keys are exactly the same.
   *  Returns false if any one of the object keys do not exist, or refer to another value than the object given.
   */
  def contains(objects: T*): Boolean = {
    objects.forall { obj ⇒
      map.get(key(obj)) == Some(obj)
    }
  }

  /** Checks that the bucket contains the given keys.
   */
  def containsKey(keys: String*): Boolean = {
    keys.forall { key ⇒
      map.contains(key)
    }
  }

  /** Returns all values contained in the bucket
   */
  def allValues: collection.Iterable[T] = map.values

  /** Gets the object with the given key.
   */
  def get(key: String): T = map(key)

  override def fetch(key: String,
                     r: RArgument = RArgument(),
                     pr: PRArgument = PRArgument(),
                     basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
                     notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
                     ifModified: IfModifiedArgument = IfModifiedArgument(),
                     onlyHead: OnlyHeadArgument = OnlyHeadArgument(),
                     deletedVClock: DeletedVClockArgument = DeletedVClockArgument()): ValidatedFutureIO[Option[T]] = {
    map.get(key).point[ValidatedFutureIO]
  }

  override def store(obj: T,
                     r: RArgument = RArgument(),
                     pr: PRArgument = PRArgument(),
                     basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
                     notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
                     w: WArgument = WArgument(),
                     dw: DWArgument = DWArgument(),
                     returnBody: ReturnBodyArgument = ReturnBodyArgument(),
                     pw: PWArgument = PWArgument(),
                     ifNotModified: IfNotModifiedArgument = IfNotModifiedArgument(),
                     ifNonMatched: IfNonMatchedArgument = IfNonMatchedArgument(),
                     returnHead: ReturnHeadArgument = ReturnHeadArgument()): ValidatedFutureIO[Option[T]] = {
    add(obj)
    (if (returnBody.v.getOrElse(false)) Some(obj) else None: Option[T]).point[ValidatedFutureIO]
  }

  override def unsafeStoreNew(obj: T,
                              basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
                              notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
                              deletedVClock: DeletedVClockArgument = DeletedVClockArgument(),
                              w: WArgument = WArgument(),
                              dw: DWArgument = DWArgument(),
                              returnBody: ReturnBodyArgument = ReturnBodyArgument(),
                              pw: PWArgument = PWArgument(),
                              ifNotModified: IfNotModifiedArgument = IfNotModifiedArgument(),
                              ifNonMatched: IfNonMatchedArgument = IfNonMatchedArgument(),
                              returnHead: ReturnHeadArgument = ReturnHeadArgument()): ValidatedFutureIO[Option[T]] = {
    store(obj)
  }

  override def delete(obj: T,
                      rw: RWArgument = RWArgument(),
                      vClock: VClockArgument = VClockArgument(),
                      r: RArgument = RArgument(),
                      w: WArgument = WArgument(),
                      pr: PRArgument = PRArgument(),
                      pw: PWArgument = PWArgument(),
                      dw: DWArgument = DWArgument()): ValidatedFutureIO[Unit] = {
    val converted = converter.write(bucketName, obj)
    map.remove(converted.key)
    deleteFromIndexes(mapBinIndex, converted.binIndexes, converted.key)
    deleteFromIndexes(mapIntIndex, converted.intIndexes, converted.key)
    ().point[ValidatedFutureIO]
  }

  override def fetchKeysForBinIndexByValue(idxk: String, idxv: String): ValidatedFutureIO[List[String]] = {
    mapBinIndex.
      getOrElse(idxk, Map[String, ArrayBuffer[String]]()).
      getOrElse(idxv, ArrayBuffer[String]()).toList.point[ValidatedFutureIO]
  }

  override def fetchKeysForIntIndexByValue(idxk: String, idxv: Int): ValidatedFutureIO[List[String]] = {
    mapIntIndex.
      getOrElse(idxk, Map[Int, ArrayBuffer[String]]()).
      getOrElse(idxv, ArrayBuffer[String]()).toList.point[ValidatedFutureIO]
  }

  override def fetchKeysForIntIndexByValueRange(idxk: String, idxr: Range): ValidatedFutureIO[List[String]] = {
    idxr.flatMap(mapIntIndex.get(idxk).get(_)).toList.point[ValidatedFutureIO]
  }

  // Gets the key for the object from the converter
  private def key(obj: T): String = converter.write(bucketName, obj).key

  /** Puts a single value for a single index in the index */
  private def put[T](indexes: Map[String, Map[T, ArrayBuffer[String]]], idxk: String, idxv: T, value: String) {
    val index = indexes.getOrElse(idxk, Map[T, ArrayBuffer[String]]())
    val keyValuePair = index.getOrElse(idxv, ArrayBuffer[String]())
    keyValuePair += value
    index.put(idxv, keyValuePair)
    indexes.put(idxk, index)
  }

  /** Deletes the object keys from the given index values */
  private def deleteFromIndexes[T](indexes: Map[String, Map[T, ArrayBuffer[String]]], indexValues: immutable.Map[String, List[T]], objectKey: String) {
    indexValues.foreach { case (idxk, idxvs) ⇒ idxvs.foreach(idxv ⇒ indexes.get(idxk).get(idxv) -= objectKey) }
  }

}
