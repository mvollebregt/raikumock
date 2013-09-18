package com.github.mvollebregt.raikumock

import org.specs2.mutable._
import org.specs2.specification.Scope
import scalaz._
import scalaz.Scalaz._
import spray.json._
import spray.json.DefaultJsonProtocol._

import nl.gideondk.raiku._
import commands.RWObject

class MockRaikuBucketSpec extends Specification {

  case class TestModel(key: String, value: String, binIndex1: List[String] = List(), binIndex2: List[String] = List(), intIndex: List[Int] = List())

  implicit val format = jsonFormat5(TestModel.apply)

  implicit val converter = new RaikuConverter[TestModel] {
    def read(o: RWObject) = try {
      format.read(new String(o.value).asJson).success
    }
    catch {
      case e: Throwable ⇒ e.failure
    }
    def write(bucket: String, obj: TestModel): RWObject = RWObject(
      bucket,
      obj.key,
      obj.toJson.toString.getBytes,
      binIndexes = Map("binIndex1" -> obj.binIndex1, "binIndex2" -> obj.binIndex2),
      intIndexes = Map("intIndex" -> obj.intIndex))
  }

  trait KeyValueFixture extends Scope {
    val bucket = new MockRaikuBucket[TestModel]()
    bucket.init(TestModel("key", "value"))
  }

  trait IndexFixture extends Scope {
    val bucket = new MockRaikuBucket[TestModel]
    val NO_INDEX = TestModel("key", "value")
    val SINGLE_INDEX = TestModel("key1", "value1", binIndex1 = List("key 1"))
    val MULTI_INDEX = TestModel("key2", "value2", binIndex1 = List("key 1"), binIndex2 = List("key 1"), intIndex = List(1))
    val MULTI_KEY_INDEX = TestModel("key3", "value3", binIndex1 = List("key 2"), binIndex2 = List("key 1", "key 2"), intIndex = List(1, 2))
    bucket.init(NO_INDEX, SINGLE_INDEX, MULTI_INDEX, MULTI_KEY_INDEX)
  }

  "containsKey" should {
    "return true if the key as added" in new KeyValueFixture() {
      bucket.containsKey("key") must_== true
    }
    "return false if the key was not added" in new KeyValueFixture() {
      bucket.containsKey("other key") must_== false
    }
    "return true if the key was overriden" in new KeyValueFixture() {
      bucket.add(new TestModel("key", "new value"))
      bucket.containsKey("key") must_== true
    }
    "return false if the key was deleted" in new KeyValueFixture() {
      bucket.delete(new TestModel("key", "value")).unsafeFulFill()
      bucket.containsKey("key") must_== false
    }
  }

  "contains" should {
    "return true if the exact same object was added" in new KeyValueFixture() {
      bucket.contains(TestModel("key", "value")) must_== true
    }
    "return false if another object with the same key was added" in new KeyValueFixture() {
      bucket.contains(TestModel("key", "other value")) must_== false
    }
    "return false if the key was not added at all" in new KeyValueFixture() {
      bucket.contains(TestModel("other key", "value")) must_== false
    }
    "return false if the object was overriden with another" in new KeyValueFixture() {
      bucket.add(new TestModel("key", "new value"))
      bucket.contains(new TestModel("key", "value")) must_== false
    }
    "return true for the overriden object" in new KeyValueFixture() {
      bucket.add(new TestModel("key", "new value"))
      bucket.contains(new TestModel("key", "new value")) must_== true
    }
  }

  "fetchKeysForIntIndexByValue" should {
    "return a single object" in new IndexFixture() {
      bucket.fetchKeysForIntIndexByValue("intIndex", 2).unsafeFulFill() must_== Success(List(MULTI_KEY_INDEX.key))
    }
    "return two objects" in new IndexFixture() {
      bucket.fetchKeysForIntIndexByValue("intIndex", 1).unsafeFulFill() must_== Success(List(MULTI_INDEX.key, MULTI_KEY_INDEX.key))
    }
    "return empty list if value does not exist" in new IndexFixture() {
      bucket.fetchKeysForIntIndexByValue("intIndex", 0).unsafeFulFill() must_== Success(List())
    }
    "return empty list if key does not exist" in new IndexFixture() {
      bucket.fetchKeysForIntIndexByValue("non existing index", 2).unsafeFulFill() must_== Success(List())
    }
    "return an empty list if single object was deleted" in new IndexFixture() {
      bucket.delete(MULTI_KEY_INDEX).unsafeFulFill()
      bucket.fetchKeysForIntIndexByValue("intIndex", 2).unsafeFulFill() must_== Success(List())
    }
    "return a single object if other object was deleted" in new IndexFixture() {
      bucket.delete(MULTI_KEY_INDEX).unsafeFulFill()
      bucket.fetchKeysForIntIndexByValue("intIndex", 1).unsafeFulFill() must_== Success(List(MULTI_INDEX.key))
    }
    "return a single object if index for other object was removed" in new IndexFixture() {
      bucket.add(MULTI_INDEX.copy(intIndex = List()))
      bucket.fetchKeysForIntIndexByValue("intIndex", 1).unsafeFulFill() must_== Success(List(MULTI_KEY_INDEX.key))
    }
    "return a single object if index for other object was changed" in new IndexFixture() {
      bucket.add(MULTI_INDEX.copy(intIndex = List(3)))
      bucket.fetchKeysForIntIndexByValue("intIndex", 1).unsafeFulFill() must_== Success(List(MULTI_KEY_INDEX.key))
    }
    "return two objects if index for other object was changed to same value" in new IndexFixture() {
      bucket.add(MULTI_INDEX.copy(intIndex = List(2)))
      bucket.fetchKeysForIntIndexByValue("intIndex", 2).unsafeFulFill() must_== Success(List(MULTI_KEY_INDEX.key, MULTI_INDEX.key))
    }
    "still return a single object if index was overriden with same value" in new IndexFixture() {
      bucket.add(MULTI_KEY_INDEX)
      bucket.fetchKeysForIntIndexByValue("intIndex", 2).unsafeFulFill() must_== Success(List(MULTI_KEY_INDEX.key))
    }
  }

}
