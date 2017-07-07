package com.aerospike.spark.sql

import org.scalatest.{FlatSpec, Matchers}
import com.aerospike.client.{AerospikeClient, AerospikeException, Bin, Key}
import com.aerospike.client.query.Statement

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions.asScalaIterator
import com.aerospike.client.policy.WritePolicy
import com.aerospike.helper.query.QueryEngine
import com.aerospike.spark.SparkASITSpecBase
import com.aerospike.spark.Globals


class QueyEngineTest extends FlatSpec with Matchers with SparkASITSpecBase{

  val ages = Array(25,26,27,28,29)
  val colours = Array("blue","red","yellow","green","orange")
  val animals = Array("cat","dog","mouse","snake","lion")

  val wp = new WritePolicy()
  wp.expiration = 600

  behavior of "QueryEngine from Scala"

  it should "Get a query engine from cache" in {
    val qe = AerospikeConnection.getQueryEngine(AerospikeConfig(conf))
    qe shouldBe a [QueryEngine]
  }

  it should "Insert data" in {
    val cl = AerospikeConnection.getClient(conf)

    var i = 0
    for (x <- 1 to 100) {
      val name = new Bin("name", "name:" + i)
      val age = new Bin("age", ages(i))
      val colour = new Bin("color", colours(i))
      val animal = new Bin("animal", animals(i))
      val key = new Key(Globals.namespace, "selector", "selector-test:"+ x)
      cl.put(null, key, name, age, colour, animal)
      i += 1
      if (i == 5) i = 0
    }
  }

  it should "Select data" in {
    val qe = AerospikeConnection.getQueryEngine(AerospikeConfig(conf))
    val stmt = new Statement()
    stmt.setNamespace(Globals.namespace)
    stmt.setSetName("selector")
    val it = qe.select(stmt)
    for (row <- it) {
      val key = row.key
      val rec = row.record
      print(".")
    }
    println
    it.close()
  }
  it should "run concurrent selects" in {

    var threads = new ListBuffer[Thread]()
    for (i <- 1 to 50) {
      val thread = new Thread {
        override def run() {

          val qe = AerospikeConnection.getQueryEngine(AerospikeConfig(conf))
          val stmt = new Statement()
          stmt.setNamespace(Globals.namespace)
          stmt.setSetName("selector")
          val it = qe.select(stmt)
          var count = 0
          try {
            while (it.hasNext) {
              val keyRecord = it.next()
              val key = keyRecord.key
              val record = keyRecord.record
              count = count + 1
            }
          } catch {
            case ae: AerospikeException => println(ae)
            case e:Exception => println(e)
          }
          finally {
            it.close()
          }
        }
      }
      thread.start()
      threads += thread
    }
    for (t <- threads) t.join()

  }
  it should "Select data by node" in {
    val nodes = AerospikeConnection.getClient(conf).getNodes

    var threads = new ListBuffer[Thread]()
    for (i <- 0 until nodes.length) {
      val thread = new Thread {
        override def run() {
          val qe = AerospikeConnection.getQueryEngine(AerospikeConfig(conf))
          val stmt = new Statement()
          stmt.setNamespace(Globals.namespace)
          stmt.setSetName("selector")
          val it = qe.select(stmt, false, nodes(i))
          var  count = 0
          for (row <- it) {
            val key = row.key
            val rec = row.record
            count = count + 1
          }
          it.close()
        }
      }
      thread.start()
      threads += thread
    }
    for (t <- threads) t.join()
  }


  it should "clean up because it's mother doesn't work here" in {
    val cl = AerospikeConnection.getClient(conf)

    var i = 0
    for (x <- 1 to 100) {
      val name = new Bin("name", "name:" + i)
      val age = new Bin("age", ages(i))
      val colour = new Bin("color", colours(i))
      val animal = new Bin("animal", animals(i))
      val key = new Key(Globals.namespace, "selector", "selector-test:"+ x)
      cl.delete(null, key)
      i += 1
      if ( i == 5) i = 0
    }
  }

}
