package com.aerospike.spark.sql

import scala.collection.immutable.Map
import com.aerospike.client.policy.CommitLevel
import com.aerospike.client.policy.GenerationPolicy
import org.apache.spark.SparkConf
import org.apache.spark.sql.RuntimeConfig

/**
  * this class is a container for the properties used during the
  * the read and save functions
  */
case class AerospikeConfig private(val properties: Map[String, Any]) extends Serializable {

  def get(key: String): Any =
    properties.getOrElse(key.toLowerCase(), notFound(key))

  def getIfNotEmpty(key: String, defaultVal: Any): Any = {
    properties.getOrElse(key.toLowerCase(), defaultVal)
      match {
      case n: Number => n.longValue
      case b: Boolean => if(b) 1 else 0
      case s: String  => if (s.isEmpty) defaultVal else s
      case _ => null
    }
  }

  def namespace(): String = {
    get(AerospikeConfig.NameSpace).asInstanceOf[String]
  }

  def set(): String = {
    get(AerospikeConfig.SetName).asInstanceOf[String]
  }

  def seedHost(): String = {
    get(AerospikeConfig.SeedHost).asInstanceOf[String]
  }

  def port(): Int = {
    get(AerospikeConfig.Port).asInstanceOf[Int]
  }

  def schemaScan(): Int = {
    get(AerospikeConfig.SchemaScan).asInstanceOf[Int]
  }

  def timeOut(): Int = {
    get(AerospikeConfig.TimeOut).asInstanceOf[Int]
  }
  def keyColumn(): String = {
    get(AerospikeConfig.KeyColumn).asInstanceOf[String]
  }

  def digestColumn(): String = {
    get(AerospikeConfig.DigestColumn).asInstanceOf[String]
  }

  def expiryColumn(): String = {
    get(AerospikeConfig.ExpiryColumn).asInstanceOf[String]
  }

  def generationColumn(): String = {
    get(AerospikeConfig.GenerationColumn).asInstanceOf[String]
  }

  def ttlColumn(): String = {
    get(AerospikeConfig.TTLColumn).asInstanceOf[String]
  }

  override def toString: String = {
    val buff = new StringBuffer("[")
    properties.map(f => {
      buff.append("{")
      buff.append(f._1)
      buff.append("=")
      buff.append(f._2)
      buff.append("}")
    })
    buff.append("]")
    buff.toString
  }

  private def notFound[T](key: String): T =
    throw new IllegalStateException(s"Config item $key not specified")
}

object AerospikeConfig {

  final val DEFAULT_SEED_HOST = "127.0.0.1"

  private val defaultValues = scala.collection.mutable.Map[String, Any](
    AerospikeConfig.SeedHost -> DEFAULT_SEED_HOST,
    AerospikeConfig.Port -> 3000,
    AerospikeConfig.SchemaScan -> 100,
    AerospikeConfig.TimeOut -> 1000,
    AerospikeConfig.NameSpace -> "test",
    AerospikeConfig.KeyColumn -> "__key",
    AerospikeConfig.DigestColumn -> "__digest",
    AerospikeConfig.ExpiryColumn -> "__expiry",
    AerospikeConfig.GenerationColumn -> "__generation",
    AerospikeConfig.TTLColumn -> "__ttl",
    AerospikeConfig.SaveMode -> "ignore")

  val SeedHost = "aerospike.seedhost"
  defineProperty(SeedHost, "127.0.0.1")

  val Port = "aerospike.port"
  defineProperty(Port, 3000)
  
  val MaxThreadCount = "aerospike.maxthreadcount"
  defineProperty(MaxThreadCount, 1)

  val TimeOut = "aerospike.timeout"
  defineProperty(TimeOut, 1000)
  
  val SocketTimeOut = "aerospike.sockettimeout"
  defineProperty(SocketTimeOut, 0)

  val sendKey = "aerospike.sendKey"
  defineProperty(sendKey, false)

  val commitLevel = "aerospike.commitLevel"
  defineProperty(commitLevel, CommitLevel.COMMIT_ALL)

  val generationPolicy = "aerospike.generationPolicy"
  defineProperty(generationPolicy, GenerationPolicy.NONE)

  val NameSpace = "aerospike.namespace"
  defineProperty(NameSpace, "test")

  val SetName = "aerospike.set"
  defineProperty(SetName, null)

  val UpdateByKey = "aerospike.updateByKey"
  defineProperty(UpdateByKey, null)

  val UpdateByDigest = "aerospike.updateByDigest"
  defineProperty(UpdateByDigest, null)

  val SchemaScan = "aerospike.schema.scan"
  defineProperty(SchemaScan, 100)

  val KeyColumn = "aerospike.keyColumn"
  defineProperty(KeyColumn, "__key")

  val DigestColumn = "aerospike.digestColumn"
  defineProperty(DigestColumn, "__digest")

  val ExpiryColumn = "aerospike.expiryColumn"
  defineProperty(ExpiryColumn, "__expiry")

  val GenerationColumn = "aerospike.generationColumn"
  defineProperty(GenerationColumn, "__generation")

  val TTLColumn = "aerospike.ttlColumn"
  defineProperty(TTLColumn, "__ttl")

  val SaveMode = "aerospike.savemode"
  defineProperty(SaveMode, "ignore")
  
  val BatchMax = "aerospike.batchMax"
  defineProperty(BatchMax, 500)

  private def defineProperty(key: String, defaultValue: Any) : Unit = {
    val lowerKey = key.toLowerCase()
    if(defaultValues.contains(lowerKey))
      sys.error(s"Config property already defined for key : $key")
    else
      defaultValues.put(lowerKey, defaultValue)
  }

  def apply(seedHost:String, port: Int, timeOut:Any ): AerospikeConfig = {
    newConfig(Map(
        AerospikeConfig.SeedHost -> seedHost,
        AerospikeConfig.Port -> port,
        AerospikeConfig.TimeOut -> timeOut
    ))
  }

  def apply(conf:SparkConf): AerospikeConfig = {
    newConfig(conf.getAll.toMap)
  }
  
  def apply(conf:RuntimeConfig): AerospikeConfig = {
    newConfig(conf.getAll.toMap)
  }

  def newConfig(props: Map[String, Any] = Map.empty): AerospikeConfig = {
    if (props.nonEmpty) {
      val ciProps = props.map(kv => kv.copy(_1 = kv._1.toLowerCase))

      ciProps.keys.filter(_.startsWith("aerospike.")).foreach { x =>
        if(!defaultValues.contains(x))
          sys.error(s"Unknown Aerospike specific option : $x")
      }
      val mergedProperties = defaultValues.toMap ++ ciProps
      new AerospikeConfig(mergedProperties)
    } else {
      new AerospikeConfig(defaultValues.toMap)
    }
  }
}
