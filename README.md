# Aerospike Spark Connector

The Aerospike Spark Connector provides features to represent data stored in [Aerospike](http://www.aerospike.com/) NoSQL database as a DataFrame in Spark.
 
Aerospike Spark Connector includes:
- Reading from Aerospike to a DataFrame
- Saving a DataFrame to Aerospike
- Spark SQL multiple filters pushed down to the Aerospike cluster

## How to build

The source code for this solution is available on GitHub at [https://github.com/aerospike/aerospark](https://github.com/aerospike/aerospark). SBT is the build tool and it will create a Uber (fat) jar as the final output of the build process. The jar will contain all the class files and dependencies.

This library requires Java JDK 7+, Scala 2.11, SBT 0.13, and Docker running locally in order to perform the tests.

Clone the [Aerospike Spark](https://github.com/aerospike/aerospark) repository using this command:

```bash
$ git clone https://github.com/aerospike/aerospark
```

After cloning the repository, build the uber jar using:

```bash
$ sbt assembly
```

Note that during the build, a number of unit tests are run that create a Docker process automatically for an Aerospike database. If you want to ignore the unit tests, use:

```bash
$ sbt 'set test in assembly := {}' assembly
```

On conclusion of the build, the uber JAR `aerospike-spark-assembly-<version>.jar` will be located in the subdirectory `target/scala-2.11`.

## Usage

The assembled JAR can be used in any Spark application.

### spark shell

To use connector with the spark-shell, use the `--jars` command line option and include the path to the assembled JAR.

Example:

```bash
$ spark-shell --jars target/scala-2.11/aerospike-spark-assembly-1.2.jar
```

Import the `com.aerospike.spark.sql._` package

```
scala> import com.aerospike.spark.sql._
import com.aerospike.spark.sql._
```

and any Aerospike packages and classes. For example:

```
scala> import com.aerospike.client.AerospikeClient
import com.aerospike.client.AerospikeClient

scala> import com.aerospike.client.Bin
import com.aerospike.client.Bin

scala> import com.aerospike.client.Key
import com.aerospike.client.Key

scala> import com.aerospike.client.Value
import com.aerospike.client.Value

```

Load some data into Aerospike with:

```scala
val TEST_COUNT = 100
val namespace = "test"
var client=AerospikeConnection.getClient(AerospikeConfig.newConfig("localhost",3000,1000))
Value.UseDoubleType = true
for (i <- 1 to TEST_COUNT) {
  val key = new Key(namespace, "rdd-test", "rdd-test-"+i)
  client.put(null, key,
     new Bin("one", i),
     new Bin("two", "two:"+i),
     new Bin("three", i.toDouble)
  )
}
```

Try a test with the loaded data:

```scala
import org.apache.spark.sql.{ SQLContext, SparkSession, SaveMode}
import org.apache.spark.SparkConf
val conf = new SparkConf().
  setMaster("local[2]").
  setAppName("Aerospike Tests for Spark Dataset").
  set("aerospike.seedhost", "localhost").
  set("aerospike.port",  "3000").
  set("aerospike.namespace", "test").
  set("stream.orig.url", "localhost")

val spark = SparkSession.builder().
  config(conf).
  master("local[*]").
  appName("Aerospike Tests").
  config("spark.ui.enabled", "false").
  getOrCreate()

val thingsDF = spark.
  read.
  format("com.aerospike.spark.sql").
  option("aerospike.set", "rdd-test").
  load

import spark.implicits._
val thing = thingsDF.filter($"one" === 55).first()
```

### Loading and Saving DataFrames

The Aerospike Spark Connector provides functions to load data from Aerospike into a DataFrame and save a DataFrame into Aerospike

#### Loading data

```scala
import org.apache.spark.sql.SparkSession
val spark: SparkSession = ...
val thingsDF = spark.
  read.
  format("com.aerospike.spark.sql").
  option("aerospike.set", "rdd-test").
  load
```

You can see that the read function is configured by a number of options, these are:

- `format("com.aerospike.spark.sql")` specifies the format to load the DataFrame and points to the Aerospike Spark Connector
- `option("aerospike.set", "rdd-test")` specifies the Set to be used e.g. `rdd-test`

Spark SQL can be used to efficiently filter (e.g. `where lastName = 'Smith'`) Bin values represented as columns.

The filter is passed down to the Aerospike cluster and filtering is done in the server.

```scala
val thingsDF = spark.
  read.
  format("com.aerospike.spark.sql").
  option("aerospike.set", "rdd-test").
  load

import spark.implicits._
val thing = thingsDF.filter($"one" === 55).first()
```

Additional meta-data columns are automatically included when reading from Aerospike, the default names are:

- `__key` the values of the primary key if it is stored in Aerospike
- `__digest` the digest as `Array[byte]`
- `__generation` the gereration value of the record read
- `__expitation` the expiration epoch
- `__ttl` the time to live value calcualed from the expiration - now
 
These meta-data column name defaults can be be changed by using additional options during read or write, for example:

```scala
val thingsDF = spark.
  read.
  format("com.aerospike.spark.sql").
  option("aerospike.set", "rdd-test").
  option("aerospike.expiryColumn", "_my_expiry_column").
  load
```

#### Saving data

A DataFrame can be saved to a Aerospike database by specifying a column in the DataFrame as the Primary Key or the Digest.

##### Saving by Digest

In this example, the value of the digest is specified by the `__digest` column in the DataFrame.

```scala
val thingsDF = spark.
  read.
  format("com.aerospike.spark.sql").
  option("aerospike.set", "rdd-test").
  load

import org.apache.spark.sql.SaveMode
thingsDF.
  write.
  mode(SaveMode.Overwrite).
  format("com.aerospike.spark.sql").
  option("aerospike.set", "rdd-test").
  option("aerospike.updateByDigest", "__digest").
  save
```
##### Saving by Key

In this example, the value of the primary key is specified by the `key` column in the DataFrame.

```
import org.apache.spark.sql.SparkSession
val spark: SparkSession = ...
import spark.implicits._

import org.apache.spark.sql.types.StructType
val schema = new StructType().
  add($"key".string.copy(nullable = false)).
  add($"last".string).
  add($"first".string).
  add($"when".long)

import org.apache.spark.sql.Row
val rowRDD = Seq(
  ("Fraser_Malcolm","Fraser", "Malcolm", 1975L),
  ("Hawke_Bob","Hawke", "Bob", 1983L),
  ("Keating_Paul","Keating", "Paul", 1991L),
  ("Howard_John","Howard", "John", 1996L),
  ("Rudd_Kevin","Rudd", "Kevin", 2007L),
  ("Gillard_Julia","Gillard", "Julia", 2010L),
  ("Abbott_Tony","Abbott", "Tony", 2013L),
  ("Tunrbull_Malcom","Tunrbull", "Malcom", 2015L)).toDF.rdd
val newDF = spark.createDataFrame(rowRDD, schema)
scala> newDF.show
+---------------+--------+-------+----+
|            key|    last|  first|when|
+---------------+--------+-------+----+
| Fraser_Malcolm|  Fraser|Malcolm|1975|
|      Hawke_Bob|   Hawke|    Bob|1983|
|   Keating_Paul| Keating|   Paul|1991|
|    Howard_John|  Howard|   John|1996|
|     Rudd_Kevin|    Rudd|  Kevin|2007|
|  Gillard_Julia| Gillard|  Julia|2010|
|    Abbott_Tony|  Abbott|   Tony|2013|
|Tunrbull_Malcom|Tunrbull| Malcom|2015|
+---------------+--------+-------+----+

import org.apache.spark.sql.SaveMode
newDF.
  write.
  mode(SaveMode.Ignore).
  format("com.aerospike.spark.sql").
  option("aerospike.set", "rdd-test").
  option("aerospike.updateByKey", "key").
  save
```

##### Using TTL while saving 

Time to live (TTL) can be set individually on each record. The TTL should be stored in a column in the DataSet before it is saved. 

To enable updates to TTL, and additional option is specified:

```scala
option("aerospike.ttlColumn", "expiry")
```
### Schema

Aerospike is Schema-less but Spark's DataFrames require a schema. To facilitate the need for schema, the Aerospike Spark Connector samples 100 records, via a scan, and reads the `Bin` names and infers the `Bin` type.

The number of records scanned can be changed by using the option:

```scala
option("aerospike.schema.scan", 20)
```

Note: the schema is derived each time `load` is called. If you call `load` before the Aerospike namespace/set has any data, only the meta-data columns will be available.

## Save mode reference

Save mode| Record Exists Policy
---------|---------------------
ErrorIfExists|CREATE_ONLY
Ignore|CREATE_ONLY
Overwrite|REPLACE
Append|UPDATE_ONLY

## Options reference

Option|Description|Default value
------|-----------|-------------
aerospike.commitLevel|Consistency guarantee when committing a transaction on the server|CommitLevel.COMMIT_ALL
aerospike.digestColumn|The name of the digest column in the Data Frame| `__digest`
aerospike.expiryColumn|The name of the expiry column in the Data Frame| `__expiry`
aerospike.generationColumn|The name of the generation column in the Data Frame| `__generation`
aerospike.generationPolicy|ow to handle record writes based on record generation| `GenerationPolicy.NONE`
aerospike.keyColumn|The name of the key column in the Data Frame| `__key`
aerospike.namespace|Aerospike Namespace| `test`
aerospike.port|Port of Aerospike| 3000
aerospike.schema.scan|The number of records to scan to infer schema| 100
aerospike.seedhost|A host name or address of the cluster| `localhost`
aerospike.sendKey| When enabled, store the value of the primary key| false
aerospike.set|Aerospike Set| (empty)
aerospike.timeout|Timeout for all operations in milliseconds| 1000
aerospike.ttlColumn|The name of the TTL column in the Data Frame| `__ttl`
aerospike.updateByDigest|This option specifies that updates are done by digest with the value in the column specified ```option("aerospike.updateByDigest", "Digest")```|
aerospike.updateByKey|This option specifies that updates are done by key with the value in the column specified ```option("aerospike.updateByKey", "key")```|


