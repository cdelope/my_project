package org.tfm.dataupload

import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}
import org.tfm.structs.{Antenna, City}


// Proceso para la carga mensual de clientes en la tabla client de cassandra
object CityStore {

  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", "127.0.0.1")
    .setAppName("AntenaStore")
    .setMaster("local[*]")
  val scCassandra = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.SQLContext(scCassandra)

  def storeCity(city: City): Unit = {

    val cityCass = scCassandra.parallelize(Seq(city))
    cityCass.saveToCassandra("tfm", "city")

  }

  def tupleOfFloats(data: String) : (Float, Float) = {
    println("Data: -> " + data)
    val pos = data.indexOf(",")
    val float1 = data.substring(0, pos)
    val float2 = data.substring(pos + 1, data.length)
    println(data + " : "  + float1 + "--" + float2)
    (float1.toFloat, float2.toFloat)
  }

  def main(args: Array[String]): Unit = {

    tupleOfFloats("-3.7906265259,40.3530853269")
    tupleOfFloats("-3.5769081116,40.3530853269")
    tupleOfFloats("-3.5769081116,40.5349377098")
    tupleOfFloats("-3.7906265259,40.5349377098")
    tupleOfFloats("-3.7906265259,40.3530853269")

    // leemos los clientes del fichero que tenemos en HDFS

    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";")
      .load("hdfs://localhost:9000////pfm/cities/cities.csv")

    import sqlContext.implicits._
    // y hacemos una carga en la tabla client de cassandra
    df.select(df("CityName"), df("Population"), df("X1"), df("X2"), df("X3"), df("X4"), df("X5"))
      .map(t => {new City(t(0).toString, t(1).toString.toInt,
                          tupleOfFloats(t(2).toString), tupleOfFloats(t(3).toString),
                          tupleOfFloats(t(4).toString), tupleOfFloats(t(5).toString),
                          tupleOfFloats(t(6).toString)
      )} ) .collect().foreach(storeCity(_)/*println*/)

  }

}
