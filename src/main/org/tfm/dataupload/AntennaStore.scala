package org.tfm.dataupload

import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.tfm.structs.{Antenna, Client}


// Proceso para la carga mensual de clientes en la tabla client de cassandra
object AntennaStore {

  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.port", "9045")
    .setAppName("AntenaStore")
    .setMaster("local[*]")
  //val scCassandra = new SparkContext(conf)

  val session = SparkSession.builder()
    .appName("Test spark")
    .master("local")
    .config(conf)
    .getOrCreate()

  //val sqlContext = new org.apache.spark.sql.SQLContext(scCassandra)
  val sqlContext = new org.apache.spark.sql.SQLContext(session.sparkContext)

  def insertAntenna(antena: Antenna): Unit = {

    //val antennaCass = sqlContext.sparkContext.parallelize(Seq(antena))
    val antennaCass = session.sparkContext.parallelize(Seq(antena))
    antennaCass.saveToCassandra("tfm", "antenna")

  }

  def updateLocaliz(antenna: Antenna) : Unit = {

    val tbl = session.sparkContext.cassandraTable("tfm", "antenna")
    //val tbl = sqlContext.sparkContext.cassandraTable("tfm", "antenna")


    val tblfiltered = tbl.filter(item => {item.getString("id") == antenna.id})
   // tblfiltered.collect().foreach(println(_))


    val tbltransformed = tblfiltered.map (item =>{
      val key =  item.getString("id")
      val needsUpdate = antenna.localiz
      ( key, needsUpdate )

    })
    tblfiltered.collect().foreach(println(_))

    tbltransformed.saveToCassandra("tfm", "antenna", SomeColumns("id", "localiz") )

  }

  def main(args: Array[String]): Unit = {

    // leemos los clientes del fichero que tenemos en HDFS

    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";")
      .load("hdfs://localhost:9000////pfm/antenas/Antennas.csv")

    import sqlContext.implicits._
    // y hacemos una carga en la tabla client de cassandra
    df.select(df("AntennaId"), df("Intensity"), df("X"), df("Y"))
      .map(t => { new Antenna(t(0).toString, t(1).toString.toInt,
                            t(2).toString.toFloat, t(3).toString.toFloat)} ) .collect().foreach(insertAntenna(_)/*println*/)

    session.close()
  }

}
