package org.tfm.dataupload

import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}
import org.tfm.structs.Client


// Proceso para la carga mensual de clientes en la tabla client de cassandra
object ClientStore {

  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", "127.0.0.1")
    .setAppName("ClientStore")
    .setMaster("local[*]")
  val scCassandra = new SparkContext(conf)

  //val sc = new SparkContext("local", "Simple")
  val sqlContext = new org.apache.spark.sql.SQLContext(scCassandra)

  def clientStore(client: Client): Unit = {

    val clienteCass = scCassandra.parallelize(Seq(client))
    clienteCass.saveToCassandra("tfm", "client")

  }

  def main(args: Array[String]): Unit = {

    // leemos los clientes del fichero que tenemos en HDFS

    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";")
      .load("hdfs://localhost:9000////pfm/clientes/Clients.csv")

    import sqlContext.implicits._
    // y hacemos una carga en la tabla client de cassandra
    df.select(df("ClientId"), df("Age"), df("Gender"), df("Nationality"), df("CivilStatus"), df("SocioeconomicLevel"))
      .map(t => {new Client(t(0).toString, t(1).toString.toInt,
                            t(2).toString, t(3).toString,
                            t(4).toString, t(5).toString)} ) .collect().foreach(clientStore(_)/*println*/)

  }

}
