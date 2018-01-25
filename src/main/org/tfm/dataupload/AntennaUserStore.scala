package org.tfm.dataupload

import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.tfm.structs.AntennaUser


// Proceso para la carga mensual de clientes en la tabla client de cassandra
object AntennaUserStore {



  def insertReg(reg: AntennaUser): Unit = {

    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.cassandra.connection.port", "9045")
      .setAppName("AntennaUserStore")
      .setMaster("local[*]")
    //val scCassandra = new SparkContext(conf)

    val session = SparkSession.builder()
      .appName("Otra spark")
      .master("local")
      .config(conf)
      .getOrCreate()

    //val sqlContext = new org.apache.spark.sql.SQLContext(scCassandra)
    val sqlContext = new org.apache.spark.sql.SQLContext(session.sparkContext)

    val antennaCass = session.sparkContext.parallelize(Seq(reg))
    antennaCass.saveToCassandra("tfm", "antennabyuser")

  }


}
