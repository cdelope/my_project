package org.tfm.dataupload

import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.tfm.structs.Antenna


// Proceso para la carga mensual de clientes en la tabla client de cassandra
object AntennaStore {

  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", Conf._cassandra_host)
    .set("spark.cassandra.connection.port", Conf._cassandra_port)
    .setAppName(Conf._app_name_antenna)
    .setMaster(Conf._master)

  val session = SparkSession.builder()
    //.appName("Test spark")
    //.master("local")
    .config(conf)
    .getOrCreate()

  val sqlContext = new org.apache.spark.sql.SQLContext(session.sparkContext)

  def insertAntenna(antena: Antenna): Unit = {

    val antennaCass = session.sparkContext.parallelize(Seq(antena))
    antennaCass.saveToCassandra(Conf._schema, Conf._table_name_antenna)

  }

  def updateLocaliz(antenna: Antenna) : Unit = {

    val tbl = session.sparkContext.cassandraTable(Conf._schema, Conf._table_name_antenna)

    val tblfiltered = tbl.filter(item => {item.getString("id") == antenna.id})

    val tbltransformed = tblfiltered.map (item =>{
      val key =  item.getString("id")
      val needsUpdate = antenna.localiz
      ( key, needsUpdate )

    })

    tbltransformed.saveToCassandra(Conf._schema, Conf._table_name_antenna, SomeColumns("id", "localiz") )

  }

  def main(args: Array[String]): Unit = {

    // leemos los clientes del fichero que tenemos en HDFS

    val df = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .load(Conf._hdfs_path_antenna)

    import sqlContext.implicits._

    // y hacemos una carga en la tabla antenna de cassandra
    df.select(df("AntennaId"), df("Intensity"), df("X"), df("Y"))
      .map(t => { new Antenna(t(0).toString, t(1).toString.toInt,
                            t(2).toString.toFloat, t(3).toString.toFloat)} )
      .toDF().write.format(Conf._cassandra_format)
      .options(Map( "keyspace" -> Conf._schema,
        "table" -> Conf._table_name_antenna ))
      .mode("append")
      .save()

    session.close()
  }

}
