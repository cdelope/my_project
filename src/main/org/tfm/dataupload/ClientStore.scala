package org.tfm.dataupload

import com.datastax.spark.connector._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.tfm.structs.Client


// Proceso para la carga mensual de clientes en la tabla client de cassandra

object ClientStore {

  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", Conf._cassandra_host)
    .set("spark.cassandra.connection.port", Conf._cassandra_port)
    .setAppName(Conf._app_name_client)
    .setMaster(Conf._master)

  val session = SparkSession.builder()
    //.appName("Test spark")
    //.master("local")
    .config(conf)
    .getOrCreate()

  val sqlContext = new org.apache.spark.sql.SQLContext(session.sparkContext)

  /**
    * Guardamos el cliente en la tabla "client" de cassandra
    * @param client: cliente a almacenar
    */
  def clientStore(client: Client): Unit = {

    val clienteCass = session.sparkContext.parallelize(Seq(client))
    clienteCass.saveToCassandra(Conf._schema, Conf._table_name_client)

  }

  /**
    * Recogemos un cliente a partir de su DNI de la base de datos client de cassandra
    * @param dni: DNI del cliente
    * @return
    */
  def getClient (dni: String) : Client = {
    val client = session.sparkContext.cassandraTable[Client](Conf._schema, Conf._table_name_city)
      .where ("clientid = '" +  dni + "'")
      .select("clientid", "gender", "age", "civilstatus", "nationality", "socioeconlev")

    val p = client.first()
    p

  }


  /**
    * Procedimento por el cual volcamos la información de los clientes desde un fichero HDFS a una tabla de cassandra "client"
    * @param args
    */
  def main(args: Array[String]): Unit = {

    // leemos los clientes del fichero que tenemos en HDFS (definimos el fichero que vamos a leer)
    val df = session.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .load(Conf._hdfs_path_client)

    import sqlContext.implicits._
    // insertamos los clientes en cassandra
    df.select(df("ClientId"), df("Age"), df("Gender"), df("Nationality"), df("CivilStatus"), df("SocioeconomicLevel"))
      .map(t => {new Client(t(0).toString, t(1).toString.toInt,
                            t(2).toString, t(3).toString,
                            t(4).toString, t(5).toString)} )
      .toDF().write.format(Conf._cassandra_format)
      .options(Map( "keyspace" -> Conf._schema, "table" -> Conf._table_name_client ))
      .mode("append")
      .save()

  }

}
