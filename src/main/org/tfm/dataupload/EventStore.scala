package org.tfm.dataupload

import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.tfm.structs.{Client, Event}

/**
  * EventStore grabamos en una tabla de cassandra todos los eventos de cada una de las antenas por día y hora.
  * Sólo se cuenta una ocurrencia del evento por cliente, y fecha (dos eventos del mismo cliente en un mismo tramo horario se contaría como uno único)
  */
object EventStore {

  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", Conf._cassandra_host)
    .set("spark.cassandra.connection.port", Conf._cassandra_port)
    .setAppName("EventStore")
    .setMaster(Conf._master)

  val session = SparkSession.builder()
    .appName("Test spark")
    .master("local")
    .config(conf)
    .getOrCreate()

  val sqlContext = new org.apache.spark.sql.SQLContext(session.sparkContext)


  def stringToDataPosition(data:String) : (String, Int) = {
    try{

      // fecha viene con el formato DD/MM/YYYY
      val fecha = data.substring(0, data.indexOf('-'))
      val hora = data.substring(data.indexOf('-')+1, data.indexOf(':'))


      import java.text.SimpleDateFormat
      val inputDate = new SimpleDateFormat("dd/MM/yyyy").parse(fecha)

      (fecha.toString, hora.toInt)

    } catch {
      case e: Exception => {
        ("", -1)
      }
    }
  }

  def convertToDate(fecha: String) : java.sql.Date = {

    import java.text.SimpleDateFormat
    val inputDate = new SimpleDateFormat("dd/MM/yyyy").parse(fecha)

    val sql = new java.sql.Date(inputDate.getTime());
    sql

  }


  /**
    * Proceso por el que, a partir de los ficheros de eventos, rellenamos las tablas:
    * - event -> tabla de eventos (fecha, hora, antennaid, userid)
    * - eventOcurrByDate (fecha, hora, antennaid, ocurrencia)
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val df = session.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ";")
        .load(Conf._hdfs_path_event_predict)

    val df2 = session.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .load(Conf._hdfs_path_event_train)

    val data = df.select(df("ClientId"), df("Date"), df("AntennaId")).union(df2.select(df2("ClientId"), df2("Date"), df2("AntennaId"))).rdd

    // ---------------------------------------------
    // eventOcurrByDate
    // ---------------------------------------------
    val events =  data.map(t=>((stringToDataPosition(t.get(1).toString), t.get(0).toString, t.get(2).toString.trim)))
                      .filter(x=> x._1._1 != "")

   /* events.map(x=> ((x._1._1, x._1._2, x._2, x._3), 1L))
        .reduceByKey( (x,y) => (1L)) // con esto me quito posibles eventos del mismo cliente a la misa antena durante la misma hora
        .map( x => {((x._1._1, x._1._2, x._1._4), 1L)}) // elimino la informacion de los clientes
        .reduceByKey(_+_) // sumo las ocurrencias
        .map(t => { new EventContByDate(t._1._1.toString, t._1._2.toInt, t._1._3.toString, t._2.toInt )})
      .saveToCassandra(Conf._schema, Conf._table_name_eventbydate)*/

    // ---------------------------------------------
    // event
    // ---------------------------------------------
    val events_net = events.map(x => {
      new Event((x._1._1), x._1._2, x._3, x._2)
    })

    import session.implicits._
    val clients = session.sparkContext.cassandraTable[Client](Conf._schema, Conf._table_name_client)
      .select("clientid", "gender", "age", "civilstatus", "nationality", "socioeconlev")
      .toDS()

    val eventosDF = events.map(x => {(x._1._1, x._1._2, x._3, x._2)}).toDF("fecha", "hora", "antennaid", "userid")
    val eventos_clientes = eventosDF.join(clients, eventosDF("userid")===clients("clientid"), "inner")

    events_net.saveToCassandra(Conf._schema, Conf._tabla_name_event)

    eventos_clientes.toDF().write.format("org.apache.spark.sql.cassandra")
      .options(Map( "keyspace" -> Conf._schema,
        "table" -> Conf._tabla_name_eventadv ))
      .mode("append")
      .save()

    session.close()
  }
}
