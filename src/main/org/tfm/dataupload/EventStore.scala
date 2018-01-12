package tfm.dataupload

import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.tfm.structs.Event

/**
  * EventStore grabamos en una tabla de cassandra todos los eventos de cada una de las antenas por día y hora.
  * Sólo se cuenta una ocurrencia del evento por cliente, y fecha (dos eventos del mismo cliente en un mismo tramo horario se contaría como uno único)
  */
object EventStore {

  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", "127.0.0.1")
    .set("spark.cassandra.connection.port", "9045")
    .setAppName("EventStore")
    .setMaster("local[*]")

  val session = SparkSession.builder()
    .appName("Test spark")
    .master("local")
    .config(conf)
    .getOrCreate()

  val sqlContext = new org.apache.spark.sql.SQLContext(session.sparkContext)

  def insertEvent(evento: Event): Unit = {

    val antennaCass = session.sparkContext.parallelize(Seq(evento))
    antennaCass.saveToCassandra("tfm", "eventocurrbydate")

  }

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


  def main(args: Array[String]): Unit = {

    var  path =  "hdfs://localhost:9000////pfm/events/predict/*Events*.csv"

    val df = session.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .load(path)

    val data = df.select(df("ClientId"), df("Date"), df("AntennaId")).rdd

      .map(t=>((stringToDataPosition(t.get(1).toString), t.get(0).toString, t.get(2).toString.trim)))
      .filter(x=> x._1._1 != "")
        .map(x=> ((x._1._1, x._1._2, x._2, x._3), 1L))
        .reduceByKey( (x,y) => (1L)) // con esto me quito posibles eventos del mismo cliente a la misa antena durante la misma hora
        .map( x => {((x._1._1, x._1._2, x._1._4), 1L)}) // elimino la informacion de los clientes
        .reduceByKey(_+_) // sumo las ocurrencias
        .map(t => { new Event(t._1._1.toString, t._1._2.toInt, t._1._3.toString, t._2.toInt )}).collect().foreach(insertEvent(_)/*println*/)

    session.close()
  }
}
