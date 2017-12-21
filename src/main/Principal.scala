import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import com.datastax.spark.connector
import com.datastax.spark.connector._
import com.datastax.driver.core.Cluster
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.PrintWriter

import org.apache.spark.sql.SQLContext

/**
  * Fichero que utilizo para hacer pruebas de conectividad con los distintos sistemas que voy a utilizar
  * (no aplica)
  */


object Principal {
  def main(args: Array[String]): Unit = {
    println("hola")
    //
    // Conexion cassandra
/*
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .setAppName("PruebaCassandra")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val test_spark_rdd = sc.cassandraTable[Antena]("tfm", "antenna")


    val file_collect=test_spark_rdd.collect()
    file_collect.foreach(println)

    // val antena = sc.parallelize(Seq( new Antena("A03", 125, -2.0f, 40.434547f)))
    //antena.saveToCassandra("tfm", "antenna")
*/

    // lectura de hdfs
/*
    println("Trying to read to HDFS...")
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://localhost:9000")
    val hdfs = FileSystem.get(conf)
    val path = new Path("////pfm/antenas/Antennas.csv")
    val stream = hdfs.open(path)

    def readLines = Stream.cons(stream.readLine, Stream.continually(stream.readLine))

    //This example checks line for null and prints every existing line consequentally
    readLines.takeWhile(_ != null).foreach(line => println(line))

*/


/*
    val sc = new SparkContext("local", "Simple")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";")
      .load("hdfs://localhost:9000////pfm/antenas/Antennas.csv")


    df.select(df("AntennaId"), df("Intensity"), df("X"), df("Y"))
      .map(t => {new Antena(t(0).toString, t(1).toString.toInt, t(2).toString.toFloat, t(3).toString.toFloat)} ) .collect().foreach(println)

*/
    /*
  val e = new EventsStore
    //e.cargaInicial("2017/11/05")
    e.modificarOcurrencia("A02", "2017/11/05", 0, 100)
*/


    println("HOLA")
  }
}
