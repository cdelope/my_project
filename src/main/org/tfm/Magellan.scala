package org.tfm

import com.datastax.spark.connector._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.tfm.dataupload.Conf
import org.tfm.structs.Antenna
/**
  * Proceso encargado de identificar la ciudad en la que están
  * situadas las antenas.
  * utilizamos la librería magellan
  */
object Magellan {
  def main(args: Array[String]): Unit = {

    val path = Conf._geo_json

    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", Conf._cassandra_host)
      .set("spark.cassandra.connection.port", Conf._cassandra_port)

    val session = SparkSession.builder()
      .appName("Magellan")
      .master("local")
      .config(conf)
      .getOrCreate()


    val df = session.read.
      format("magellan").
      option("type", "geojson").
      load(path)
    df.show()

    import org.apache.spark.sql.magellan.dsl.expressions._
    import session.implicits._


    val points = session.sparkContext
      .cassandraTable[(String, Float, Float)](Conf._schema, Conf._table_name_antenna)
        .select("id", "cx","cy").toDF("id", "x", "y").select($"id", point($"x", $"y") as "punto")


    val join = points.join(df).where($"punto" within $"polygon")

    join.select($"metadata".getField("name"), $"id").rdd.
      map(x => {new Antenna(x.getString(1),0, 0f, 0f, -1, x.getString(0))})
      .saveToCassandra(Conf._schema, Conf._table_name_antenna, SomeColumns("id", "city") )

    session.close()

  }

}
