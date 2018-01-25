package org.tfm.app

import java.util.Calendar

import com.datastax.spark.connector._
import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.tfm.structs.{Antenna, AntennaUser, Client}
import org.tfm.dataupload.Conf

case class antennauserid (iduser: String, idantenna: String){
}

object EventsKMeansModel {

  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", Conf._cassandra_host)
    .set("spark.cassandra.connection.port", Conf._cassandra_port)

  val session = SparkSession.builder()
    .appName(Conf._app_name_kmeans)
    .master("local")
    .config(conf)
    .getOrCreate()

  /**
    * Este método devuelve la posición (de 0 a 167) dependiendo del día de la semana y hora que se pasa
    * como argumento.
    * Domingo - 0 h -> 0
    * Domingo - 1 h -> 1
    * Sábado - 23 h -> 167
    * @param data fecha con formato  DD/MM/YYYY-HH:MM:SS.mmm.
    * @return
    */
  def stringToDataPosition(data:String) : Int = {
    try{

      // fecha viene con el formato DD/MM/YYYY
      val fecha = data.substring(0, data.indexOf('-'))
      val hora = data.substring(data.indexOf('-')+1, data.indexOf(':'))


      import java.text.SimpleDateFormat
      val inputDate = new SimpleDateFormat("dd/MM/yyyy").parse(fecha)
      val calendar = Calendar.getInstance
      calendar.setTime(inputDate)

      val pos = calendar.get(Calendar.DAY_OF_WEEK)
      val res = ((pos-1) * 24) + hora.toInt

      res

    } catch {
      case e: Exception => {
        //println("ERROR: " + data )
        -1
      }
    }
  }

  /**
    * Transformación de una lista de tuplas (entero, long) a una lista de tuplas (entero, double)
    * @param list
    * @return
    */
  def parseVector(list: List[(Int, Long)]) : List[(Int, Double)] = {

    if (list.length == 1){
      val x = list.apply(0)
      List((x._1, x._2.toDouble))
    }
    else{
      val x = list.apply(0)
      (x._1, x._2.toDouble) :: parseVector(list.tail)
    }

  }

  /**
    * createDataSet genera un vector de ocurrenicas de eventos por antena
    * @param trainning entero que indica si el dataset que vamos a crear es el de entrenamiento o de predicción
    * @return
    */
  def crearDataSet(trainning: Int) : DataFrame = {

    var  path = ""
    if (trainning == 0)  path = Conf._hdfs_path_event_train
        else path = Conf._hdfs_path_event_predict

    val df = session.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .load(path)


    val data = df.select(df("ClientId"), df("Date"), df("AntennaId")).rdd
      .map(clientDateAntennaRow => {
        ((clientDateAntennaRow(0).toString,(clientDateAntennaRow(2).toString.trim)), stringToDataPosition(clientDateAntennaRow(1).toString))
      //}).filter(case (_, date) => {date != -1})
      }).filter(x=> x._2 != -1)
      .aggregateByKey(Map[Int, Double]())((acc, curr) => {
        acc ++ Map(curr -> 1.0)
      }, (l, r) => {
        l ++ r
      }).mapValues(activityInMap => Vectors.sparse(168, activityInMap.toSeq))


    val parsedData = data

    // convertir un array de vector a un seq de (0, vector)
    val dataset = session.createDataFrame(parsedData).toDF("id","features")
    dataset
  }


  /**
    * entrenar el modelo
    * - leemos lo fichero de eventos
    * - agrupamos por antena
    * - aprendemos el modelo KMeans con k=2
    * - guardamos el model
    * TODO: faltaría -> una vez que tenemos el modelo entrenado, comparalo con el modelo que nos han dado los expertos
    *                   para asignar casa/trabajo a los valores 0 y 1 según corresponda
    */
  def entrenarModelo(): Unit = {

    val dataset = crearDataSet(0)
    //dataset.show(false)

    val kmeans = new KMeans().setK(2).setMaxIter(20)
    val model = kmeans.fit(dataset)

    println(s"Centroids: \n${model.clusterCenters.mkString("\n")}")



    val WSSSE = model.computeCost(dataset)
    println(s"Within Set Sum of Squared Errors= ${WSSSE}")
    println(s"The size of each cluster is {${model.summary.clusterSizes.mkString(",")}}")

   model.summary.predictions.show(100)

    model.save(Conf._hdfs_path_model)

  } // entrenar modelo

  def getDNI (data: String) : String = {
    val dni = data.replaceAll("\\[", "").replace("]", "").split(",")
    dni.apply(0)
  }

  def getAntenna (data: String) : String = {
    val antenna = data.replaceAll("\\[", "").replace("]", "").split(",")
    antenna.apply(1)
  }

  def predecirModelo () : Unit = {

    val sameModel = KMeansModel.load(Conf._hdfs_path_model)

    // convertir un array de vector a un seq de (0, vector)
    val dataset = crearDataSet(1)

    import session.implicits._

    val clients = session.sparkContext.cassandraTable[Client](Conf._schema, Conf._table_name_client)
        .select("clientid", "gender", "age", "civilstatus", "nationality", "socioeconlev")
      .toDS()//.show(10)


    val transformed = sameModel.transform(dataset)

    val eventos = transformed.map(x => {
      val dni = getDNI(x.get(0).toString)
      (getDNI(x.get(0).toString),getAntenna(x.get(0).toString), x.get(2).toString.toInt, 1L)
    }).toDF("clientid", "antennaid", "categoria", "count")

    val eventos_clientes = eventos.join(clients, eventos("clientid")===clients("clientid"), "inner")
    val tablaCassandra = eventos_clientes.map(event => {
      new AntennaUser(event.getString(0), event.getString(6), event.getInt(5), event.getString(8), event.getString(7),
        event.getString(9), event.getString(1), event.getInt(2))
    })

    tablaCassandra.toDF().write.format("org.apache.spark.sql.cassandra")
      .options(Map( "keyspace" -> Conf._schema,
      "table" -> Conf._table_name_antennabyuser ))
      .mode("append")
          .save()

    // para cada uno de los eventos, actualizo el contador
    eventos.select("antennaid" , "categoria", "count")
      .write.format("org.apache.spark.sql.cassandra")
      .options(Map( "keyspace" -> Conf._schema,
        "table" -> Conf._table_name_antennacounter))
      .mode("append")
      .save()

  } // predecirModelo


  def lista(str: String) : Unit = {
    val p = str.split(',').map(_.toFloat)
    println(p)
    p.foreach(println(_))
  }


  /**
    * Una vez que tenemos clasificadas las antenas por usuario, debemos
    * hacer una clasificacion "general" para cada antena,
    * para ello cruzamos la tabla de contadores con las antenas y actualizamos la localizacion (clasificación)
    */
  def definirTipoAntena(): Unit = {

    import session.implicits._

    //val antennas = session.sparkContext.cassandraTable[String](Conf._schema, Conf._table_name_antenna)
     // .select("id").toDF("id")//.show(10)

    val counter = session.sparkContext
      .cassandraTable[(String, Int, Long)](Conf._schema, Conf._table_name_antennacounter)
      .select("antennaid", "categoria", "count").toDF("antennaid", "categoria", "count")//.show(10)

    val clasificacion = counter.rdd //antennas.join(counter, antennas("id") === counter("antennaid"), "inner").rdd
        .map(x => {
          (x.getString(0), (x.getInt(1), x.getLong(2)))
        }).aggregateByKey((0,0L))(
      (acc,curr)=>{
        if (acc._2 > curr._2)
          (acc)
        else
          curr
      },(l,r)=>{
        if (l._2 > r._2)
          (l)
        else
          r
      }).map(x => {new Antenna(x._1,0, 0f, 0f ,x._2._1)})
      .saveToCassandra(Conf._schema, Conf._table_name_antenna, SomeColumns("id", "localiz") )

  }


  // 7 * ( numero de dia de la semana -1) + hora
  def main(args: Array[String]): Unit = {

    //entrenarModelo()
    //predecirModelo()
    //definirTipoAntena()

    session.close()

  }

}



