package org.tfm.app

import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.tfm.dataupload.AntennaStore
import org.tfm.structs.Antenna



object EventsKMeansModel {

  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.port", "9045")

  val session = SparkSession.builder()
    .appName("Test spark")
    .master("local")
    .config(conf)
    .getOrCreate()


  //val normalizer1 = new Normalizer()


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

      //println("Fecha: " + data + " pos: " + pos + " hora: " + hora.toInt.toString + " res: " +res)

      res

    } catch {
      case e: Exception => {
        println("ERROR: " + data )
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
    if (trainning == 0)  path = "hdfs://localhost:9000////pfm/events/trainning/*elephoneEvents*.csv"
        else path = "hdfs://localhost:9000////pfm/events/predict/*elephoneEvents*.csv"

    val df = session.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .load(path)

    val data = df.select(df("ClientId"), df("Date"), df("AntennaId")).rdd
      .map(t => {
        (stringToDataPosition(t(1).toString),(t(2).toString.trim))
      }).filter(x=> {x._1 != -1})
      .map(x => {(x,1L)})
      .reduceByKey((x,y) => {(1L)})//_+_)
      .map(x => {(x._1._2, (x._1._1, x._2))})
      .aggregateByKey(List[(Int, Long)]())((acc, curr) => {
        (curr :: acc).sortBy(_._1)
      }, (l, r) => {
        (l ::: r).sortBy(_._1)
      })

    val parsedData =  data.map(x=> {
      (x._1, Vectors.sparse(168, parseVector(x._2)))
    }).collect().toSeq

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
    dataset.show(false)

    val kmeans = new KMeans().setK(2).setMaxIter(5)
    val model = kmeans.fit(dataset)

    println(s"Centroids: \n${model.clusterCenters.mkString("\n")}")



    val WSSSE = model.computeCost(dataset)
    println(s"Within Set Sum of Squared Errors= ${WSSSE}")
    println(s"The size of each cluster is {${model.summary.clusterSizes.mkString(",")}}")

    model.summary.predictions.show()
    println(model.getPredictionCol(1))


    //model.save("hdfs://localhost:9000////pfm/models/model2")

  } // entrenar modelo

  def predecirModelo () : Unit = {

    val sameModel = KMeansModel.load("hdfs://localhost:9000////pfm/models/model2")

    // convertir un array de vector a un seq de (0, vector)
    val dataset = crearDataSet(1)

    import session.implicits._

    val transformed = sameModel.transform(dataset)
    transformed.map(x =>{
      new Antenna(x.get(0).toString, 0, 0.toFloat, 0.toFloat, x.get(2).toString.toInt)})
      .collect().foreach(AntennaStore.updateLocaliz(_))

  } // predecirModelo

  /**
    * compararModelo hace una comparación entre el modelo que hemos entrenado y el suministrado por los expertos
    * (distancias)
    */
  def compararModelo() : Unit = {

    val sameModel = KMeansModel.load("hdfs://localhost:9000////pfm/models/model2")

    println(s"Centroids: \n${sameModel.clusterCenters.mkString("\n")}")

    println("Cluster 0 " + sameModel.clusterCenters.apply(0))
    println("Cluster 1 " + sameModel.clusterCenters.apply(1))


    val df = session.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .load("./src/main/org/tfm/data/expertModel.csv")

    val data = df.select(df("Label"), df("Data")).rdd
      .map(x=> (x.getString(0), x.getString(1).split(',').map(_.toFloat))).foreach(println(_))
      //.foreach(x=> lista(x.getString(1)))

  } // comprararModelo

  def lista(str: String) : Unit = {
    val p = str.split(',').map(_.toFloat)
    println(p)
    p.foreach(println(_))
  }


  // 7 * ( numero de dia de la semana -1) + hora
  def main(args: Array[String]): Unit = {

    //val df = crearDataSet(0)
    //df.show(false)
    //entrenarModelo()
    //compararModelo()
    predecirModelo()

    session.close()

  }

}



