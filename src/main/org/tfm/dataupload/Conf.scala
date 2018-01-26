package org.tfm.dataupload

object Conf {

  // --- Configuración cassandra
  val _cassandra_host = "127.0.0.1"
  val _cassandra_port = "9045"
  val _cassandra_format = "org.apache.spark.sql.cassandra"

  val _schema = "tfm"
  val _table_name_client = "client"
  val _table_name_city = "city"
  val _table_name_antenna = "antenna"
  val _tabla_name_event = "event"
  val _tabla_name_eventadv = "eventadv"
  val _table_name_eventbydate = "eventocurrbydate"
  val _table_name_antennabyuser = "antennabyuser"
  val _table_name_antennacounter = "antennacounter"

  val _app_name_client = "ClientsStore"
  val _app_name_city = "CityStore"
  val _app_name_antenna = "AntenaStore"
  val _app_name_kmeans = "KMeansApp"

  val _master = "local[*]"

  // configuracion hdfs
  val _hdfs_path_client = "hdfs://localhost:9000/pfm/data/clients/data/clients*.csv"
  val _hdfs_path_city = "hdfs://localhost:9000/pfm/data/cities/cities.csv"
  val _hdfs_path_antenna = "hdfs://localhost:9000/pfm/data/antennas/Antennas.csv"
  val _hdfs_path_event_predict = "hdfs://localhost:9000/pfm/data/events/predict/*Events*.csv"
  val _hdfs_path_event_train = "hdfs://localhost:9000/pfm/data/events/training/*Events*.csv"
  val _hdfs_path_model = "hdfs://localhost:9000/pfm/data/models/model"

  // fichero geo
  val _geo_json = "./data/cities.json"


}
