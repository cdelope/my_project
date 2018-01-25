package org.tfm.structs

case class Event (fecha: String, hora: Int, antennaid: String, userid: String){}

case class EventAdv (fecha: String, hora: Int, antennaid: String, userid: String,
                     gender: String, civilstatus: String, socioeconlev:String,
                     nationality: String, age: Int){}

//case class EventContByDate (fecha: String, hora: Int,  antennaid: String, ocurr: Int){}
