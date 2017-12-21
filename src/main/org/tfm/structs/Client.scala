package org.tfm.structs

case class Client (clientid: String,
                   age: Int,
                      gender: String, // F: Female, M: Male
                      nationality: String,
                      civilstatus: String, // Married, Single
                      socioeconlev: String) // High, Medium, Low
{}
