/**
 * CS6240 - Fall 2017
 * Assignment A7
 * Spark and Scala
 *
 * @author Shreysa Sharma
 */

import org.apache.log4j.{LogManager, Level, PropertyConfigurator}
import org.apache.spark.rdd

class Tempo(row : String) extends java.io.Serializable {
    
    val line : Array[String] = row.split(";")
    var tempo = 0.00
    var isValidRow = true;

     try {
      tempo = line(7).toDouble
      } catch {
      case e: Exception => isValidRow = false
    }

    def getTempo()    : Double = tempo

    def checkValidity() : Boolean = {
    var result = false
    if(isValidRow && getTempo() > 0.00)
      result = true
    result
  }
}