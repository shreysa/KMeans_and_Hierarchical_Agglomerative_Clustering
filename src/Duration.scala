/**
 * CS6240 - Fall 2017
 * Assignment A7
 * Spark and Scala
 *
 * @author Shreysa Sharma
 */

import org.apache.log4j.{LogManager, Level, PropertyConfigurator}
import org.apache.spark.rdd

class Duration(row : String) extends java.io.Serializable {
    
    val line : Array[String] = row.split(";")
    var duration = 0.00
    var isValidRow = true;

     try {
      duration = line(5).toDouble
      } catch {
      case e: Exception => isValidRow = false
    }

    def getDuration() : Double = duration

    def checkValidity() : Boolean = {
    var result = false
    if(isValidRow  && getDuration() > 0.00)
      result = true
    result
  }
}