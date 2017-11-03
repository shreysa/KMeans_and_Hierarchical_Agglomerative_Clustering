/**
 * CS6240 - Fall 2017
 * Assignment A7
 * Spark and Scala
 *
 * @author Shreysa Sharma
 */

import org.apache.log4j.{LogManager, Level, PropertyConfigurator}
import org.apache.spark.rdd

class Loudness(row : String) extends java.io.Serializable {
    
    val line : Array[String] = row.split(";")
    var loudness = 0.00
    var isValidRow = true;

     try {
      loudness = line(6).toDouble
      } catch {
      case e: Exception => isValidRow = false
    }

    def getLoudness() : Double = loudness

    def checkValidity() : Boolean = {
    var result = false
    if(isValidRow  && getLoudness() < 0.00)
      result = true
    result
  }
}