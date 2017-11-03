/**
 * CS6240 - Fall 2017
 * Assignment A7
 * Spark and Scala
 *
 * @author Shreysa Sharma
 */

import org.apache.log4j.{LogManager, Level, PropertyConfigurator}
import org.apache.spark.rdd

class Hotness(row : String) extends java.io.Serializable {
    
    val line : Array[String] = row.split(";")
    var artHotness = 0.00
    var songHotness = 0.00
    var isValidRow = true;

     try {
      songHotness = line(25).toDouble
      artHotness = line(20).toDouble
      } catch {
      case e: Exception => isValidRow = false
    }

    def getArtHotness() : Double = artHotness
    def getSongHotness() : Double = songHotness
    def getCombHotness() : (Double, Double) = (artHotness, songHotness)

    def checkValidity() : Boolean = {
    var result = false
    if(isValidRow  && (getSongHotness() > 0.00 || getSongHotness() <= 1))
      result = true
    result
  }

  def checkCombValidity() : Boolean = {
    var result = false
    if(isValidRow  && getSongHotness() > 0.00 && getSongHotness() <= 1 && getArtHotness() > 0.00 && getArtHotness <= 1 )
      result = true
    result
  }
}