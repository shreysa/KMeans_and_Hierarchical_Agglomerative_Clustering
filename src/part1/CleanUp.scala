/**
  * CS6240 - Fall 2017
  * Assignment A6
  * Spark and Scala
  *
  * @author Shreysa Sharma
  */

import org.apache.log4j.{LogManager, Level, PropertyConfigurator}
import org.apache.spark.rdd

class CleanUp(row : String) extends java.io.Serializable {

  val line : Array[String] = row.split(";")
  var trackId = ""
  var duration = 0.00
  var loudness = 0.00
  var tempo = 0.00
  var artHot = 0.00
  var songHot = 0.00
  var isValidRow = true;

  // Local logger
  //val log = LogManager.getLogger("CleanUp")
  //log.setLevel(Level.INFO)

  // Converting values to respective data types and eliminating invalid record
  try {
    duration = line(5).toDouble
    loudness = line(6).toDouble
    tempo = line(7).toDouble
    artHot = line(20).toDouble
    songHot = line(25).toDouble
  } catch {
    case e: Exception => isValidRow = false
  }
  def getDuration() : Double = duration
  def getLoudness() : Double = loudness
  def getTempo()    : Double = tempo
  def getArtHot()   : Double = artHot
  def getSongHot()  : Double = songHot
  def getSongId()   : String = line(23)

  def checkValidity() : Boolean = {
    var result = false
    if(isValidRow && getDuration() > 0.00
      && getLoudness() < 0.00
      && getTempo() > 0.00
      && getArtHot() > 0
      && getSongHot() > 0 && getSongHot() <= 1
      && !(getSongId().isEmpty) ) {
      result = true
    }
    result
  }
}
