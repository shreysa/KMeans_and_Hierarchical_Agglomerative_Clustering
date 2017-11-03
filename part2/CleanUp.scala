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
  var artistId = ""
  var artFam = 0.00
  var artistName = ""
  var isValidRow = true;

  var simArt = new ArrayBuffer[CleanUp]

  // Local logger
  //val log = LogManager.getLogger("CleanUp")
  //log.setLevel(Level.INFO)

  // Converting values to respective data types and eliminating invalid record
  try {
    artFam = line(19).toDouble
    artHot = line(20).toDouble
  } catch {
    case e: Exception => isValidRow = false
  }
  def getArtId()    : String = line(16)
  def getArtistName() : String = line(17)
  def getArtFam()   : Double = artFam

  def checkValidity() : Boolean = {
    var result = false
    if(isValidRow
      && getArtFam() > 0
      && !(getArtistName.isEmpty)
      && !(getArtId().isEmpty) {
      result = true
    }
    result
  }
}
