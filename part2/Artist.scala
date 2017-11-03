

import org.apache.log4j.{LogManager, Level, PropertyConfigurator}
import org.apache.spark.rdd

class Artist(aid: String, cid: String) extends java.io.Serializable {

  val Art_id = aid
  var Clust_id = cid

  def getArtId(): String ={
    return Art_id
  }

  def getClustId(): String ={
    return Clust_id
  }

  def update(s: String): Unit ={
    Clust_id = s;
  }

  def update(a: Array[(String, (String, Int))]): Unit ={
    Clust_id = a(0)._2._1
  }
}