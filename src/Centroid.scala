/**
 * CS6240 - Fall 2017
 * Assignment A7
 * Spark and Scala
 *
 * @author Jashangeet Singh
 */

class Centroid() extends java.io.Serializable {
  var i = 0
  var b = -1.0
  var data = 0
  var rand = scala.util.Random

  def getValue() = b
  def getKey() = i
  def getData() = data
  def hasNoData(): Boolean = { if (data == 0) return true; false }
  def hasData(): Unit = { data = 1 }
  def hasDataNoLonger: Unit = { data = 0 }

  def putValue(): Unit ={
    b = rand.nextDouble
  }

  def putValue(n: Double): Unit ={
    b = n
  }

  def putKey(m: Int): Unit ={
    i = m
  }

  def !=(oldC: Centroid): Boolean ={
    var bool = false
    if (getValue() - oldC.getValue() < 10)
      bool =  true
    bool
  }

  def copies(c: Centroid): Unit ={
    b = c.getValue()
    i = c.getKey()
    data = getData()
  }

}