



class Centroid() extends java.io.Serializable {
  var i = 0
  var b = -1.0
  var data = 0

  def getValue() = b
  def getKey() = i
  def getData() = data
  def hasNoData(): Boolean = { if (data == 0) return true; false }
  def hasData(): Unit = { data = 1 }
  def hasDataNoLonger: Unit = { data = 0 }

  def putValue(n: Double): Unit ={
    b = n
  }

  def putKey(m: Int): Unit ={
    i = m
  }

  def ==(oldC: Centroid): Boolean ={
    if (getValue() == oldC.getValue())
      true
    false
  }

  def copies(c: Centroid): Unit ={
    b = c.getValue()
    i = c.getKey()
    data = getData()
  }

}