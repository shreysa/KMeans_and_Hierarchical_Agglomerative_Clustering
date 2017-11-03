


class Edges(aid1: String, aid2: String, cmn: Int) extends java.io.Serializable {
  val Art1 = aid1
  val Art2 = aid2
  val cmnlty = cmn

  /*def getArts(): scala.collection.immutable.Set ={
    Set(Art1, Art2)
  }*/

  def getArt1(): String ={
    return Art1
  }

  def getArt2(): String ={
    return Art2
  }

  def getComm(): Int ={
    return cmnlty
  }

  def existsFirst(a: String): Boolean ={
    if (Art1.equals(a))
      return true
    false
  }

  def exists(a: String): Boolean ={
    if (Art1.equals(a) || Art2.equals(a))
      return true
    false
  }

  def compare(c: Edges): Boolean ={
    if (cmnlty > c.getComm())
      return true

    false
  }
}