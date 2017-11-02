import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd
import java.util.Calendar
import java.lang.Math

import scala.collection.mutable.ArrayBuffer

object ClusterMain {
  val log = LogManager.getLogger("Clusterer")
  log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {
    // Set-up logging
    // Logging code derived from
    // https://syshell.net/2015/02/03/spark-configure-and-use-log4j/
    PropertyConfigurator.configure("./src/log4j.properties")
    val rootLog = LogManager.getRootLogger()
    rootLog.setLevel(Level.OFF)

    // Local logger
    //log.info("Starting SongAnalysis with input file: " + inputFileName)

    // Spark Configuration
    val conf = new SparkConf().setAppName("Clustering").setMaster("local")
    val sc = new SparkContext(conf)

    // Start Processing
    val songFile = sc.textFile("/Users/jashanrandhawa/Downloads/spark-2.2.0-bin-hadoop2.7/a6/a61/all/song_info.csv", 2)
    //val artistFile = sc.textFile(inputArtFileName, 2)
    //val headerAndRows = songFile.map(line => line.split(";").map(_.trim))
    //val header = headerAndRows.first
    //val Records = headerAndRows.filter(_(0) != header(0))
    log.info(songFile.collect)
    var records = songFile.map(row => new CleanUp(row)).filter(_.checkValidity())

    // Clean-up
    //records = records.filter(_.checkValidity())
    //log.info(records.count)
    //records.map(_.getLoudness())
    ClusterLoudness(records, sc)
  }

  def ClusterLoudness(records: rdd.RDD[CleanUp], sc: SparkContext): Unit ={
    var loudness = records.map(line => {
      if(line.getLoudness().toDouble > -7)  (1, (line.getLoudness(), -1.0));
      else if(line.getLoudness().toDouble > -12) (2, (line.getLoudness(), -1.0));
      else (3, (line.getLoudness(), -1.0))})

    var iterations = 0
    var oldC = new ArrayBuffer[Centroid]
    var centroids = new ArrayBuffer[Centroid]
    centroids += new Centroid()
    centroids += new Centroid()
    centroids += new Centroid()
    getCentroids(loudness, centroids, 3)
    //centroids parallelize
    while (!shouldStop(iterations, centroids, oldC)) {
      var i = 0
      for(centroid <- centroids) {
        if(oldC.size < centroids.size)
          oldC += new Centroid
        oldC(i).copies(centroid)
        i+=1
      }
      iterations+=1
      //centroid parallelize
      sc.parallelize(centroids)
      sc.parallelize(oldC)
      loudness = setLabels(loudness, centroids)
      getCentroids(loudness, centroids, 3)
      //centroids parallelize
    }
    println(centroids + " " + iterations)
  }



  def getCentroids(loudness: rdd.RDD[(Int, (Double, Double))], centroids: ArrayBuffer[Centroid], k: Int): Unit ={
    var i = 1
    for (centroid <- centroids) {
      if (centroid.getValue() == -1.0) { //get random centroid somehow... if there if one of the centroids has initial value.
        //val r = new scala.util.Random(loudness.count)
        //val newL = loudness.collect
        //val newC = newL.toList(r)
        //val newK =
        centroid.putKey(i)
        if (i == 1)
          centroid.putValue(1)
        else if(i == 2)
          centroid.putValue(2)
        else
          centroid.putValue(3)
        centroid.hasData()
        i+=1
      }
    }
    if (true) {
        //loudness.reduceByKey()
        val L = loudness.mapValues(x => (x._1, x._2, 1))
          .reduceByKey((x, y) => (x._1 + y._1, -1, x._3+y._3))
          .mapValues(x => (x._1 / x._3))
          .collect
          .toSeq
          .toMap
        for (centroid <- centroids){
          centroid.putValue(L.getOrElse(centroid.getKey(), -1.0))
        }
        //loudness.reduceByKey((x, y) => ((x._1.toDouble + y._1.toDouble).toString, x._2+y._2))
      }
  }

  def setLabels(loudness: rdd.RDD[(Int, (Double, Double))], centroids: ArrayBuffer[Centroid]): rdd.RDD[(Int, (Double, Double))] ={
    //update the first value in loudness according to centroid distances.
    loudness.map(l =>
      ({ if(Math.abs(l._2._1-centroids(0).getValue()) < Math.min(Math.abs(l._2._1-centroids(1).getValue()), Math.abs(l._2._1-centroids(2).getValue()))) 1;
      else if(Math.abs(l._2._1-centroids(1).getValue()) < Math.abs(l._2._1-centroids(2).getValue())) 2;
      else 3 },
        l._2))
      .map(l =>
        (l._1, (l._2._1, Math.abs(l._2._1-centroids(l._1-1).getValue()))))
  }

  def shouldStop(iterations: Int,centroids: ArrayBuffer[Centroid], oldC: ArrayBuffer[Centroid] ): Boolean ={
    if (iterations > 10)
      return true
    var i = 0
    if(oldC.size != 0)
      for (centroid <- centroids) {
        if (centroid != oldC(i))
          return false
        i+=1
      }
    else
      return false
    true
  }

  def getDistance(): Unit ={

  }






  /*def getByCName(mC: ArrayBuffer, i: Int): (Int, Double) ={

  }*/
}