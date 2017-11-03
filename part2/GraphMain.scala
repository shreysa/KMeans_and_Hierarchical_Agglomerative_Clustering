
/**
  * @author jashangeetsingh
  */


import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd
import java.util.Calendar
import java.lang.Math

import scala.collection.mutable.Queue


object GraphMain {
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
    val termFile = sc.textFile("/Users/jashanrandhawa/Downloads/spark-2.2.0-bin-hadoop2.7/a6/a61/all/artist_terms.csv", 2)
    val simFile = sc.textFile("/Users/jashanrandhawa/Downloads/spark-2.2.0-bin-hadoop2.7/a6/a61/all/similar_artists.csv", 2)
    val headerAndRows = termFile.map(line => line.split(";").map(_.trim))
    val header = headerAndRows.first
    val termRecords = headerAndRows.filter(_ (0) != header(0)).map(l => (l(0), l(1)))
    //var artRecords = songFile.map(row => (row(16), new Artist(row))).filter(_.checkValidity())

    val headerAndRows = songFile.map(line => line.split(";").map(_.trim))
    val header = headerAndRows.first
    val artRecords = headerAndRows.filter(_ (0) != header(0)).map(row => (row(16), row(19)))
    //val artist_fam_num = artRecords.map(l => (l._1, l._2.putNumSongs(1))).reduceByKey((l, m) => (l._1, l._2+m._2))
    val headerAndRows = simFile.map(line => line.split(";").map(_.trim))
    val header = headerAndRows.first
    var simRecords = headerAndRows.filter(_ (0) != header(0)).map(l => (l(0), l(1)))

    var trendsetters = getTrend(artRecords, simRecords).map(_._1).take(30)
    sc.parallelize(trendsetters)
    //como(artRecords.map(x => (x._1, x._1)).distinct, termRecords)

    //val sR = simRecords.distinct.map(x => (x, new Edges(x)))
    val graph = getGraph(termRecords, simRecords).persist

    val clusters = artRecords.map(l=>(l._1, "")).distinct.map(l => new Artist(l._1, l._2)).collect
    sc.parallelize(clusters)

    Clustering(trendsetters, graph, clusters, sc)

  }

  /**
    *
    * @param centroids
    * @param graph
    * @param clusters
    */

  def Clustering(centroids: Array[String], graph: rdd.RDD[(Edges)], clusters: Array[(String, String)], sc: sparkContext): Unit ={
    var iterations = 0
    val oldTr = new rdd.RDD[String]

    while (!shouldStop(iterations, centroids, oldC)) {
      iterations+=1
      setLabels(clusters, centroids, graph)
      getCentroids(graph, centroids, clusters)
    }
  }

  /**
    *
    * @param graph
    * @param centroids
    * @param clusters
    */

  def getCentroids(graph: rdd.RDD[(Edges)], centroids: Array[String], clusters: Array[(String, String)]): Unit ={

  }

  /**
    * @param clusters Contains the artist_ids with the centroid they are clustered around
    * @param centroids Contains Current centroids
    * @param graph Represents the graph
    */

  def setLabels(clusters: Array[Artist], centroids: Array[String], graph: rdd.RDD[(Edges)]): Unit ={
    for(point <- clusters){
      try {
        val initClustId = point.getClustId()
        for (centroid <- centroids) {
          if (point.equals(centroid)) {
            point.update(centroid)
            throw new Exception                           //break the inner loop
          }
          bfsCentroid(graph, point, centroids.toList)

        }
      } catch {case e: Exception => println("No worry") }
      if(point.getClustId().equals(initClustId)) {

      }
    }
    sc.parallelize(clusters)
  }

  /**
    * Clunky method to find perform BFS for the "nearest" centroid
    * @param graph RDD of Edges
    * @param point A vertex which we are trying to find a centroid for
    * @param centroids Contains the current centroid values
    */
  def bfsCentroid(graph: rdd.RDD[(Edges)], point: Artist, centroids: List[String]): Unit ={
    var Q = Queue[list[Artist, String]]
    var p = List[point, "P"]
    Q += p
    p(1) = "Q"
    while (!Q.isEmpty()){
      var t = Q.dequeue
      if (centroids.exists(t(0).getartId()))
        return t(0)
      for (n <- graph.filter(e => e.existsFirst(t(0).getArtId())).collect) {
        
      }
    }
  }

  def getAverage()

  def shouldStop()

  /**
    * @param aR RDD contains the Artist to familiarity pairs
    * @param sR RDD contains the Similar_Artist pairs
    * @return An RDD with Artists sorted by popularity
    */

  def getTrend(aR: rdd.RDD[(String, String)], sR: rdd.RDD[(String, String)]): rdd.RDD[(String, Double)] ={
    val artist_fam_num = aR.filter(l => l._2 != "NA").map(l => (l._1, (l._2, 1))).reduceByKey((l, m) => (l._1, l._2 + m._2))
    val art_sim_num = sR.filter(l => !l._1.equals(l._2)).map(l => (l._1, 1)).reduceByKey((x, y) => x + y)
    val pop_artist = artist_fam_num.join(art_sim_num).mapValues(l => l._1._1.toDouble * l._1._2 * l._2)
    pop_artist.sortBy(_._2, false)
  }

  /**
    * @param tR RDD contains the Artist to terms info
    * @param sR RDD contains the Similar_Artist pairs
    * @return An RDD of Edges representing the graph
    */

  def getGraph(tR: rdd.RDD[(String, String)], sR: rdd.RDD[(String, String)]): rdd.RDD[(Edges)] = {
    val tRUnique = tR.distinct.reduceByKey((x, y) => x+":"+y).mapValues(x => x.split(":").toList)
    val sRUnique = sR.distinct
    val joined = sRUnique.join(tRUnique)
    val joined2 = joined.map(x => (x._2._1, (x._1, x._2._2))).join(tRUnique)
    joined2.map(x => new Edges(x._1, x._2._1._1, x._2._1._2.toSet.intersect(x._2._2.toSet).size))
  }
}