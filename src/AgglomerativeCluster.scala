/**
  * CS6240 - Fall 2017
  * Assignment A7
  * Spark and Scala test program
  *
  * @author Shreysa Sharma and Jashangeet Singh
  */

import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext, rdd}
import scala.collection.mutable.ListBuffer

object AgglomerativeCluster {
  // Set-up logging
  // Logging code derived from
  // https://syshell.net/2015/02/03/spark-configure-and-use-log4j/
  PropertyConfigurator.configure("./src/log4j.properties")
  val rootLog = LogManager.getRootLogger()
  rootLog.setLevel(Level.OFF)

  // Class specific logging
  val log = LogManager.getLogger("AgglomerativeCluster")
  log.setLevel(Level.INFO)

  // Spark Configuration
  val conf = new SparkConf().setAppName("AgglomerativeCluster").setMaster("local")
  val sc = new SparkContext(conf)

  /**
    * Distance function used in as the clustering parameter
    * using a simple euclidian distance for the purpose
    * @param pt1 Point 1 (in this case a 1D value)
    * @param pt2 Point 2 (in this case a 1D value)
    * @return Distance as a double
    */
  def euclideanDistance(pt1: Double, pt2: Double): Double = {
    var square = (pt1 - pt2) * (pt1 - pt2)
    math.sqrt(square)
  }

  /**
    * Function to filter pairs, for computing the pairwise distances
    * this method eliminates
    *  - ~duplicate pairs - such as (1,2) == (2,1)
    *  - x == y, ie. (1,1) pair is not required
    * @param element
    * @return
    */
  def filterEntry(element1: (Long, Double), element2: (Long, Double)): Boolean = {
    ((element1._1 + 1 > element2._1) && (element1._1 != element2._1))
  }

  /**
    * Compute the pairwise distances of each pair that can be formed from the input dataset
    * Ex: (1,2,3) -> (1,2), (1,3), (2,3). NOTE: (1,2) == (2,1) - therefore (2,1) not computed
    * @param dataset Input dataset RDD of (ID, ParameterValue)
    * @return RDD of (distance, (ID_1, ID_2))
    */
  def pairwiseDistances(dataset: rdd.RDD[(Long, Double)]): rdd.RDD[(Double, (List[Long], List[Long]))] = {
    val cartesian = dataset.cartesian(dataset).filter(elem => filterEntry(elem._1,elem._2))
    var distance = cartesian.map { lines => (euclideanDistance(lines._1._2, lines._2._2), (List(lines._1._1), List(lines._2._1))) }

    println(distance)

    // Sort by distance
    distance.sortBy(_._1)
  }

  def buildClusters(input: rdd.RDD[(Double, (List[Long], List[Long]))], numClusters: Int): Unit = {
    var clusters = input
    var oldClusters = ListBuffer[List[Int]]()

    while (clusters.count > numClusters) {
      // Get the top most element
      val first = clusters.take(1)

      // Remove from  cluster
      val firstRdd = sc.makeRDD(first)
      clusters.subtract(firstRdd)

      var pairData = first.toList(0)._2

      println(pairData._1)
      println(pairData._1)

      // Skip already considered nodes
      var skip = false
      oldClusters.foreach(oldCluster => {
        var contains1 = pairData._1.contains(elem)
        var contains2 = pairData._2.contains(elem)
        skip = contains1 || contains2
      })

      // Add merged clusters to oldClusters
      oldClusters += pairData._1
      oldClusters += pairData._2

      var newClusterAdditions = ListBuffer()
      clusters.foreach(elem => {

      })

    }
  }

  /**
    * Entry point for the program
    * @param args Input Arguments to the program
    */
  def main(args: Array[String]) {
    val inputFileName = args(0)
    val inputArtFileName = args(1)


    log.info("Starting SongAnalysis with input file: " + inputFileName)

    // Start Processing
    val songFile = sc.textFile(inputFileName, 2).sample(false, 0.005, 314159)
    val artistFile = sc.textFile(inputArtFileName, 2)
    val headerAndRows = artistFile.map(line => line.split(";").map(_.trim))
    val header = headerAndRows.first
    val artistRecords = headerAndRows.filter(_ (0) != header(0))

    // Clean-up
    var records = songFile.map(row => new CleanUp(row)).filter(_.checkValidity())
    records.cache()

    val numEntries = records.collect().length
    log.info(s"Number of entries in dataset: ${numEntries}")

    // Get Loudness
    var loudnessValues = records.zipWithIndex().map(entry => (entry._2, entry._1.getLoudness()))
    var sortedDistances = pairwiseDistances(loudnessValues)
    println(sortedDistances.collect.toList)
    sortedDistances.saveAsTextFile("./output")
    log.info(s"Number of entries in sorted list = ${sortedDistances.count}")

    buildClusters(sortedDistances, 3)
  }

  // def compute_centroid(dataset : Array[Double], data_points_index : Array[Double]): Double = {
  //     var size = data_points_index.length
  //     var centroid = 0.00
  //     data_points_index.foreach( idx => {
  //         var dim_data = dataset[idx]
  //         centroid += dim_data.toDouble
  //     centroid = centroid/size
  //     })
  //    centroid
  // }

}
