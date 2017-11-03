/**
 * CS6240 - Fall 2017
 * Assignment A7
 * Spark and Scala
 *
 * @author Jashangeet Singh
 */

import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd
import java.lang.Math
import scala.collection.mutable.ListBuffer

import scala.collection.mutable.ArrayBuffer

object ClusterMain {
  val log = LogManager.getLogger("Cluster Main")
  log.setLevel(Level.INFO)
  var rand = scala.util.Random 
  val startTime: Long = System.currentTimeMillis / 1000

  def main(args: Array[String]): Unit = {
    // Set-up logging
    // Logging code derived from
    // https://syshell.net/2015/02/03/spark-configure-and-use-log4j/
    PropertyConfigurator.configure("./src/log4j.properties")
    val rootLog = LogManager.getRootLogger()
    rootLog.setLevel(Level.OFF)

     var inputFileName = args(0)
     var inputArtFileName = args(1)
    // Local logger
    log.info("Starting Clustering with input file: " + inputFileName)

    // Spark Configuration
    val conf = new SparkConf().setAppName("Clustering").setMaster("local")
    val sc = new SparkContext(conf)

    // Start Processing
    val songFile = sc.textFile(inputFileName, 2)
    val artistFile = sc.textFile(inputArtFileName, 2)
    val headerAndRows = songFile.map(line => line.split(";").map(_.trim))
    val header = headerAndRows.first
    val Records = headerAndRows.filter(_(0) != header(0))
    var loudRecords = songFile.map(row => new Loudness(row)).filter(_.checkValidity())
    var tempoRecords = songFile.map(row => new Tempo(row)).filter(_.checkValidity())
    var durationRecords = songFile.map(row => new Duration(row)).filter(_.checkValidity())
    var hotnessRecords = songFile.map(row => new Hotness(row)).filter(_.checkValidity())
    var combHotnessRecords = songFile.map(row => new Hotness(row)).filter(_.checkCombValidity())

    // Clean-up
    ClusterLoudness(loudRecords, sc)
    ClusterTempo(tempoRecords, sc)
    ClusterDuration(durationRecords, sc)
    ClusterHotness(hotnessRecords, sc)
    ClusterCombHotness(hotnessRecords, sc)
    println(System.currentTimeMillis / 1000 - startTime)

   //Agglomerative Clustering
   val songFileAgglo = sc.textFile(inputFileName, 2).sample(false, 0.005, 314159)

    // Clean-up
    var aggloRecords = songFileAgglo.map(row => new Loudness(row)).filter(_.checkValidity())
    aggloRecords.cache()

    val numEntries = aggloRecords.collect().length
    log.info(s"Number of entries in dataset: ${numEntries}")

    // Get Loudness
    var loudnessValues = aggloRecords.zipWithIndex().map(entry => (entry._2, entry._1.getLoudness()))
    var sortedDistances = pairwiseDistances(loudnessValues)
    log.info(s"Number of entries in sorted list = ${sortedDistances.count}")
    println("Building clusters for agglomerative")
    
    buildClusters(3, loudnessValues, sortedDistances, sc)
  }

  def ClusterLoudness(records: rdd.RDD[Loudness], sc: SparkContext): Unit ={
    var loudness = records.map(line => {
      if(line.getLoudness().toDouble > (rand.nextDouble - rand.nextInt(10)))  (1, (line.getLoudness(), -1.0));
      else if(line.getLoudness().toDouble > (rand.nextDouble - rand.nextInt(20))) (2, (line.getLoudness(), -1.0));
      else (3, (line.getLoudness(), -1.0))})
      println("details for loudness cluster")
      GetClusters(loudness, sc)
  }

  def ClusterTempo(records: rdd.RDD[Tempo], sc: SparkContext): Unit ={
    var tempo = records.map(line => {
      if(line.getTempo().toDouble > (rand.nextDouble + rand.nextInt(250)))  (1, (line.getTempo(), -1.0));
      else if(line.getTempo().toDouble > (rand.nextDouble + rand.nextInt(150))) (2, (line.getTempo(), -1.0));
      else (3, (line.getTempo(), -1.0))})
      println("details for Tempo cluster")
      GetClusters(tempo, sc)

  }
  def ClusterDuration(records: rdd.RDD[Duration], sc: SparkContext): Unit ={
    var duration = records.map(line => {
      if(line.getDuration().toDouble > (rand.nextDouble + rand.nextInt(2800)))  (1, (line.getDuration(), -1.0));
      else if(line.getDuration().toDouble > (rand.nextDouble + rand.nextInt(800))) (2, (line.getDuration(), -1.0));
      else (3, (line.getDuration(), -1.0))})
       println("details for duration cluster")
      GetClusters(duration, sc)
  }

  def ClusterHotness(records: rdd.RDD[Hotness], sc: SparkContext): Unit ={
    var hotness = records.map(line => {
      if(line.getSongHotness() > rand.nextDouble)  (1, (line.getSongHotness(), -1.0));
      else if(line.getSongHotness() > rand.nextDouble) (2, (line.getSongHotness(), -1.0));
      else (3, (line.getSongHotness(), -1.0))})
       println("details for song hotness cluster")
      GetClusters(hotness, sc)
  }

   def ClusterCombHotness(records: rdd.RDD[Hotness], sc: SparkContext): Unit ={
    val pairs = records.map(_.getCombHotness)
    var centroids = pairs.takeSample(false, 3).zipWithIndex
    for(i <- 0 to 10) {
      centroids = Cluster2D(pairs,centroids,sc)
    }
    // final centroids
    sc.parallelize(centroids.map(_.productIterator.mkString(","))).saveAsTextFile("output/2d_centroids")
    records.map(record => {
      (record.songId, record.getCombHotness, centroids.minBy( r=> {
        euclideanDistance2D(record.getCombHotness, r._1)
      })._2)
    }).map(_.productIterator.mkString(",")).saveAsTextFile("output/2d")
  }

  def Cluster2D(records: rdd.RDD[(Double, Double)],centroids :Array[((Double, Double), Int)], sc: SparkContext): Array[((Double, Double), Int)] ={
    val pointsToCluster = records.map(x => 
      (centroids.minBy( r=> {
        euclideanDistance2D(x, r._1)
      })._2, (x, 1))
    )

    pointsToCluster.reduceByKey((x,y)=> {
      ((x._1._1 + y._1._1, x._1._2 + y._1._2 ) , x._2+y._2)
    }).mapValues(value =>  (value._1._1/ value._2, value._1._2/ value._2)).map(_.swap).collect
    
  }
   def euclideanDistance2D(x1: (Double, Double), x2: (Double, Double)): Double = {
    var square = (x1._1 - x2._1) * (x1._1 - x2._1) + (x1._2 - x2._2)*(x1._2 - x2._2)
    math.sqrt(square)
  }

  def GetClusters(ClusterParameter : rdd.RDD[(Int, (Double, Double))], sc: SparkContext) : Unit = {
    var param = ClusterParameter
    var iterations = 0
    var oldC = new ArrayBuffer[Centroid]
    var centroids = new ArrayBuffer[Centroid]
    centroids += new Centroid()
    centroids += new Centroid()
    centroids += new Centroid()
    getCentroids(param, centroids, 3)
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
      param = setLabels(param, centroids)
      getCentroids(param, centroids, 3)
      //centroids parallelize
    }

    centroids.foreach(av => println("Centroids is : " + av.getValue()))
    println("Cluster 1 size is" + param.filter(l => l._1 == 1).count)
    println("Cluster 2 size is" + param.filter(l => l._1 == 2).count)
    println("Cluster 3 size is" + param.filter(l => l._1 == 3).count)
    println("Number of iterations done is " + iterations)
  }



  def getCentroids(loudness: rdd.RDD[(Int, (Double, Double))], centroids: ArrayBuffer[Centroid], k: Int): Unit ={
    var i = 1
    for (centroid <- centroids) {
      if (centroid.getValue() == -1.0) { 
        centroid.putKey(i)
        if (i == 1)
          centroid.putValue()
        else if(i == 2)
          centroid.putValue()
        else
          centroid.putValue()

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
      }
  }

  def setLabels(parameter: rdd.RDD[(Int, (Double, Double))], centroids: ArrayBuffer[Centroid]): rdd.RDD[(Int, (Double, Double))] ={
    //update the first value in loudness according to centroid distances.
    parameter.map(l =>
      ({ if(Math.abs(l._2._1-centroids(0).getValue()) < Math.min(Math.abs(l._2._1-centroids(1).getValue()), Math.abs(l._2._1-centroids(2).getValue()))) 1;
      else if(Math.abs(l._2._1-centroids(1).getValue()) < Math.abs(l._2._1-centroids(2).getValue())) 2;
      else 3 },
        l._2))
      .map(l =>
        (l._1, (l._2._1, Math.abs(l._2._1-centroids(l._1-1).getValue()))))
  }


  def euclideanDistance(x1 : Double, y1 : Double, x2 : Double, y2 : Double): Double = {
    var square = ((x1 - x2)* (x1 - x2)) + ((y1 - y2) * (y1 -y2))
    math.sqrt(square)
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





  /*def getByCName(mC: ArrayBuffer, i: Int): (Int, Double) ={

  }*/

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
    var distance = cartesian.map {lines => (euclideanDistance(lines._1._2, lines._2._2), (List(lines._1._1), List(lines._2._1)))}

    // Sort by distance
    distance.sortBy(_._1)
  }

  def continueIteration(clusters: rdd.RDD[(Double, (List[Long], List[Long]))], numClusters: Int): Boolean = {
    (clusters.count() > numClusters)
  }

  /**
    * This is the meat of the algorithm, this iterates over the clusters - picking the pair with the shortest
    * distance, merging them and continuing the process till the number of clusters remaining is the number
    * required.
    * @param input Initial Clusters
    * @param numClusters Target number of clusters
    */
  def buildClusters(numClusters: Int, dataset: rdd.RDD[(Long, Double)], input: rdd.RDD[(Double, (List[Long], List[Long]))], sc: SparkContext): Unit = {
    var clusters = input
    var clusterArray = input.collect().toList
    var oldClusters = ListBuffer[List[Long]]()

    while (continueIteration(clusters, numClusters)) {
      // Get the top most element
      val first = clusters.take(1)

      // Remove taken pair from cluster
      val firstRdd = sc.makeRDD(first)
      clusters = clusters.subtract(firstRdd)

      var pairData = first.toList(0)._2

      println(s"Considering clusters ${pairData._1}, ${pairData._2} - Remaining Size: ${clusters.count}")

      // Skip already considered nodes
      var skip = false
      oldClusters.foreach(elem => {
        var contains1 = pairData._1.contains(elem)
        var contains2 = pairData._2.contains(elem)
        skip = contains1 || contains2
      })

      if (!skip) {
        // Add merged clusters to oldClusters
        oldClusters += pairData._1
        oldClusters += pairData._2
      }
    }
  }

}