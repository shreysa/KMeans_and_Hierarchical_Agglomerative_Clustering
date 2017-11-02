///**
//  * CS6240 - Fall 2017
//  * Assignment A6
//  * Spark and Scala test program
//  *
//  * @author Shreysa Sharma and Jashangeet Singh
//  */
//
//import org.apache.log4j.{LogManager, Level, PropertyConfigurator}
//import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.rdd
//import java.util.Calendar
//
//object SongAnalysis {
//  val log = LogManager.getLogger("SongAnalysis")
//  log.setLevel(Level.INFO)
//
//  // Set with articles, prepositions and conjunctions
//  val ignored = Set(
//      "THAT", "WITH", "THE", "AND", "TO", "OF",
//      "A", "IT", "SHE", "HE", "YOU", "IN", "I",
//      "HER", "AT", "AS", "ON", "THIS", "FOR",
//      "BUT", "NOT", "OR", "ME", "MY", "LP",
//      "VERSION", "IS", "ALBUM", "AN", "THE")
//
//  // entry point to the program
//  def main(args: Array[String]) {
//    val inputFileName = args(0)
//    val inputArtFileName = args(1)
//
//
//    // Set-up logging
//    // Logging code derived from
//    // https://syshell.net/2015/02/03/spark-configure-and-use-log4j/
//    PropertyConfigurator.configure("./src/log4j.properties")
//    val rootLog = LogManager.getRootLogger()
//    rootLog.setLevel(Level.OFF)
//
//    // Local logger
//    log.info("Starting SongAnalysis with input file: " + inputFileName)
//
//    // Spark Configuration
//    val conf = new SparkConf().setAppName("Song Analysis").setMaster("local")
//    val sc = new SparkContext(conf)
//
//    // Start Processing
//    val songFile = sc.textFile(inputFileName, 2)
//    val artistFile = sc.textFile(inputArtFileName, 2)
//    val headerAndRows = artistFile.map(line => line.split(";").map(_.trim))
//    val header = headerAndRows.first
//    val artistRecords = headerAndRows.filter(_(0) != header(0))
//    var records = songFile.map(row => new CleanUp(row))
//
//    // Clean-up
//    records = records.filter(_.checkValidity())
//
//    fuzzyLoudness(records)
//
//    // Task: The count of distinct artists, albums and songs
//    getUnique(records)
//
//    // Task: the top 5 loudest, longest, fastest and hottest songs
//    getTopSong(records)
//
//    //Task: The top 5 prolific, familiar and hottest artists
//    getTopArtist(records)
//
//    //Task: the 5 most popular keys
//    getTopKey(records, sc)
//
//    //Task: the 5 most common words in song titles
//    getTopWords(records, sc)
//
//    //Task: The 5 hottest genres
//    getTopGenre(records, sc, artistRecords)
//
//  }
//
//  // K-Means Clustering - Loudness
//  def fuzzyLoudness(records : rdd.RDD[CleanUp]): Unit = {
//
//    var songtuple = x.map { l => (l.getSongId(), l.getTitle()) }             // (song_id, song_name)
//    val loud = x.map { _.getLoudness() }
//    //val top5loud = songtuple.zip(loud).map{ x => (x._1, x._2) }.sortBy(_._2, false).take(5).toList
//    //log.info("[Task 2] The top 5 loud songs are: " + top5loud)
//
//    // Load and parse the data
//    val parsedData = loud.map(s => Vectors.dense(s).cache()
//
//    val numClusters = 3
//    val numIterations = 20
//    val clusters = KMeans.train(parsedData, numClusters, numIterations)
//
//    // Evaluate clustering by computing Within Set Sum of Squared Errors
//    val WSSSE = clusters.computeCost(parsedData)
//    println("Within Set Sum of Squared Errors = " + WSSSE)
//  }
//
//  // Logs the count of distinct artists, albums and songs
//  def getUnique( records : rdd.RDD[CleanUp] ): Unit ={
//    val uniq_artist = records.map(_.getArtId()).distinct.count
//    val uniq_song = records.map(_.getSongId()).distinct.count
//    val uniq_alb = records.map(_.getAlbum()).distinct.count
//    log.info("[Task 1.a] Count of distinct artists "  + uniq_artist)
//    log.info("[Task 1.b] Count of distinct albums "  + uniq_alb)
//    log.info("[Task 1.c] Count of distinct songs "  + uniq_song)
//  }
//
//  // Logs the top 5 loudest, longest, fastest and hottest songs
//  def getTopSong( x : rdd.RDD[CleanUp] ): Unit = {
//
//    var songtuple = x.map { l => (l.getSongId(), l.getTitle()) }             //(song_id, song_name)
//    val loud = x.map { _.getLoudness() }
//    val top5loud = songtuple.zip(loud).map { x => (x._1, x._2) }.sortBy(_._2, false).take(5).toList
//    log.info("[Task 2] The top 5 loud songs are: " + top5loud)
//
//    val duration = x.map { _.getDuration() }
//    val top5long = songtuple.zip(duration).map { x => (x._1, x._2) }.sortBy(_._2, false).take(5).toList
//    log.info("[Task 3] The top 5 long songs are: " + top5long)
//
//    val tempo = x.map { _.getTempo() }
//    val top5fast = songtuple.zip(tempo).map { x => (x._1, x._2) }.sortBy(_._2, false).take(5).toList
//    log.info("[Task 4] The top 5 fast songs are: " + top5fast)
//
//    val hotns = x.map { _.getSongHot() }
//    val top5hot = songtuple.zip(hotns).map { x => (x._1, x._2) }.sortBy(_._2, false).distinct.take(5).toList
//    log.info("[Task 6] The top 5 hot songs are: " + top5hot)
//
//  }
//
//  // Logs the top 5 prolific, familiar and hottest artists
//  def getTopArtist( records : rdd.RDD[CleanUp] ): Unit = {
//
//    var artistuple = records.map{ l => (l.getArtId(), l.getArtistName()) }
//    val top5Art = artistuple.mapValues(x => (x,1)).reduceByKey((x, y) => (x._1, x._2 + y._2)).sortBy(_._2._2, false).take(5).toList
//    log.info("[Task 10] The top 5 prolific artists are: " + top5Art)
//
//    artistuple = records.map { l => (l.getArtistName(), l.getArtId()) }               //(artist_name, artist_id) tuple
//    val fam = records.map { _.getArtFam() }
//    val top5famArt = artistuple.zip(fam).map { x => (x._1, x._2) }.distinct.sortBy(_._2, false).take(5).toList
//    log.info("[Task 5] The top 5 familiar artists are: " + top5famArt)
//
//    val artHot = records.map { _.getArtHot() }
//    val top5famArtHot = artistuple.zip(artHot).map { x => (x._1, x._2) }.distinct.sortBy(_._2, false).take(5).toList
//    log.info("[Task 7] The top 5 hot artists are: " + top5famArtHot)
//  }
//
//  // Logs the 5 most popular keys
//  def getTopKey( x : rdd.RDD[CleanUp], sc : SparkContext ): Unit ={
//    val top5key = sc.parallelize(x.map{ _.getKey() }.countByValue.toSeq).sortBy(_._2, false).take(5).toList
//    log.info("[Task 9] The 5 most popular keys are: " + top5key)
//  }
//
//  // Logs the 5 most common words in song titles
//  def getTopWords( records : rdd.RDD[CleanUp], sc : SparkContext): Unit ={
//    val words = records.map{_.getTitle()}.flatMap { line => line.filter(c => c.isLetter || c.isWhitespace).toUpperCase.split(' ') }.filter { !_.isEmpty }
//    val top5words = sc.parallelize(words.filter { !ignored.contains(_) }.countByValue.toSeq).sortBy(_._2, false).take(5).toList
//
//    log.info("[Task 11] The 5 most common words in song titles are: " + top5words)
//  }
//
//  // Logs the 5 hottest genres
//  def getTopGenre( records : rdd.RDD[CleanUp], sc : SparkContext, artistRecords : rdd.RDD[Array[String]]): Unit ={
//
//    val artist_term = artistRecords.map(lines => (lines(0), lines(1))).distinct
//    val artist_hotness = records.map(lines => (lines.getArtId(), lines.getArtHot()))
//    var afterJoin = artist_term.join(artist_hotness)
//    var afterGrouping = afterJoin.map(gr => (gr._2._1, gr._2._2)).groupBy(_._1)
//    val top5Genres =  afterGrouping.map(lines => (lines._1, getMeanScore(lines._2))).sortBy(- _._2).take(5)
//
//    log.info("[Task 8] The 5 hottest genres are : " + top5Genres.foreach {println})
//  }
//
//  def getMeanScore(values : Iterable[(String, Double)]) : Double = {
//    var noOfEntries : Int = 0
//    var sum : Double = 0.00
//   values.foreach(av => {
//      noOfEntries += 1
//      sum += av._2
//      })
//      sum/noOfEntries
//  }
//
//}
