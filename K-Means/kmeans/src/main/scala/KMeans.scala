import org.apache.spark.sql.SparkSession
import breeze.linalg.{DenseVector, Vector, squaredDistance}
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD


/**
 * A K Means clustering algorithm that can be distributed across machines. The number of iterations can be decided to
 * get different K values and SSE (Sum of squared Errors) on convergence. The SSE it written out to a new file, which
 * can be plotted on a graph to extract the ideal K.
 */
object KMeans {
  def parseVector(line: String): Vector[Double] = {
    DenseVector(line.split(',').map(_.toDouble))
  }

  /**
   * This method calculates the closest distance between the point and all the centroids and returns closest centroid
   * to this point
   *
   * @param p       it is the point whose closest neighbor is found
   * @param centers all all the centroids
   * @return index of the closest centroid
   */
  def closestPoint(p: Vector[Double], centers: Array[Vector[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    bestIndex
  }


  /**
   * This is the main driver program
   *
   * @param args are command line arguments, that take in
   */
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger()

    val spark = SparkSession
      .builder
      .appName("KMeans")
      .config("spark.driver.memory", "8g")
      .config("spark.executor.memory", "8g")
      .config("spark.yarn.executor.memoryOverhead", "4096")
      .getOrCreate()

    val sc = spark.sparkContext

    val lines = sc.textFile(args(0)).filter(data => data.split(",").length == 2)

    val data = lines.map(parseVector _).cache()

    var K = 1
    val convergeDist = 0.5 //args(2).toDouble
    val sseValues = new Array[Double](11)

    while (K <= 10) {
      val kPoints = data.takeSample(withReplacement = false, K, 42)
      val tempDist = 1.0

      val value = getSSEValues(tempDist, convergeDist, K, data, kPoints, args)
      sseValues(K) = value
      K = K + 1
    }

    logger.info("______SEE Values_______")
    for (i <- 1 until 11) {
      logger.info("K:" + i + " SSE: " + sseValues(i))

    }
    spark.stop()
  }


  /**
   * This method is used to calculate the SSE values associated with a given K. The algorithm runs till it converges
   *
   * @param tempDist     It is the squared distance value between the New points and old points
   * @param convergeDist The threshold for convergence
   * @param K            The number of K the SSE values are being calculated for
   * @param data         is a Dense vector that is cached
   * @param kPoints      are the centroids of cluster
   */
  def getSSEValues(tempDist: Double, convergeDist: Double, K: Int, data: RDD[Vector[Double]], kPoints:
  Array[Vector[Double]], args: Array[String]): Double = {
    var tempDistMutable = tempDist
    var SSE = 0.0

    var outputPoints: RDD[(Int, Iterable[Vector[Double]])] = null

    var closest = data.map(p => (closestPoint(p, kPoints), (p, 1)))
    while (tempDistMutable > convergeDist) {
      //Compute the closest center to each point
      closest = data.map(p => (closestPoint(p, kPoints), (p, 1)))

      val pointStats = closest.reduceByKey { case ((p1, c1), (p2, c2)) => (p1 + p2, c1 + c2) }
      val pointsInACluster = closest.groupByKey().mapValues(_.map(_._1))
      val newPoints = pointStats.map { pair =>
        (pair._1, pair._2._1 * (1.0 / pair._2._2))
      }.collectAsMap()
      tempDistMutable = 0.0
      for (i <- 0 until K) {
        tempDistMutable += squaredDistance(kPoints(i), newPoints(i))
      }


      for (newP <- newPoints) {
        kPoints(newP._1) = newP._2
      }

      SSE = 0.0


      for (i <- 0 until K) {
        val list = pointsInACluster.lookup(i)
        for (l <- list) {
          for (litem <- l) {
            SSE += squaredDistance(litem, newPoints(i))
          }
        }
      }
      if (tempDistMutable <= convergeDist && K == 9) {
        outputPoints = pointsInACluster
        val rddCount = outputPoints.count().toInt
        val clusterComposition = outputPoints.mapValues(v=>v.size)
        clusterComposition.saveAsTextFile(args(1)+"Composition")
        outputPoints.repartition(rddCount).saveAsTextFile(args(1))
      }
    }

    SSE
  }
}