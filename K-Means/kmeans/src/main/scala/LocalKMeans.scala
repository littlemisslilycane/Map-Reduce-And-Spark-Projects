import KMeans.{closestPoint, parseVector}
import breeze.linalg.{DenseVector, Vector, squaredDistance}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object LocalKMeans {



  def nline(n: Int, path: String) = {
    val sc = SparkContext.getOrCreate
    val conf = new Configuration(sc.hadoopConfiguration)
    conf.setInt("mapreduce.input.lineinputformat.linespermap", n);

    sc.newAPIHadoopFile(path,
      classOf[NLineInputFormat], classOf[LongWritable], classOf[Text], conf
    )
  }


  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("Usage: SparkKMeans <file> <k> <convergeDist>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("KMeans")
      .config("spark.driver.memory", "8g")
      .config("spark.executor.memory", "8g")
      .config("spark.yarn.executor.memoryOverhead", "4096")
      .getOrCreate()

    val sc = spark.sparkContext

    val conf = new Configuration(sc.hadoopConfiguration)
    conf.setInt("mapreduce.input.lineinputformat.linespermap", 1);
    val convergeDist = args(2).toDouble

    val lines = sc.newAPIHadoopFile("local_input/k_values",
      classOf[NLineInputFormat], classOf[LongWritable], classOf[Text], conf
    )
    var newLines = lines.map{case (t1: LongWritable, t2: Text) => ""+t2}

    val textFile = sc.textFile("input")
    var crimeLocation = textFile.flatMap(line => line.split(" ")).filter(data => data.split(",").length == 2)



    var newCrimeLocation = crimeLocation.collect()
    newLines.sparkContext.broadcast(newCrimeLocation)

    val newLinesList = newLines.collect().toList

    kMeansMethod(newLinesList, sc.parallelize(newCrimeLocation), convergeDist, newLines)
  }


  def parseVector(line: String): Vector[Double] = {
    DenseVector(line.split(',')
      .filter(data => data.toString.matches("[-+]?([0-9]*\\.[0-9]+|[0-9]+)"))
      .map(data => {
        if (data.length() == 0 || data.toString.equals("")) {
          "0.0"
        } else data
      })
      .map(_.toDouble))

  }

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

  def kMeansMethod(newLinesList: List[String], lines: RDD[String], convergeDist:Double, newLines: RDD[String]) ={

    for(eachValueOfK <- newLinesList) {
      val K = eachValueOfK.toInt

      val data = lines.map(parseVector _).cache()


      val kPoints = data.takeSample(withReplacement = false, K, 42)
      var tempDist = 1.0
      var closest = data.map(p => (closestPoint(p, kPoints), (p, 1)))
      var pointsInACluster = closest.groupByKey().mapValues(_.map(_._1))
      var  pointStats = closest.reduceByKey { case ((p1, c1), (p2, c2)) => (p1 + p2, c1 + c2) }
      var newPoints = pointStats.map { pair =>
        (pair._1, pair._2._1 * (1.0 / pair._2._2))
      }.collectAsMap()

      while (tempDist > convergeDist) {
        //Compute the closest center to each point
        closest = data.map(p => (closestPoint(p, kPoints), (p, 1)))
        pointsInACluster = closest.groupByKey().mapValues(_.map(_._1))
        pointStats = closest.reduceByKey { case ((p1, c1), (p2, c2)) => (p1 + p2, c1 + c2) }

        newPoints = pointStats.map { pair =>
          (pair._1, pair._2._1 * (1.0 / pair._2._2))
        }.collectAsMap()


        tempDist = 0.0
        for (i <- 0 until K) {
          tempDist += squaredDistance(kPoints(i), newPoints(i))
        }

        for (newP <- newPoints) {
          kPoints(newP._1) = newP._2
        }

        println(s"Finished iteration " + K + " (delta = $tempDist)")
      }

      var SSE = 0.0
      for (i <- 0 until K) {
        val list = pointsInACluster.lookup(i)
        for (l <- list) {
          for (litem <- l) {
            SSE += squaredDistance(litem, newPoints(i))
          }
        }
      }

      println("**************************Iteration " + K + " **********************")
      closest.saveAsTextFile("output"+K)
      println("For "+ K +" The SSE value is  "+ SSE)
    }
  }
}
