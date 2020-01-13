import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

object DataCleaning {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TwitterFollower").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val textFile = sc.textFile(args(0))


    import sqlContext.implicits._
    for (i <- 0 until args.length - 1) {
      val lines = textFile.filter(data => {
        val arr = data.split(",")
        arr(arr.length - 1).contains(")")
      }).map(data => {
        val arr = data.split(",")

        (arr(arr.length - 2).replace("\"", "").replace("(", ""),
          arr(arr.length - 1).replace("\"", "").replace(")", ""))
      }).toDF("Latitude", "Longitude")

      lines.coalesce(1).write.csv(args(1))
    }
  }

  def cleanDataAndWriteToAFile(): Unit = {

  }
}
