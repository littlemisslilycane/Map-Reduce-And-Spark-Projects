package wc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession


object ScalaMain {
  val logger: org.apache.log4j.Logger = LogManager.getRootLogger;
  val conf = new SparkConf().setAppName("ScalaMain").setMaster("local")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {

    if (args.length != 2) {
      logger.error("Usage:\nwc.ScalaMain <input dir> <output dir>")
      System.exit(1)
    }


    datasetCount(args);
    //Call the below functions one by one.






  }
  //with dataset
  def datasetCount(args: Array[String]): Unit = {
    val textFile = sc.textFile(args(0))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val textRDD = textFile.flatMap(line => line.split(" ")).map(word => (word.split(",")(1), 1))

    // convert the rdd into dataset and sum the followee
    val dataSet = textRDD.toDS().groupBy("_1").sum("_2")
    dataSet.explain(true)
  }
  //RDD with reduceByKey
  def rddReduceByKey(args: Array[String]): Unit = {
    val textFile = sc.textFile(args(0))

    val counts = textFile.map(line => if (line.split(",").length == 2) {
      line.split(",")(1)
    } else {
      None
    })
      .map(word => (word, 1));

    val finalcount = counts.reduceByKey(_+_)
    logger.info(finalcount.toDebugString)
    finalcount.saveAsTextFile(args(1))

  }
  //RDD with AggregateByKey
  def rddFoldByKey(args: Array[String]): Unit = {
    val textFile = sc.textFile(args(0))

    val counts = textFile.map(line => if (line.split(",").length == 2) {
      line.split(",")(1)
    } else {
      None
    })
      .map(word => (word, 1));

    val finalcount = counts.foldByKey(0)(_+_)
    logger.info(finalcount.toDebugString)
    finalcount.saveAsTextFile(args(1))

  }
  //RDD with AggregateByKey
  def rddAggregateByKey(args: Array[String]): Unit = {
    val textFile = sc.textFile(args(0))

    val counts = textFile.map(line => if (line.split(",").length == 2) {
      line.split(",")(1)
    } else {
      None
    })
      .map(word => (word, 1));

    val finalcount = counts.aggregateByKey(0)(_ + _, _ + _)
    logger.info(finalcount.toDebugString)
    finalcount.saveAsTextFile(args(1))

  }

  //RDD with groupByKey
  def rddGroupByKey(args: Array[String]) {

    val textFile = sc.textFile(args(0))
    val counts = textFile.map(line => if (line.split(",").length == 2) {
      line.split(",")(1)
    } else {
      None
    })
      .map(word => (word, 1));
    val groups = counts.groupByKey();
    logger.info("After group by key:")
    logger.info(groups.toDebugString);
    val finalCount = groups.map(t => (t._1, t._2.sum));
    logger.info("Final Count Debug String:")
    logger.info(finalCount.toDebugString)
    finalCount.saveAsTextFile(args(1))
  }

}