import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.broadcast


object RSJoin {


  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nRSJoin <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("RSJoin").setMaster("local")
    val sc = new SparkContext(conf)
    conf.set("spark.sql.crossJoin.enabled", "true")

    /*val textFile = sc.textFile(args(0))

    val maxFilter = 40000;
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._


    //Replicate join using data frames
    val edges = textFile.map(line => (line.split(",")(0), line.split(",")(1))).filter(data => data._1.toInt <= maxFilter && data._2.toInt <= maxFilter)
      .toDF("key", "value")
    val path2 =  edges.as("edges1").join(broadcast(edges.as("edges2")),$"edges1.value" === $"edges2.key").select($"edges2.value", $"edges1.key").toDF("traingle1", "traingle2");
    var trianglecount = path2.as("path2").join(edges.as("edges"), ($"edges.key" === $"path2.traingle1") &&($"edges.value" === $"path2.traingle2")).count()
    logger.info("Triangle Count: " + trianglecount/ 3)
    */
    val k = 2;
    var a: List[(String, String)] = List()
    var i = 1;
    var j = i;
    while (i < k * k) {
      while (j < i + k - 1) {
        a = a :+ ((j.toString(), (j + 1).toString()))
        j = j + 1;
      }

      a = a :+ ((j.toString(), "0"))
      i = i + k;
      j = j + 1;
    }

    a = a :+ (("0", ""))

    var rdd1 = a.groupBy(_._1).map { case (k, v) => (k, v.map(_._2)) } // Ask TA if we need this for this program
    val graph = sc.parallelize(rdd1.toSeq).map { case (k, v) => (k, v) }
    graph.persist()
    // graph.saveAsTextFile(args(1))

    var ranks = graph.map { case (key, value) => (key,
      if (key.equals("0")) {
        0
      }
      else {
        1.0 / (k * k)
      })
    }
    //   ranks.saveAsTextFile(args(1))
    for (i <- 1 to 1) {

      var contributions = graph.join(ranks).flatMap { case (pageId, (links, rank)) => links.map(dest => if (dest == "") ("0", 0.0) else (dest, rank / links.size)) }

      var noincomingNodes = ranks.subtractByKey(contributions).mapValues(d => d* 0.0)

      ranks = contributions.reduceByKey((x, y) => x + y).union(noincomingNodes)
      var dangling_mass = 0.0
      dangling_mass = ranks.lookup("0").sum
      dangling_mass = dangling_mass / (k * k * 1.0)
      ranks = ranks.map(key =>
        if (key._1 != "0") {
          var pr = (key._2 + dangling_mass )* 0.85;
          var random = 0.15 / (k *k *1.0);
          (key._1, pr+random)
        }
        else {
          (key._1, key._2 * 0.85)
        }
      )
    }
    ranks.saveAsTextFile(args(1))

    /**
     * 1 -0.14375  2- sas 4 3 4 0.35625
     */


  }
}