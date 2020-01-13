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
    val conf = new SparkConf().setAppName("RSJoin")
    val sc = new SparkContext(conf)
    conf.set("spark.sql.crossJoin.enabled", "true")

    val textFile = sc.textFile(args(0))

    val maxFilter = 20000;
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._






 //    * RS Join using RDD

 val leftRDD =  textFile.map(line => (line.split(",")(0), line.split(",")(1))).filter(data => data._1.toInt <= maxFilter && data._2.toInt <= maxFilter)
 val rightRDD =  textFile.map(line => (line.split(",")(1), line.split(",")(0))).filter(data => data._1.toInt <= maxFilter && data._2.toInt <= maxFilter)
 val joinRDD= leftRDD.join(rightRDD);
 val path2rdd = joinRDD.map(line => (line._2._1+","+line._2._2,""));
 val edgesrdd = textFile.map(line => (line.split(",")(0)+","+line.split(",")(1),""));
 val countRDD = path2rdd.join(edgesrdd).count();
 logger.info("Triangle Count:" + countRDD/3);





    //Reduce Join using Dataframes
    val edge = textFile.map(line => (line.split(",")(0), line.split(",")(1))).filter(data => data._1.toInt <= maxFilter && data._2.toInt <= maxFilter)
      .toDF("key", "value")
    val path2df = edge.as("edges1").join(edge.as("edges2"),$"edges1.value"=== $"edges2.key").select($"edges2.value", $"edges1.key").toDF("traingle1", "traingle2");
    val trianglecount = path2df.as("path2").join(edge.as("edges"),($"path2.traingle1" === $"edges.key") && ($"path2.traingle2" === $"edges.value")).toDF();
    logger.info("Reduce side dataframe triangle count:" + trianglecount.count()/3);




   /*  Replicate Join using RDDs */

    val smallRDD = textFile.filter(data => (data.split(",")(0).toInt <= maxFilter && data.split(",")(1).toInt <= maxFilter))
      .map(line => (line.split(",")(0), line.split(",")(1)))
    val smallRDDLocal1 = smallRDD.groupBy(_._1).mapValues(_.map(_._2))
    val smallRDDLocal = smallRDDLocal1.collectAsMap()
    val bigRDD  = smallRDD
    bigRDD.sparkContext.broadcast(smallRDDLocal);
    val path2RDD = bigRDD.mapPartitions(iter=>{
      iter.flatMap{
        case(key,value) => smallRDDLocal.get(value) match{
          case None=> None
          case Some(v2) => v2.map(u=>(u, key))

        }
        }


      },preservesPartitioning = true)
    trianglecount = path2RDD.join(bigRDD).filter(data => data._2._1 == data._2._2).count()
    System.out.println("triangle count" + trianglecount/3);



    //Replicate join using data frames
    val edges = textFile.map(line => (line.split(",")(0), line.split(",")(1))).filter(data => data._1.toInt <= maxFilter && data._2.toInt <= maxFilter)
      .toDF("key", "value")
    val path2 =  edges.as("edges1").join(broadcast(edges.as("edges2")),$"edges1.value" === $"edges2.key").select($"edges2.value", $"edges1.key").toDF("traingle1", "traingle2");
     trianglecount = path2.as("path2").join(edges.as("edges"), ($"edges.key" === $"path2.traingle1") &&($"edges.value" === $"path2.traingle2")).count()
    logger.info("Triangle Count: " + trianglecount/ 3)

  }


}