import breeze.linalg._
import breeze.plot._

import scala.io.Source


/**
 * The class is used to plot the different K values and their
 * corresponding SSE from the input files and then
 * compute the optimal based on elbow method.
 *
 **/
object GraphPlot {
  def main(args: Array[String]): Unit = {
    //Need to fix the relative path issue here
    val FilePath =  "/home/vandysnape/lspdpproject-Project/kmeans/ElbowMethod/input.csv"
    val k = DenseVector(Source.fromFile(FilePath)
      .getLines.map(_.split(",")(0).toDouble).toSeq :_ * )
    val sse = DenseVector(Source.fromFile(FilePath)
      .getLines.map(_.split(",")(1).toDouble).toSeq :_ * )
    val fig = Figure()
    val plt = fig.subplot(0)
    plt += plot(k, sse, name = "Elbow method. Find optimal K")
    plt.legend = true
    plt.xlabel = "K"
    plt.ylabel = "SSE"
    fig.saveas("KPlot.png")
  }
}
