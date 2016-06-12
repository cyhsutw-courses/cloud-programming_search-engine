package preprocess

import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.xml._


object Worker {
  
  def main(args: Array[String]) {
    val inputPath = args(0)
    val outputDir = args(1)

    val conf = new SparkConf().setAppName("Preprocessor")
    val sc = new SparkContext(conf)

    // clean output directory
    val hadoopConf = sc.hadoopConfiguration
    var hdfs = FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new Path(outputDir), true)
    } catch {
      case ex : Throwable => {
        println(ex.getMessage)
      }
    }
    
    
    val pages = sc.textFile(inputPath, sc.defaultParallelism * 3)
    
    pages.map { line =>
      val xmlElement = XML.loadString(line)
      val title = (xmlElement \\ "title").text.capitalize
      val text = (xmlElement \\ "text").text
      title + "|-----|" + text
    }.saveAsTextFile(outputDir)
    
    try { sc.stop } catch { case _ : Throwable => {} }
  }
}