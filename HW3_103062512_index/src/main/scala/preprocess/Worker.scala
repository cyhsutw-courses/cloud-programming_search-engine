package preprocess

import org.apache.hadoop.fs._
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.xml._

object Worker {
  
  def main(args: Array[String]) {
    val inputPath = args(0)
    val outputDirDoc = args(1)
    val outputDirMap = args(2)
    val outputDirInvertedMap = args(3)

    val conf = new SparkConf().setAppName("Preprocessor")
    val sc = new SparkContext(conf)

    // clean output directory
    val hadoopConf = sc.hadoopConfiguration
    var hdfs = FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new Path(outputDirDoc), true)
    } catch {
      case ex : Throwable => {
        println(ex.getMessage)
      }
    }
    
    try {
      hdfs.delete(new Path(outputDirMap), true)
    } catch {
      case ex : Throwable => {
        println(ex.getMessage)
      }
    }
    
    
    val pages = sc.textFile(inputPath, sc.defaultParallelism * 3)
    
    val maps = pages.map { line =>
      val xmlElement = XML.loadString(line)
      val title = (xmlElement \\ "title").text.trim.capitalize
      val text = (xmlElement \\ "text").text.replaceAll("\\s+", " ").trim
      (title, (text, line))
    }.zipWithUniqueId
      .map(x => (x._2.toString(), x._1))
      .cache
     maps.map(x => x._1 +"\t" + x._2._2._1)
         .saveAsTextFile(outputDirDoc)
     maps.map(x => x._2._1 + "\t" + x._1).saveAsTextFile(outputDirMap)
     maps.map(x => x._1 + "\t" + x._2._1).saveAsTextFile(outputDirInvertedMap)
    
    try { sc.stop } catch { case _ : Throwable => {} }
  }
}