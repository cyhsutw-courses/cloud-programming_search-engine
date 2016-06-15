package index

import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable._


object Worker {

  def main(args: Array[String]) {
    val inputDir = args(0)
    val outputDir = args(1)

    val conf = new SparkConf().setAppName("Index")
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
    
    val tokenPattern = """[a-zA-Z]+""".r
    
    val pages = sc.wholeTextFiles(inputDir, sc.defaultParallelism)
    
    val a = pages
      .flatMap { 
        case (filePath, page) =>
          val fileId = filePath.split("/").last
          val matches = tokenPattern.findAllIn(page)
          var arr = ListBuffer.empty[((String, String), List[Long])]
          while (matches.hasNext) {  
            matches.next()
            arr.append(((matches.group(0).toLowerCase(), fileId), List(matches.start)))
          }
          arr.toList
      }
      .reduceByKey { (off1, off2) => off1 ++ off2 }
      .map { x => (x._1._1, (x._1._2.toString(), x._2.mkString(","))) }
      .groupByKey
      .flatMap { x =>
        x._2.map(tup => (x._1, tup)) 
      }
      .map { x =>
        x._1 + ";" + x._2._1 + ";" + x._2._2
      }
      .saveAsTextFile(outputDir)
    
    try { sc.stop } catch { case _ : Throwable => {} }
  }
}