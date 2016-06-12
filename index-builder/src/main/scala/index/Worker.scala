package index

import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable._

object Worker {
  private def unescape(str: String) : String = {
    str.replaceAllLiterally("&lt;", "<")
       .replaceAllLiterally("&gt;", ">")
       .replaceAllLiterally("&amp;", "&")
       .replaceAllLiterally("&quot;", "\"")
       .replaceAllLiterally("&apos;", "'")
  }
  
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
    
    val pages = sc.textFile(inputDir, sc.defaultParallelism * 3).zipWithIndex()
    
    val a = pages
      .flatMap { 
        case (line, offset) =>
          val kv = line.split("\\|\\-\\-\\-\\-\\-\\|")
          if (kv.size == 2) {
            val title = kv(0) 
            val text = kv(1)
            val matches = tokenPattern.findAllIn(text)
            var arr = ListBuffer.empty[((String, String), List[Long])]
            while (matches.hasNext) {  
              matches.next()
              arr.append(((matches.group(0), title), List(offset + matches.start)))
            }
            arr.toList
          } else {
            List()
          }
      }
      .reduceByKey { (off1, off2) => off1 ++ off2 }
      .map { tup => (tup._1._1, (tup._1._2, tup._2)) }
      .groupByKey()
      .mapValues(iter => iter.mkString(", "))
      .saveAsTextFile(outputDir)
    
    try { sc.stop } catch { case _ : Throwable => {} }
  }
}