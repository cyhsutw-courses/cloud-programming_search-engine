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
          val tokenCount = tokenPattern.findAllIn(page).size

          var arr = ListBuffer.empty[((String, String), (List[Long], Double))]
          while (matches.hasNext) {  
            matches.next()
            arr.append(
                (
                    (matches.group(0).toLowerCase(), fileId), 
                    (List(matches.start), 1.0 / tokenCount)
                )
            )
          }
          arr.toList
      }
      .reduceByKey { (off1, off2) => (off1._1 ++ off2._1, off1._2 + off2._2) }
      .mapValues { x => x._2.toString() + ";" + x._1.mkString(",") }
      .map { x => (x._1._1, x._1._2 + ";"+ x._2) }
      .groupByKey()
      .map(x => x._1 + ":" + x._2.size.toString +":" + x._2.mkString("/"))
      .saveAsTextFile(outputDir)
    
    try { sc.stop } catch { case _ : Throwable => {} }
  }
}