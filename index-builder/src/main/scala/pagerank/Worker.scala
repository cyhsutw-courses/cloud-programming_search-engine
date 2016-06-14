package pagerank

import scala.xml._
import org.apache.spark._
import org.apache.hadoop.fs._

object Worker { 
  def main(args: Array[String]) {
    val inputPath = args(0)
    val inputMapPath = args(1)
    val outputDir = args(2)

    val config = new SparkConf().setAppName("PageRank")
    val ctx = new SparkContext(config)

    // clean output directory
    val hadoopConf = ctx.hadoopConfiguration
    var hdfs = FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new Path(outputDir), true)
    } catch {
      case ex : Throwable => {
        println(ex.getMessage)
      }
    }
    
    val maps = ctx.textFile(inputMapPath, ctx.defaultParallelism * 9)
    
    val titleIDMap = maps.map { 
      case (line) => 
        val parts = line.split("🐻")
        if (parts.size == 2) {
          (parts(0), parts(1))
        } else {
          ("", "")
        }
    }

    
    val pages = ctx.textFile(inputPath, ctx.defaultParallelism * 9)

    val linkPattern = """\[\[[^\]]+\]\]""".r
    val linkSplitPattern = "[#|]"
    
    val parseStartTime = System.nanoTime()
    println("Start parsing")
    
    val adjMat = pages.flatMap { 
      case (page) =>
        val xmlElement = XML.loadString(page)
        val title = (xmlElement \\ "title").text.capitalize
        linkPattern.findAllIn(xmlElement.text)
                   .toArray
                   .map { link => link.substring(2, link.length() - 2).split(linkSplitPattern) }
                   .filter { arr => arr.size > 0 }
                   .map { arr => (arr(0).capitalize, title) }
                   .union(Array((title, "🐦" + title + "🐦")))
    }.groupByKey(ctx.defaultParallelism * 9)
     .filter { tup => 
      val magicWord = "🐦" + tup._1 + "🐦"
      val titles = tup._2.toSet
      titles.contains(magicWord)
     }
     .flatMap { tup =>
       val link = tup._1
       val magicWord = "🐦" + link + "🐦"
       val titles = tup._2.toSet
       titles.map { x =>
         if (x != magicWord) {
           (x, link)
         } else {
           (link, "")
         }
       }
    }.groupByKey(ctx.defaultParallelism * 9).map { tup =>
      if(tup._2.size == 1){
        (tup._1, Iterable())
      } else {
        (tup._1, tup._2.filter(x => !x.isEmpty()))
      }
    }.cache
    
    val finalT = System.nanoTime()
    println("Parsing time " + ((finalT - parseStartTime)/1000000000.0).toString())
    println("Start counting")
    val cnt_s = System.nanoTime()
    
    val numDocs = pages.count()
    val cnt_e = System.nanoTime()
    
    println("count time " + ((cnt_e - cnt_s)/1000000000.0).toString())
    
    val teleport = 0.15 * (1.0 / numDocs)
    
    val rank_s = System.nanoTime()
    var ranks = adjMat.map(x => (x._1, 1.0 / numDocs))
    val rank_e = System.nanoTime()
    println("rank map time " + ((rank_e - rank_s)/1000000000.0).toString())
    
    var diff = 0.0
    var iter = 0
    do {
      val begin = System.nanoTime()
      var sinkNodeRankSum = adjMat.join(ranks)
                                  .filter(tup => tup._2._1.size == 0)
                                  .map(tup => tup._2._2)
                                  .sum()

      sinkNodeRankSum = sinkNodeRankSum / numDocs * 0.85


      val updates = adjMat.join(ranks)
                          .values
                          .filter(tup => tup._1.size >= 1)
                          .flatMap { case (links, rank) =>
                            val size = links.size
                            links.map(x => (x, rank / size))
                           }
      var newRanks = updates.reduceByKey(_ + _)
      newRanks = ranks.fullOuterJoin(newRanks).map(x => (x._1, x._2._2.getOrElse(0.0) * 0.85 + teleport + sinkNodeRankSum))
      diff = ranks.join(newRanks).map(x => math.abs(x._2._1 - x._2._2)).sum()
      println("diff = " + diff.toString())
      ranks = newRanks

      val end = System.nanoTime()
      println(s"round: " + (end - begin)/1000000000.0)
    } while(diff >= 0.001)

    ranks.join(titleIDMap).map(x => (x._2._2, x._2._1))
         .sortBy(tup => (-tup._2, tup._1), true, ctx.defaultParallelism * 9)
          .map(tup => tup._1 + ":" + tup._2.toString())
          .saveAsTextFile(outputDir)

    try { ctx.stop } catch { case _ : Throwable => {} }
  }
}
