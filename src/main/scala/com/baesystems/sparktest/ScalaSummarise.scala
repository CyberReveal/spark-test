package com.baesystems.sparktest

import org.apache.spark.SparkContext
import scala.util.matching.Regex
import scala.util.matching.Regex.Match
import scala.util.Try
import scala.util.Success
import scala.util.Failure

/**
 * ScalaSummarise App
 */
object ScalaSummarise extends App {

  class Stats(val count: Int, val numBytes: Int) extends Serializable {
     def merge(other: Stats): Stats = new Stats(count + other.count, numBytes + other.numBytes) {
     }

     override def toString: String = "bytes=%s\tcount=%s".format(numBytes, count)
  }

  def extractKey(line: String): (String, String, String) = {
    val m = doMatching(line)
    if (m==null) return (null, null, null)

    val ip = m.group(1);
    val user = m.group(3);
    val query = m.group(5);

    (ip, user, query)
  }

  def extractStats(line: String): Stats = {
    val m = doMatching(line)
    if (m==null) return new Stats(1, 0)

    val bytes = Try(m.group(9).toInt)
    bytes match {
      case Success(v) =>
        new Stats(1, v)
      case Failure(e) =>
        new Stats(1, 0)
    }
  }

  def doMatching(line: String): Match = {
    val logRegex = """^(\S+) (\S+) (\S+) \[(.*)] "(\S+) (\S+) (\S+)" (\d*) (\d*).?""".r
    val matched = logRegex.findFirstMatchIn(line)
    matched match {
      case Some(m) => m
      case None => null
    }
  }

  /**
   * Main def
   */
  override def main(args: Array[String]) {
    val sc = new SparkContext()
    
    val dataSet = if (args.length == 1) sc.textFile(args(0)) else sc.textFile("hdfs:/access_log.txt")
    
    var counter = 0;
    dataSet.map(line => (extractKey(line), extractStats(line)))
      .reduceByKey((a, b) => a.merge(b))
      .collect().foreach {
        case (key, value) =>
          println("%s\t%s".format(key, value))
          counter += value.count
      }

    println("Total rows: " + counter)
  }
}
