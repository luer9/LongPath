import DataProcess.DataReader.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map


object Main {
  val spark: SparkSession = Configuration.Configuration.sparkSession
  val _sc: SparkContext = spark.sparkContext
  import spark.implicits._
  def main(args: Array[String]): Unit = {
    // vp dir
    val vpDir = "F:\\dbproject\\DataAnaysis\\VP"
    //    val dir = new File(vpDir)
    //    if(dir.exists() && dir.isDirectory()) {
    //      val files = dir.listFiles()
    //      files.foreach(file => println(file.getName()))
    //    }
    // 读取一个vp表
    val curPred = "14967"
    // "45293"   "14966" OME
    val VP = vpDir + File.separator + curPred
    // 构件图
    val curVP = spark.sqlContext.read.parquet(VP).toDF().distinct()
    // 边
    val edges = curVP.count()
    // 顶点
    val nodes = curVP.select("sub").union(curVP.select("obj")).distinct().count()
    println("======================================")
    println("当前谓词：" + curPred + " -> 顶点：" + nodes + " " + " 边：" + edges)
    val datas = curVP.collect()
    // dp
    //     读取数据
    case class Rec(x: Long, y: Long)
    val RecArray: ArrayBuffer[Rec] = ArrayBuffer.empty[Rec]
    datas.foreach(row => {
      RecArray += Rec(row.get(0).toString.toLong, row.get(1).toString.toLong)
    })
    //    构建边与边的关系
    try{
      val mapp: Array[Array[Int]] = Array.ofDim[Int](nodes.toInt, nodes.toInt)
      var index = 0
      val vis: Map[Long, Int] = Map.empty
      for (i <- 0 until (edges.toInt)) {
        // 遍历所有的边，建立 mapp顶点与顶点之间的连接
        // x -> y
        // 把 x 和 y的编号局限到 [0, 13896)范围内
        val uu = RecArray(i).x
        val vv = RecArray(i).y
        var u = -1
        var v = -1
        if (vis.contains(uu)) {
          u = vis.get(uu).get
        } else {
          u = index
          vis += (uu -> index)
          index += 1
        }
        if (vis.contains(vv)) {
          v = vis.get(vv).get
        } else {
          v = index
          vis += (vv -> index)
          index += 1
        }
        mapp(u)(v) = 1
      }
      println("图构建完毕")
      val dp: ArrayBuffer[Int] = ArrayBuffer.fill(nodes.toInt)(0)
      var max = -1
      // 谓词 三元组数（边） 顶点 最长路径
      for (i <- 0 until nodes.toInt) {
        DP(i, dp, nodes.toInt, mapp)
      }
      for (i <- 0 until nodes.toInt) {
        if (max < dp(i)) {
          max = dp(i)
        }
      }
      println("最长路径：" + max)
      println("======================================")
    }catch {
      case ex: OutOfMemoryError => {
        println("谓词：" + curPred + "-> java.lang.OutOfMemoryError: Java heap space ")
        println("======================================")
      }
    }
  }

  def DP(i: Int, dp: ArrayBuffer[Int], nodes: Int, map: Array[Array[Int]]): Int = {
    if(dp(i) > 0) return dp(i)
    for(j <- 0 until nodes)
    {
      if(map(i)(j) == 1) {
//        println("(" + i + ", " + j + " )")
        dp(i) = Math.max(dp(i),
          DP(j, dp, nodes, map) + map(i)(j))
      }
    }
    return dp(i)
  }
}
