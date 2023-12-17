import LongestPathGraphX.sc
import org.apache.log4j.{Level, Logger}
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark
import org.apache.spark.graphx.{Edge, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, FileInputStream, FileOutputStream}
import scala.collection.mutable.ListBuffer

object LongestPathGraphX {
  Logger.getLogger("org").setLevel(Level.WARN)
  // 初始化Spark配置
  val conf = new SparkConf().setAppName("DAGLongestPath").setMaster("local[*]")
    .set("spark.default.parallelism", "300")
    .set("spark.executor.memory", "120G")
    .set("spark.driver.memory", "100G")
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder().config(conf).getOrCreate()

  // vp dir
  val vpDir = "/Users/luyang/Documents/project/db/LongestPathGraphX/VP"
//  val excelFilePath = "/Users/luyang/Documents/project/db/LongestPathGraphX/pathdblp.xlsx"
  val donefile = "/Users/luyang/Documents/project/db/LongestPathGraphX/done.txt"

  // vp dir
//  val vpDir = "/home/lulu/new/uobm20/VP"
//  val excelFilePath = "/home/lulu/new/dblp_nonempty/pathdblp.xlsx"
//  val donefile = "/home/lulu/new/uobm20/done.txt"


  def main(args: Array[String]): Unit = {
    // load 加载已经处理的谓词
    val textData = spark.read.text(donefile)
    val array = textData.collect().map(row => row.get(0).toString)
    // load the existing Excel File
//    val fileIn = new FileInputStream(excelFilePath)
//    val workbook = new XSSFWorkbook(fileIn)
//    val sheet = workbook.getSheetAt(0)
    val dir = new File(vpDir)
    if(dir.exists() && dir.isDirectory()) {
      val files = dir.listFiles()
      files.foreach(
        file => {
          val curPred = file.getName
          if (!array.contains(curPred)) {
            try {
              val VP = vpDir + File.separator + curPred
              // 读取vp表
              val curVP = spark.sqlContext.read.parquet(VP).toDF().distinct()
              // 边
              val edges_count = curVP.count()
              // 顶点
              val nodes = curVP.select("sub").union(curVP.select("obj")).distinct()
              val nodes_count = nodes.count()
              println("======================================")
              println("当前谓词：" + curPred + " -> 顶点：" + nodes_count + " " + " 边：" + edges_count)
              val datas = curVP.collect()
              val ori = nodes.collect().maxBy(row => row.getLong(0)).get(0).toString.toInt
              val vertices = Array
                .tabulate(ori)(i => (i.toLong, i))

              val edges = datas
                .map(row => Edge(row.get(0).toString.toLong, row.get(1).toString.toLong, 1))

              // 创建Graph对象
              val graph = Graph(sc.parallelize(vertices, numSlices = 300), sc.parallelize(edges, numSlices = 300))
              println("[Graph done]")
              // 调用Pregel算法计算最长路径
              val resultGraph = pregel(graph, ori)
              println("[Pregel done]")
              // 从结果图中提取最长路径长度
              val longestPathLength = resultGraph.vertices.map(_._2).reduce(Math.max)

              // 打印最长路径长度
              println("Longest Path Length: " + longestPathLength)

              // =================== save excel
              //            val newRow = sheet.createRow(sheet.getLastRowNum + 1)
              //            val cell1 = newRow.createCell(0, CellType.STRING)
              //            val cell2 = newRow.createCell(1, CellType.NUMERIC)
              //            val cell3 = newRow.createCell(2, CellType.NUMERIC)
              //            val cell4 = newRow.createCell(3, CellType.NUMERIC)
              //            cell1.setCellValue(curPred)
              //            cell2.setCellValue(edges_count)
              //            cell3.setCellValue(nodes_count)
              //            cell4.setCellValue(longestPathLength)
              //            val fileOut = new FileOutputStream(excelFilePath)
              //            workbook.write(fileOut)
              //            fileOut.close()
              println("======================================")
              // 清除内存
              curVP.unpersist()
              nodes.unpersist()
              graph.unpersist()
              resultGraph.unpersist()
            } catch {
              case ex: OutOfMemoryError => {
                println("谓词：" + curPred + "-> java.lang.OutOfMemoryError: Java heap space ")
              }
              case e: StackOverflowError => {
                println("谓词：" + curPred + "-> StackOverflowError ")
              }
              case _ => {
                // 处理其他异常
                println("谓词：" + curPred + "-> 其他异常 ")
              }
            }
          }

        }
      )
    }
    // 停止SparkContext

    sc.stop()
  }

  def pregel(graph: Graph[Int, Int], ori: Int): Graph[Int, Int] = {
    // 初始化所有顶点的属性为负无穷
//    val initialGraph = graph.mapVertices((id, _)=> if (id == ori) 0 else Int.MinValue)
//    val reversedGraph = graph.reverse // 反转图
    val initialGraph = graph.mapVertices((_, _) => 0)
    println("[Initial Graph done]")
    // 定义Pregel迭代函数
    def vprog(vertexId: VertexId, value: Int, message: Int): Int = {
      math.max(value, message)
    }

    def sendMsg(triplet: org.apache.spark.graphx.EdgeTriplet[Int, Int]): Iterator[(VertexId, Int)] = {
      if (triplet.srcAttr + triplet.attr > triplet.dstAttr) {
//        println("[line 106] " + triplet.srcAttr + " " + triplet.attr + " " + triplet.dstAttr)
        Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
      } else {
        Iterator.empty
      }
    }

    def mergeMsg(a: Int, b: Int): Int = {
      math.max(a, b)
    }

    // 调用Pregel算法
    val resultGraph = initialGraph.pregel(
      initialMsg = Int.MinValue,
      maxIterations = 10,
      activeDirection = EdgeDirection.Out
    )(vprog, sendMsg, mergeMsg)

    resultGraph
  }
}
