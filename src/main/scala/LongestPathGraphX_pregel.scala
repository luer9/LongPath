import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.graphx.{Edge, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object LongestPathGraphX {
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    // 初始化Spark配置
    val conf = new SparkConf().setAppName("DAGLongestPath").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 构建图的顶点和边
    val vertices = Array.tabulate(3)(i => (i.toLong, i))
    val edges = Array(
      Edge(0L, 1L, 1), Edge(0L, 2L, 1), Edge(1L, 3L, 1), // 你需要根据实际图的结构定义边
      Edge(0L, 3L, 1), Edge(0L, 4L, 1), Edge(1L, 5L, 1),
      Edge(5L, 6L, 1), Edge(6L, 7L, 1), Edge(8L, 9L, 1)
      // ...
    )

    // 创建Graph对象
    val graph = Graph(sc.parallelize(vertices), sc.parallelize(edges))

    // 调用Pregel算法计算最长路径
    val resultGraph = pregel(graph)

    // 从结果图中提取最长路径长度
    val longestPathLength = resultGraph.vertices.map(_._2).reduce(Math.max)

    // 打印最长路径长度
    println("Longest Path Length: " + longestPathLength)

    // 停止SparkContext
    sc.stop()
  }

  def pregel(graph: Graph[Int, Int]): Graph[Int, Int] = {
    // 初始化所有顶点的属性为负无穷
    val initialGraph = graph.mapVertices((id, _)=> if (id == 0) 0 else Int.MinValue)

    // 定义Pregel迭代函数
    def vprog(vertexId: VertexId, value: Int, message: Int): Int = {
      math.max(value, message)
    }

    def sendMsg(triplet: org.apache.spark.graphx.EdgeTriplet[Int, Int]): Iterator[(VertexId, Int)] = {
      if (triplet.srcAttr + triplet.attr > triplet.dstAttr) {
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
      maxIterations = Int.MaxValue,
      activeDirection = EdgeDirection.Out
    )(vprog, sendMsg, mergeMsg)

    resultGraph
  }
}


//// 构建图的边
//val edges: RDD[Edge[Null]] = sc.parallelize(Seq(
//  Edge(1L, 2L, null),
//  Edge(1L, 3L, null),
//  Edge(2L, 4L, null),
//  Edge(2L, 5L, null),
//  Edge(3L, 4L, null),
//  Edge(4L, 5L, null),
//  Edge(4L, 6L, null),
//  Edge(5L, 6L, null)
//  // 添加更多边...
//))
//
//// 构建图
//val graph: Graph[Int, Null] = Graph.fromEdges(edges, defaultValue = 1)
//
//// 执行拓扑排序，检测是否有环
//topologySort(graph) match {
//  case Some(sortedVertices) =>
//    println(s"The topological order of the graph is: $sortedVertices")
//    // 计算最长路径
//    val longestPathGraph: Graph[Int, Null] = graph.pregel(
//      initialMsg = 0,
//      maxIterations = 20,
//      activeDirection = EdgeDirection.Out
//    )(
//      // 在顶点上发送消息的函数
//      (_, dist, newDist) => math.max(dist, newDist),
//      // 合并消息的函数
//      triplet => Iterator((triplet.dstId, triplet.srcAttr + 1)),
//      // 合并消息的函数
//      (a, b) => math.max(a, b)
//    )
//    // 输出最长路径长度
//    val maxLength: Int = longestPathGraph.vertices.map(_._2).max().toInt
//    println(s"The longest path in the graph is: $maxLength")
//
//  case None =>
//    println("The graph contains a cycle and is not a DAG.")
//}
//
//// 关闭 Spark 上下文
//sc.stop()

//  // 拓扑排序函数
//  def topologySort(graph: Graph[_, _]): Option[List[VertexId]] = {
//    var visited = Set[VertexId]()
//    var result: List[VertexId] = List()
//    var onStack = Set[VertexId]()
//
//    def visit(vertexId: VertexId): Option[List[VertexId]] = {
//      if (onStack.contains(vertexId)) {
//        // Detected a cycle, graph is not a DAG
//        None
//      } else if (!visited.contains(vertexId)) {
//        onStack += vertexId
//        visited += vertexId
//        graph.edges.filter(_.srcId == vertexId).foreach { edge =>
//          visit(edge.dstId) match {
//            case Some(path) => result = path ::: result
//            case None => return None // Propagate None if cycle is detected
//          }
//        }
//        onStack -= vertexId
//        result = vertexId :: result
//        Some(result)
//      } else {
//        None // Already visited, not part of current path
//      }
//    }
//
//    graph.vertices.foreach { case (vertexId, _) =>
//      if (!visited.contains(vertexId)) {
//        visit(vertexId) match {
//          case Some(path) => result = path ::: result
//          case None => return None // Propagate None if cycle is detected
//        }
//      }
//    }
//
//    Some(result.reverse)
//  }
//}









//import org.apache.log4j.{Level, Logger}
//import org.apache.poi.xssf.usermodel.XSSFWorkbook
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.SparkConf
//import org.apache.spark.graphx.{Graph, VertexId, Pregel}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.DataFrame
//import org.apache.poi.ss.usermodel._
//import org.apache.poi.xssf.usermodel.XSSFWorkbook
//
//import java.io.{File, FileInputStream}
//
//object LongestPathGraphX {
//  Logger.getLogger("org").setLevel(Level.WARN)
//  Logger.getLogger("akka").setLevel(Level.WARN)
//  // 初始化 Spark 配置
//  val conf = new SparkConf()
//    .setAppName("LongestPathGraphX")
//    .setMaster("local[*]")
//    .set("spark.driver.memory", "30G")
//    .set("spark.executor.memory", "20G")
//
//  val sc = new org.apache.spark.SparkContext(conf)
//  val sparkSession = SparkSession.builder().config(conf).getOrCreate()
//
//  val vpDir = "/Users/luyang/Documents/project/db/LongestPathGraphX/uobm20/VP"
//  val excelFilePath = "/Users/luyang/Documents/project/db/LongestPathGraphX/longPathUobm20.xlsx"
//  val doneFile = "/Users/luyang/Documents/project/db/LongestPathGraphX/done.txt" // 已经处理过的谓词
//  def main(args: Array[String]): Unit = {
////    // load 加载已经处理的谓词
////    val textData = sparkSession.read.text(doneFile)
////    val array = textData.collect().map(row => row.get(0).toString)
////    // load the existing Excel File
////    val fileIn = new FileInputStream(excelFilePath)
////    val workbook = new XSSFWorkbook(fileIn)
////    val sheet = workbook.getSheetAt(0)
////    val dir = new File(vpDir)
////
////    if(dir.exists() && dir.isDirectory()) {
////
////    }
//
//
//    // 构建图的顶点和边
//    val vertices: RDD[(VertexId, Long)] = sc.parallelize(Seq(
//      (1L, 1L),
//      (2L, 2L),
//      (3L, 3L),
//      (4L, 4L),
//      (5L, 5L),
//      (6L, 6L)
//    ))
//
//    val edges: RDD[org.apache.spark.graphx.Edge[Long]] = sc.parallelize(Seq(
//      org.apache.spark.graphx.Edge(1L, 2L, 1L),
//      org.apache.spark.graphx.Edge(2L, 3L, 1L),
//      org.apache.spark.graphx.Edge(3L, 6L, 1L),
//      org.apache.spark.graphx.Edge(6L, 5L, 1L),
//      org.apache.spark.graphx.Edge(5L, 4L, 1L),
//      org.apache.spark.graphx.Edge(4L, 1L, 1L),
//      org.apache.spark.graphx.Edge(5L, 2L, 1L)
//    ))
//
//    // 构建图
//    val graph = Graph(vertices, edges)
//    println("[Graph Done]")
//    // 定义 Pregel 迭代计算的初始消息
//    val initMsg: Long = 0L
//
//    // 定义 Pregel 迭代计算的计算逻辑
//    def dpUpdate(vertexId: VertexId, attr: Long, msg: Long): Long = {
//      msg + 1L
//    }
//
//    // 执行 Pregel 迭代计算
//    val longestPath = Pregel(graph, initMsg)(
//      (_, attr, msg) => math.max(attr, msg),
//      triplet => Iterator((triplet.dstId, dpUpdate(triplet.dstId, triplet.dstAttr, triplet.srcAttr))),
//      (a, b) => math.max(a, b)
//    )
//    println("[Pregel Done]")
//    // 打印最长路径长度
//    val maxLength = longestPath.vertices.map(_._2).max()
//    println(s"The length of the longest path in the graph is: $maxLength")
//
//    // 停止 Spark 上下文
//    sc.stop()
//  }
//
//  def getTriples(triplesFile: String): DataFrame = {
//    val triDF = sparkSession.read.parquet(triplesFile).toDF()
//    triDF.show(false)
//    triDF
//  }
//
//  // print pred info
//  def getPreds(predsFile: String): DataFrame = {
//    val predsDF = sparkSession.read.parquet(predsFile).toDF()
//    predsDF.show(false)
//    predsDF
//  }
//}
