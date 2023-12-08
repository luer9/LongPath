import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object LongestPathGraphX {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val vertices = List(1, 2, 3, 4, 5, 6) // 顶点列表
    val edges = List((1, 2), (1, 3), (2, 4), (2, 5), (3, 4), (4, 5), (4, 6), (5, 6), (6, 3)) // 边列表，表示有向边

    val adjacencyList = Array.fill(vertices.length)(ListBuffer[Int]()) // 邻接表，存储顶点的邻接顶点

    // 构建邻接表
    for ((src, dst) <- edges) {
      adjacencyList(src - 1).append(dst)
    }

    val topologicalOrder = topologicalSort(adjacencyList) // 拓扑排序
    val longestPaths = Array.fill(vertices.length)(0) // 最长路径数组

    // 动态规划计算最长路径
    for (v <- topologicalOrder) {
      val vIndex = v - 1
      for (u <- adjacencyList(vIndex)) {
        val uIndex = u - 1
        longestPaths(uIndex) = math.max(longestPaths(uIndex), longestPaths(vIndex) + 1)
      }
    }

    // 找到最长路径长度
    val maxPathLength = longestPaths.max

    println(s"The longest path length in the DAG is: $maxPathLength")
  }

  // 拓扑排序
  def topologicalSort(adjacencyList: Array[ListBuffer[Int]]): List[Int] = {
    val visited = Array.fill(adjacencyList.length)(false)
    val stack = ListBuffer[Int]()

    def dfs(v: Int): Unit = {
      visited(v - 1) = true

      for (u <- adjacencyList(v - 1)) {
        if (!visited(u - 1)) {
          dfs(u)
        }
      }

      stack.prepend(v)
    }

    for (v <- adjacencyList.indices) {
      if (!visited(v)) {
        dfs(v + 1)
      }
    }

    stack.toList
  }
}

