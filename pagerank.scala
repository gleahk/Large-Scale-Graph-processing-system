import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.lib.PageRank


val inputFile = "/Users/varun_nuthakki/Desktop/amazon0302.txt/"

// Load the edges as a graph
val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, inputFile)


// Run PageRank for 10 iterations
val numIterations = 10
val pageRankGraph = PageRank.run(graph, numIterations)

// Get the top 10 vertices with the highest PageRank
val top10 = pageRankGraph.vertices.top(10)(Ordering.by(_._2))

// Print the results
println("Top 10 vertices by PageRank:")
top10.foreach { case (id, rank) => println(s"$id\t$rank") }