import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


val inputFile = "/Users/varun_nuthakki/Desktop/amazon0302.txt/"

// Load the edges as a graph
val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, inputFile)
println(s"Number of vertices: ${graph.vertices.count()}")
println(s"Number of edges: ${graph.edges.count()}")

def findNeighbors(vertexId: VertexId): Seq[VertexId] = {
  graph.edges.filter(e => e.srcId == vertexId).map(_.dstId).collect()
}


val vertexId = 12345L
val neighbors = findNeighbors(vertexId)
println(s"Neighbors of $vertexId: $neighbors")


val triangleCountGraph = graph.triangleCount()
val numTriangles = triangleCountGraph.vertices.map{ case (vid, count) => count }.reduce(_ + _) / 3
println(s"Number of triangles in the graph: $numTriangles")

val degrees = graph.degrees
val maxDegreeVertex = degrees.reduce((a, b) => if (a._2 > b._2) a else b)
val maxDegree = maxDegreeVertex._2
val maxDegreeNodeId = maxDegreeVertex._1
println(s"The node with ID $maxDegreeNodeId has the highest degree of $maxDegree")


import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId

// assume graph is already loaded

val src: VertexId = 0L
val dst: VertexId = 15L

// set the initial distance to infinity, except for the source vertex
val initialGraph = graph.mapVertices((id, _) => if (id == src) 0.0 else Double.PositiveInfinity)

// send the message along the edges to update the distances
val messages = initialGraph.pregel(Double.PositiveInfinity)(
  (id, dist, newDist) => math.min(dist, newDist), // update function
  triplet => {  // send message function
    if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
      Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
    } else {
      Iterator.empty
    }
  },
  (a, b) => math.min(a, b) // merge function
)

// get the shortest path
val shortestPath = messages.vertices.filter(_._1 == dst).first()._2

println(s"Shortest path between $src and $dst: $shortestPath")