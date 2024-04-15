import scala.io.StdIn.readLine
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession
import java.lang.management.ManagementFactory
import java.lang.management.ThreadMXBean
import java.nio.file.{Files, Paths}

val threadBean: ThreadMXBean = ManagementFactory.getThreadMXBean()
val starttime = System.nanoTime()
val startCpuTime: Long = threadBean.getCurrentThreadCpuTime()
val graphPath = "/Users/varun_nuthakki/Desktop/amazon0302.txt/"
val disksUtilized = Files.getFileStore(Paths.get(graphPath)).getTotalSpace()

val graph = GraphLoader.edgeListFile(sc, "/Users/varun_nuthakki/Desktop/amazon0302.txt/")
val endtime = System.nanoTime()
val elapsedCpuTime: Long = threadBean.getCurrentThreadCpuTime() - startCpuTime
val elapsed = (endtime-starttime)/1e9
val cpuUtilization: Double = elapsedCpuTime.toDouble/1e9 / elapsed * 100
println(s"The graph file is utilizing $disksUtilized disk(s)")
println(s"ElapsedTime for loading graph: $elapsed")
println(s"CPU utilization: $cpuUtilization%")
println(s"The graph file is utilizing $disksUtilized disk(s)"); //semicolon added here to terminate the line
