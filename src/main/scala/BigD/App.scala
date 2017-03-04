package BigD
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex


/**
 * @author ${francesco.contaldo, graphX tries}
 */
object App {

  def main(args : Array[String]) {
    //val scanner=new Scanner(System.in)
    println("Give me the thr -> ")
    val thr=Console.readInt()
    println("Give me the sizethr -> ")
    val size=Console.readInt()
    //val conf = new SparkConf().setAppName("Simple Application")
    val conf = new SparkConf().setAppName("BigD").setMaster("local[4]")
    val sc = new SparkContext(conf)
    var graph=builtGraphfromFile("data/graphGenOut0.dot",sc)
    val frequentO=new FrequentSubG(graph,thr,size)
    //frequentEdges(graph,thr)
    val frequentEdges: RDD[(String,String,String)]=frequentO.frequentEdges()
    frequentO.candidateGeneration(frequentEdges)
    //main loop of the algorithm
    //construction of candidates
    //DFSCode
    //CSP


  }
  //Construction of the candidate




  //Construct the graph from the file
  def builtGraphfromFile(fileName :String,sc :SparkContext): Graph[String,String] ={
    var vertices=Array[(VertexId,(String))]()
    var edges=Array[Edge[String]]()
    val patternE=new Regex("[0-9]+ [\\[label=]")
    for(line <- scala.io.Source.fromFile("data/graphGenOut0.dot").getLines()){
      if(line!="digraph prova{" && line!="}"){ //se la linea è diversa da inizio e fine allora costruisci il grafo
        if(patternE.findFirstMatchIn(line)==None){
          var splitted=line.trim().split("[^0-9]+")
          var edge=Edge(splitted(0).toLong,splitted(1).toLong,splitted(2))
          edges=edges:+edge
        }
        else {
          var splitted = line.trim().split(" ")
          var vertex = (splitted(0).toLong,(splitted(5)))
          vertices = vertices :+ vertex
        }
      }
    }

    var vRDD=sc.parallelize(vertices)
    var eRDD=sc.parallelize(edges)
    val graph=Graph(vRDD,eRDD)
    return graph
  }



}
