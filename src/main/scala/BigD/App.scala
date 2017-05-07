package BigD
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
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
    val conf = new SparkConf().setAppName("BigD").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //creazione graf con oggetto
    var graph=builtGraphfromFile("data/graphGenOut0.dot")
    val frequentO=new FrequentSubG(graph,thr,size,sc)

    val frequentEdges: RDD[(String,String,String)]=frequentO.frequentEdges()
    //TODO da aggiungere assolutamente anche il DFSCODE sull'estensione dei candidati, sia simple che complex
    frequentEdges.collect().foreach(print(_))
    //--ROBA VECCHIA --
    //REDUCING COUPLES USING MAP REDUCE -> THEN FILTERING THEM
    //Potrebbe non servire
    //var keyCouples = graph.triplets.map(el => ((el.srcAttr, el.dstAttr), List((el.srcId.toString, el.dstId.toString))))
    //var reducedCouples = keyCouples.reduceByKey((nodo1, nodo2) => nodo1++nodo2)
    //couple reduced


    var candidateGen:RDD[MyGraph]=frequentO.candidateGeneration(frequentEdges)
    var candidate2:RDD[MyGraph]=frequentO.extension(candidateGen,frequentEdges)
    //frequentO.CSPMapReduce(graph,candidateGen.collect().head)
    //print(candidateGen.foreach(el=>))

    //main loop of the algorithm
    //construction of candidates
    //DFSCode
    //CSP


  }


  //Construct the graph from the file, no more with graphX but exploiting the new class vertexAfInput and MyGraphInput
  def builtGraphfromFile(fileName :String): MyGraphInput ={
    var graphIn:MyGraphInput=new MyGraphInput()
    var nodeS:VertexAFInput=null
    var nodeD:VertexAFInput=null
    val patternE=new Regex("[0-9]+ [\\[label=]")
    for(line <- scala.io.Source.fromFile("data/graphGenOut0.dot").getLines()){
      if(line!="digraph prova{" && line!="}"){ //se la linea è diversa da inizio e fine allora costruisci il grafo
        if(patternE.findFirstMatchIn(line)==None){
          var splitted=line.trim().split("[^0-9]+")
          //Nodo sorgente
          if (graphIn.keyPres(splitted(0))==false) { //non esiste nel grafo di input
            nodeS=new VertexAFInput(splitted(0),"label")
            graphIn.addNode(splitted(0),nodeS)
          }
          else
            nodeS=graphIn.getNode(splitted(0))

          //Nodo dest
          if (graphIn.keyPres(splitted(1))==false){//se false vuol dire che non esiste il nodo
            nodeD=new VertexAFInput(splitted(1),"label")
            graphIn.addNode(splitted(1),nodeD)
          }
          else
            nodeD=graphIn.getNode(splitted(1))

          nodeS.addEdge(nodeD,splitted(2))
        }
        else {
          var splitted = line.trim().split(" ")
          //var vertex = (splitted(0).toLong,(splitted(5)))
          nodeD=graphIn.getNode(splitted(0))
          nodeD.label=splitted(5)
        }
      }
    }
    return graphIn
  }



}


/*
*   def builtGraphfromFile(fileName :String,sc :SparkContext): Graph[String,String] ={
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
  }*/