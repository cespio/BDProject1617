package BigD
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}
import scala.util.matching.Regex


/**
 * @author ${francesco.contaldo, graphX tries}
 */
object App extends Serializable{

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
    val frequentO=new FrequentSubG(graph,thr,size)
    //var proova=sc.parallelize(frequentO)

    val frequentEdges: RDD[(String,String,String)]=frequentEdgesAPP(graph,sc,thr)
    frequentEdges.collect().foreach(println(_))
    //TODO da aggiungere assolutamente anche il DFSCODE sull'estensione dei candidati, sia simple che complex
    //frequentEdges.collect().foreach(print(_))
    //--ROBA VECCHIA --
    //REDUCING COUPLES USING MAP REDUCE -> THEN FILTERING THEM
    //Potrebbe non servire
    //var keyCouples = graph.triplets.map(el => ((el.srcAttr, el.dstAttr), List((el.srcId.toString, el.dstId.toString))))
    //var reducedCouples = keyCouples.reduceByKey((nodo1, nodo2) => nodo1++nodo2)
    //couple reduced

    //TODO aggiungere DFS code tra gli extension e verificare se esiste o meno
    var candidateGen:RDD[MyGraph]=frequentO.candidateGeneration(frequentEdges)
    //candidateGen.collect().foreach(el=>el.toPrinit())
    var candidate2:RDD[MyGraph]=frequentO.extension(candidateGen,frequentEdges)
    var arra:List[MyGraphInput]=List(graph)
    var arraRDD=sc.parallelize(arra)
    var ris=candidate2.map(el=> frequentO.CSPMapReduce(graph,el))
    ris.collect().foreach(el=>print(el))
    //candidate2.cartesian(arraRDD).foreach(el=> print(el))
    //prova.collect()
    //frequentO.CSPMapReduce(graph,candidateGen.collect().head)
    //print(candidateGen.foreach(el=>))

    //main loop of the algorithm
    //construction of candidates
    //DFSCode
    //CSP


  }
  def frequentEdgesAPP(graph:MyGraphInput,sc:SparkContext,thr:Int): RDD[(String, String, String)] = {
    //definisco una map come in datamining
    var frequent=scala.collection.mutable.HashMap.empty[(String,String,String),Int]
    for(k <- graph.nodes){
      for(el <- k.adjencies){
        var edge:(String,String,String)=(k.label,el._1.label,el._2)
        if(frequent.contains(edge)==false)
          frequent+=(edge->1)
        else{
          var n=frequent(edge)+1
          frequent.update(edge,n)
        }
      }
    }
    var frequentArr=frequent.filter(p=>p._2>=thr).keys.toArray.clone()
    var temp1=sc.parallelize(frequentArr)
    /*
    val temp = graph.triplets.map(tr => ((tr.srcAttr, tr.dstAttr, tr.attr), 1))
    val temp1 = temp.reduceByKey((a, b) => a + b).filter(el => (el._2.toInt >= thr && (el._1._1 != el._1._2))).map(el => el._1)*/
    return temp1
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
            graphIn.addNode(nodeS)
          }
          else
            nodeS=graphIn.getNode(splitted(0))

          //Nodo dest
          if (graphIn.keyPres(splitted(1))==false){//se false vuol dire che non esiste il nodo
            nodeD=new VertexAFInput(splitted(1),"label")
            graphIn.addNode(nodeD)
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

