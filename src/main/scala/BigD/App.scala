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

    var candidateGen:RDD[MyGraph]=frequentO.candidateGeneration(frequentEdges)
    //ELIMINAZIONE dei duplicati
    var dfsred=candidateGen.map(el=>(el.dfscode,el)).reduceByKey((a,b)=>a)


    //ELIMINAZIONE dei candidati
    var ite=2
    while(ite<=size) {
      var candidate2:RDD[MyGraph]=frequentO.extension(candidateGen,frequentEdges)
      var app=candidate2.map(el=>el.makeItUndirect())
      var dfsred1 = app.map(el => (el.dfscode, el)).reduceByKey((a, b) => a).map(el => el._2)
      var ris = dfsred1.map(el => (el.dfscode, graph, el, (frequentO.CSPMapReduce(graph, el)))).flatMap(el => el._4.map(a => (el._1, el._2, el._3, a))).map(kkk => (kkk._1, frequentO.checkGraph(kkk._3, kkk._4, kkk._2)))
      var ris2 = ris.reduceByKey((x, y) => x + y)
      var ris3 = ris2.filter(o=>o._2 > thr).map(a=>a._1)
      //candidateGen=dfsred1.filter(el => ris3.contains(el.dfscode))
      ite+=1
    }



  }
  def frequentEdgesAPP(graph:MyGraphInput,sc:SparkContext,thr:Int): RDD[(String, String, String)] = {
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
