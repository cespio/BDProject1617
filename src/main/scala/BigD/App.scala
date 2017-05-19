package BigD
import org.apache.log4j.Level
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}
import scala.util.matching.Regex

import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * @author ${francesco.contaldo, graphX tries}
 */
object App extends Serializable{

  def main(args : Array[String]) {
    /*SparkConfiguration*/
    val conf = new SparkConf().setAppName("BigD").setMaster("local[4]")
    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    /*Acquiring the input*/
    println("Give me the threshold -> ")
    val thr=Console.readInt()
    println("Give me the size threshold-> ")
    val size=Console.readInt()
    /*Creating the graph from the input dot file*/
    var graph=builtGraphfromFile("data/graphGenOut0.dot")
    val frequentO=new FrequentSubG(graph,thr,size)
    /*RDD containing the frequentedges*/
    val frequentEdges: RDD[(String,String,String)]=frequentEdgesAPP(graph,sc,thr)
    println("FREQUENT EDGES "+frequentEdges.count())
    /*First step generate the candidate with size at least two edges*/
    var candidateGen:RDD[MyGraph]=frequentO.candidateGeneration(frequentEdges)

    /*Clearing the one with the double DFSCode*/


    candidateGen=candidateGen.map(el=>(el.dfscode,el)).reduceByKey((a,b)=>a).map(el=>el._2)
    println("LENG "+candidateGen.count())
    /*var to=candidateGen.collect()
    var total=0
    for(el1 <- to) {
      total=0
      var r = frequentO.CSPMapReduce(graph, el1)

      print("\n")
      for (el <- r) {

        var a = frequentO.checkGraph(el1, el, graph)
        /*if(a==1){
          println("Assegnmaneto " + el)
        }*/
        total+=a
        //println("Ris " + a)
      }
      if(total>=thr){
        el1.toPrinit()
      }
    }*/
    var ris1=candidateGen.map(el => (el.dfscode, graph, el, (frequentO.CSPMapReduce(graph, el)))).flatMap(el => el._4.map(a => (el._1, el._2, el._3, a))).map(el => (el._1, frequentO.checkGraph(el._3, el._4, el._2)))
    ris1=ris1.reduceByKey((x, y) => x + y).filter(el=>el._2>= thr)
    /*Updating the new candiateGen*/
    candidateGen=candidateGen.map(el => (el.dfscode, el)).join(ris1).map(ris => ris._2._1)
    println("dopo csp primo -> "+candidateGen.count())
    candidateGen.foreach(el => println(el.dfscode))
    //candidateGen.foreach(el=>el.toPrinit())
    /*Number of iterations at least two*/
    var itera=2
    var candidate2:RDD[MyGraph]=null
    var candidatePre:RDD[MyGraph]=candidateGen /*Where the result will be stored*/
    frequentEdges.cache()
    while(itera<size && candidatePre.count()!=0){
      println("Entro nel cicloo\n")
      candidate2=frequentO.extension(candidateGen,frequentEdges)
      /*Adding the DFSCode and removing the duplicate*/
      candidate2=candidate2.map(el=>el.makeItUndirect()).map(el=>(el.dfscode,el)).reduceByKey((a,b)=>a).map(el=>el._2)
      println("INTEREMEDIA lunghezza "+candidate2.count())
      ris1=candidate2.map(el => (el.dfscode, graph, el, (frequentO.CSPMapReduce(graph, el)))).flatMap(el => el._4.map(a => (el._1, el._2, el._3, a))).map(el => (el._1, frequentO.checkGraph(el._3, el._4, el._2)))
      ris1=ris1.reduceByKey((x, y) => x + y).filter(el=>el._2>= thr)
      candidatePre=candidate2.map(el => (el.dfscode, el)).join(ris1).map(ris => ris._2._1)
      if(candidatePre.count()!=0)
        candidateGen=candidatePre
      itera=itera+1
      print("ITERA "+itera)
    }

    println("Quello che abbiamo ottenuto dopo"+itera+" iterazioni")
    //candidateGen.collect().foreach(el=>{println(el.dfscode);el.toPrinit()})
    println("NUMBER OF SOLUTION -> "+candidateGen.count())


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
    for(line <- scala.io.Source.fromFile("data/graphGenOut.dot").getLines()){
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
