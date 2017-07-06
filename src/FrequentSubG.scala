import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.control.Breaks._


/*
*  BigData project 2017, Frequent Pattern Mining on Labeled Oriented Single Graph
*  @authors { Francesco Contaldo, Alessandro Rizzuto}
*
* */

//Class used to compute the Frequent Pattern functions
class FrequentSubG (graph_arg:MyGraphInput,thr_arg:Int,size_arg:Int) extends Serializable{
  var graph: MyGraphInput = graph_arg
  val thr = thr_arg
  val size = size_arg

  //Generation of the simplest candidates
  def candidateGeneration(freQE: RDD[(String, String, String)]):RDD[MyGraph]={
    var temp1=freQE.cartesian(freQE).filter(el => compare(el._1,el._2)).filter(el=> Math.abs(el._1._3.toInt - el._2._3.toInt) <= 4)
    var temp2 = temp1.flatMap(el => constructTheGraph(el)).map(el => el.makeItUndirect())
    return temp2
  }

  def makekey(el:((String,String,String),(String,String,String))):String= {
    var a = el._1._1 ++ el._1._2 ++ el._1._3 ++ el._2._1 ++ el._2._2 ++ el._2._3
    return a.sorted
  }

  //Function used to compute the triangularisation of the cartesian product
  def compare(fst:(String,String,String),snd:(String,String,String)):Boolean={
    var a=fst._1+fst._2+fst._3
    var b=snd._1+snd._2+snd._3
    return a <= b
  }

  def boolCondition(arc1: (String, String, String), arc2: (String, String, String)): Boolean = {
    var ret = false
    if ((arc1._1 == arc2._2 && arc1._2 != arc2._1) || (arc1._2 == arc2._1 && arc1._1 != arc2._2) || (arc1._1 == arc2._1 && arc1._2 != arc2._2) || (arc1._2 == arc2._2 && arc1._1 != arc2._1)) {
      ret = true
    }
    return ret
  }


  //Given two edges, it generates all the possible candidate subgraphs
  def constructTheGraph(couple: ((String, String, String), (String, String, String))): mutable.MutableList[MyGraph] = {
    var listRis: mutable.MutableList[MyGraph] = mutable.MutableList.empty[MyGraph]

    if (couple._1._1!=couple._1._2 && couple._2._1!=couple._2._2 && (couple._1._1 == couple._2._2) && (couple._1._2 == couple._2._1) && (couple._1._1 != couple._1._2) && (couple._2._1 != couple._2._2)) {
      var V1 = new VertexAF(couple._1._1)
      var V2 = new VertexAF(couple._1._2)
      var G = new MyGraph()
      V1.addEdge(V2, couple._1._3)
      V2.addEdge(V1, couple._2._3)
      G.addNode(V1)
      G.addNode(V2)
      G.maxH=Math.max(couple._1._3.toInt,couple._2._3.toInt)
      G.minH=Math.min(couple._1._3.toInt,couple._2._3.toInt)
      listRis +=G

    }
    if (couple._1._1!=couple._1._2 && couple._2._1!=couple._2._2 && (couple._1._2 == couple._2._2) && (couple._1._1 != couple._1._2) && (couple._1._2 != couple._2._1) && (couple._1._1 != couple._2._1) && (couple._1._1 != couple._2._2)) {
      var V0 = new VertexAF(couple._2._1)
      var V1 = new VertexAF(couple._1._1)
      var V2 = new VertexAF(couple._1._2)
      var G = new MyGraph()
      V0.addEdge(V2, couple._2._3)
      V1.addEdge(V2, couple._1._3)
      G.addNode(V0)
      G.addNode(V1)
      G.addNode(V2)
      G.maxH=Math.max(couple._1._3.toInt,couple._2._3.toInt)
      G.minH=Math.min(couple._1._3.toInt,couple._2._3.toInt)
      listRis += (G)
    }

    if (couple._1._1!=couple._1._2 && couple._2._1!=couple._2._2 && (couple._1._1 == couple._2._2) && (couple._1._1 != couple._1._2) && (couple._1._1 != couple._2._1) && (couple._1._2 != couple._2._1) && (couple._1._2 != couple._2._2)) {
      var V0 = new VertexAF(couple._2._1)
      var V1 = new VertexAF(couple._1._1)
      var V2 = new VertexAF(couple._1._2)
      var G = new MyGraph()
      V0.addEdge(V1, couple._2._3)
      V1.addEdge(V2, couple._1._3)
      G.addNode(V0)
      G.addNode(V1)
      G.addNode(V2)
      G.maxH=Math.max(couple._1._3.toInt,couple._2._3.toInt)
      G.minH=Math.min(couple._1._3.toInt,couple._2._3.toInt)
      listRis += (G)

    }

    if (couple._1._1!=couple._1._2 && couple._2._1!=couple._2._2 && (couple._1._1 == couple._2._1) && (couple._1._1 != couple._1._2) && (couple._1._1 != couple._2._2) && (couple._1._2 != couple._2._1) && (couple._1._2 != couple._2._2)) {
      var V0 = new VertexAF(couple._1._1)
      var V1 = new VertexAF(couple._1._2)
      var V2 = new VertexAF(couple._2._2)
      var G = new MyGraph()
      V0.addEdge(V1, couple._1._3)
      V0.addEdge(V2, couple._2._3)
      G.addNode(V0)
      G.addNode(V1)
      G.addNode(V2)
      G.maxH=Math.max(couple._1._3.toInt,couple._2._3.toInt)
      G.minH=Math.min(couple._1._3.toInt,couple._2._3.toInt)
      listRis += (G)
    }
    return listRis
  }

  //Exstension of a Graph given a single edge
  def newExtension(candidate: RDD[MyGraph], freqE: RDD[(String, String, String)]): RDD[MyGraph] = {
    var toExtend=candidate.cartesian(freqE).map(el => complexExt(el._1,el._2)).filter(el => el._1==1).map(el => el._2)
    return toExtend
  }


  def complexExt(cand:MyGraph,edge:(String,String,String)): (Int,MyGraph) ={
    var candidate=cand.myclone() //Clone of the candidate
    candidate.dfscode=cand.dfscode
    var hourPass=0
    var flag=0
    var nEdge=edge._3.toInt
    var sor:mutable.MutableList[VertexAF]=candidate.nodes.filter(el=> el.vid==edge._1) //Used to check the presence of the nodes in the graph
    var des:mutable.MutableList[VertexAF]=candidate.nodes.filter(el=> el.vid==edge._2) //Used to check the presence of the nodes in the graph

    //Hour test -> wether the new edge is in the range or it can be extended
    if((candidate.maxH-candidate.minH)==4){
      if(nEdge<=candidate.maxH && nEdge >=candidate.minH){
        hourPass=1
      }
    }
    else{
      if(nEdge< candidate.minH && (candidate.maxH-nEdge)<=4){
        hourPass=1
        candidate.minH=nEdge
      }
      else{
        if(nEdge>candidate.maxH && (nEdge-candidate.minH)<=4){
          hourPass=1
          candidate.maxH=nEdge
        }
      }

    }
    if(hourPass==1) {
      if (sor.length == 1 && des.length == 0 && flag==0) { //source matched and destination doesn't
        flag = 1
        var toadd = new VertexAF(edge._2)
        var nodo = sor.head
        nodo.addEdge(toadd,edge._3)
        candidate.addNode(toadd)
      }

      if (sor.length == 0 && des.length == 1 && flag==0) { //dest matched and source doesn't
        flag = 1
        var toadd = new VertexAF(edge._1)
        var nodo = des.head
        toadd.addEdge(nodo, edge._3)
        candidate.addNode(toadd)
      }

      if (sor.length == 1 && des.length == 1 && flag==0) { //both matched

        //before check if the two nodes are already linked
        if (sor.head.adjencies.count(el => el._1.vid == des.head.vid) == 0) {
          flag = 1
          sor.head.addEdge(des.head, edge._3)
        }
      }
    }
    return (flag,candidate)
  }



  def CSPMapReduce(inputGraph: MyGraphInput, toVerify: MyGraph):mutable.MutableList[List[String]]={

    var domainCoup=mutable.MutableList.empty[mutable.MutableList[(String,String)]]
    var domainLabel=mutable.MutableList.empty[List[String]]
    var temporalDom=scala.collection.mutable.HashMap.empty[String,mutable.MutableList[String]]
    var couples = toVerify.allCouples() //return all the couples present in the candidate graph
    //For each time the two label are "fresh",rescue the domains from the input graph: if at least one of them has been
    // already seen, uses the domain retrieved in the next iterations
    for(el <- couples){
      if(!temporalDom.contains(el._1) && !temporalDom.contains(el._2)) {
        var ris=inputGraph.retrevieTwo(el._1,inputGraph.nodes.filter(n=>n.label==el._1).map(n=>n.vid),el._2, inputGraph.nodes.filter(n=>n.label==el._2).map(n=>n.vid),el._3)
        temporalDom += (el._1 -> ris.filter(p => p._1 == el._1).map(p => p._2).distinct)
        temporalDom += (el._2 -> ris.filter(p => p._1 == el._2).map(p => p._2).distinct)
      }
      else{
        if(temporalDom.contains(el._1) && !temporalDom.contains(el._2)){
          var ris=inputGraph.retrevieTwo(el._1,temporalDom(el._1),el._2,inputGraph.nodes.filter(n=>n.label==el._2).map(n=>n.vid),el._3)
          temporalDom += (el._1 -> ris.filter(p => p._1 == el._1).map(p => p._2).distinct)
          temporalDom += (el._2 -> ris.filter(p => p._1 == el._2).map(p => p._2).distinct)

        }
        else{
          if(!temporalDom.contains(el._1) && temporalDom.contains(el._2)){
            var ris=inputGraph.retrevieTwo(el._1,inputGraph.nodes.filter(n=>n.label==el._1).map(n=>n.vid),el._2,temporalDom(el._2),el._3)
            temporalDom += (el._1 -> ris.filter(p => p._1 == el._1).map(p => p._2).distinct)
            temporalDom += (el._2 -> ris.filter(p => p._1 == el._2).map(p => p._2).distinct)

          }
          else{
            var ris=inputGraph.retrevieTwo(el._1,temporalDom(el._1),el._2,temporalDom(el._2),el._3)
            temporalDom += (el._1 -> ris.filter(p => p._1 == el._1).map(p => p._2).distinct)
            temporalDom += (el._2 -> ris.filter(p => p._1 == el._2).map(p => p._2).distinct)
          }
        }
      }
    }
    var i=0
    //create a cartesian product among all possible domain
    for(el <- toVerify.nodes){
      if(i==0) {
        domainLabel = temporalDom(el.vid).map(ed => List(ed))
      }
      else{
        var tmp1=temporalDom(el.vid)
        var tmp2=domainLabel.flatMap(l1 => tmp1.map(a => l1++List(a)))
        domainLabel=tmp2
      }
      i=1
    }
    return domainLabel.map(el=>el.distinct).distinct

  }

  //takes as input the input graph, the domain, and the candidate
  //and check if the possible domain can resolve the CSP problem
  def checkGraph(toVerify:MyGraph, dom:List[String], input: MyGraphInput):Int={

    if(dom.length!=toVerify.nodes.length){
      return  0
    }
    for(el <- toVerify.nodes){
      var index1=toVerify.nodes.indexOf(el)
      var n1=dom(index1)
      for(nested <- el.adjencies){
        var index2=toVerify.nodes.indexOf(toVerify.nodes.filter(el1 => el1.vid==nested._1.vid).head)
        var n2=dom(index2)
        var rit=input.edgeBool(dom(index1),dom(index2),nested._2)
        //if at least one of the edges is missing, returns 0
        if(!rit){
          return 0
        }
      }
    }
    //otherwise count one
    return 1
  }
}

/*Candidate Generation complex*/
/*def extension(candidate: RDD[MyGraph], freqE: RDD[(String, String, String)]): RDD[MyGraph] = {

  def matching(el: (MyGraph, (String, String, String))): Boolean = {
    var edge = el._2
    for (i <- el._1.nodes) yield {
      for (j <- i.adjencies) yield {
        if ((i.vid == edge._1) || (i.vid == edge._2) || (j._1.vid == edge._1) || (j._1.vid == edge._2)) {
          return true
        }
      }
    }
    return false
  }

  def labeling(el: (MyGraph, (String, String, String))): Boolean = {
    for (i <- el._1.nodes) yield {
      if ((i.vid == el._2._1) && (el._2._1 != el._2._2)) {
        if ((i.vid == el._2._2))
          return false
      }

      if ((i.vid == el._2._2) && (el._2._1 != el._2._2)) {
        if (i.vid == el._2._1)
          return false
      }

      for (j <- i.adjencies) {
        if ((j._1.vid == el._2._1) && (el._2._1 != el._2._2)) {
          if ((j._1.vid == el._2._2) || (i.vid == el._2._2))
            return false
        }

        if ((j._1.vid == el._2._2) && (el._2._1 != el._2._2)) {
          if ((j._1.vid == el._2._1) || (i.vid == el._2._1))
            return false
        }
      }
    }
    return true
  }

  def categorize(el: (MyGraph, (String, String, String))): (MyGraph, (String, String, String), Int) = {
    var temp = 0
    for (i <- el._1.nodes) {
      if (i.vid == el._2._1)
        temp = 1
      if (i.vid == el._2._2)
        temp = 2
      if (temp == 1) {
        if (i.vid == el._2._2)
          temp = 3
      }
      if (temp == 2) {
        if (i.vid == el._2._1)
          temp = 4
      }

      for (j <- i.adjencies) {
        if (j._1.vid == el._2._1)
          temp = 1
        if (j._1.vid == el._2._2)
          temp = 2
        if (temp == 1) {
          if (j._1.vid == el._2._2)
            temp = 3
        }
        if (temp == 2) {
          if (j._1.vid == el._2._1)
            temp = 4
        }
      }
    }
    (el._1, el._2, temp)
  }


  def hourGap(el: (MyGraph, (String, String, String))): Boolean = {
    var min: Int = Int.MaxValue
    var max: Int = Int.MinValue

    for (i <- el._1.nodes) yield {
      for (j <- i.adjencies) {

        if ((j._2.toInt) < min && (j._2 != null))
          min = j._2.toInt

        if (j._2.toInt > max && (j._2 != null))
          max = j._2.toInt
      }
    }

    if (el._2._3.toInt < min)
      min = el._2._3.toInt
    if (el._2._3.toInt > max)
      max = el._2._3.toInt

    if (max - min <= 4)
      return true
    else
      return false
  }

  var filteredFE=freqE.filter(el => el._1!=el._2)
  var partialGraphs = candidate.cartesian(filteredFE).filter(el => matching(el)).filter(el => labeling(el)).filter(el => hourGap(el)).map(el => categorize(el))
  println("LUNGHEZZA partial graph "+partialGraphs.count())
  var toExtend = partialGraphs.flatMap(el => extendTheGraph(el))
  println("ZIONE -> "+toExtend.count())

  //partialGraphs.collect().map(el=>println(el._1.visualRoot(), el._2, el._3))
  return toExtend

}

def extendTheGraph(triplet: (MyGraph, (String, String, String), Int)): mutable.MutableList[MyGraph] = {
  var listR: mutable.MutableList[MyGraph]=mutable.MutableList.empty[MyGraph]
  println("----------------------")
  println("PRIMA ESTENSIONE GRAFO")
  println("----------------------")
  println(clone.toPrinit(), triplet._2)

  if (triplet._3 == 1) {
    var clone: MyGraph = triplet._1
    breakable {
      for (i <- clone.nodes) {
        if (i.vid == triplet._2._1) {
          var newVertex = new VertexAF(triplet._2._2)
          i.addEdge(newVertex, triplet._2._3)
          clone.addNode(newVertex)
         // println("Caso 1, sorg = "+i.vid+", dest = "+triplet._2._2+", tripla = "+triplet._2+", DFS = "+clone.dfscode)
          listR:+=clone
          break
        }
      }
    }

  }

  if (triplet._3 == 2) {
    var clone: MyGraph = triplet._1
    breakable {
      for (i <- clone.nodes) {
        if (i.vid == triplet._2._2) {
          var newVertex = new VertexAF(triplet._2._1)
          clone.addNode(newVertex)
          newVertex.addEdge(i, triplet._2._3)
          //println("Caso 2, sorg = "+i.vid+", dest = "+triplet._2._2+", tripla = "+triplet._2+", DFS = "+clone.dfscode)
          listR:+=clone
          break
        }
      }
    }
  }

  if (triplet._3 == 3) {
    var clone: MyGraph = triplet._1
    breakable {
      for (i <- clone.nodes) {
        if (i.vid == triplet._2._1) {
          for (j <- i.adjencies) {
            if (j._1.vid == triplet._2._2) {
              i.addEdge(j._1, triplet._2._3)
             // println("Caso 3, sorg = "+i.vid+", dest = "+triplet._2._2+", tripla = "+triplet._2+", DFS = "+clone.dfscode)
              listR:+=clone
              break
            }
          }
        }
      }
    }
  }

  if (triplet._3 == 4) {
    var clone: MyGraph = triplet._1
    breakable {
      for (i <- clone.nodes) {
        if (i.vid == triplet._2._2) {
          for (j <- i.adjencies) {
            if (j._1.vid == triplet._2._1) {
              j._1.addEdge(i, triplet._2._3)
            //  println("Caso 1, sorg = "+i.vid+", dest = "+triplet._2._2+", tripla = "+triplet._2+", DFS = "+clone.dfscode)
              listR:+=clone
              break
            }
          }
        }
      }
    }
  }

  println("-----------------------")
  println("DOPO L'ESTENSIONE GRAFO")
  println("-----------------------")
  println(clone.toPrinit())
  return listR
}*/
