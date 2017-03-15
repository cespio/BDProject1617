package BigD

import org.apache.spark.rdd.RDD



/**
  * Created by francesco on 02/03/17.
  */
class FrequentSubG (graph_arg: org.apache.spark.graphx.Graph[String,String],thr_arg:Int,size_arg:Int) extends Serializable {
  var graph: org.apache.spark.graphx.Graph[String, String] = graph_arg
  val thr = thr_arg
  val size = size_arg

  //function do define
  //frequent edges
  def frequentEdges(): RDD[(String, String, String)] = {
    //si potrebbe applicare anche qui un principio di map reduce
    val temp = graph.triplets.map(tr => ((tr.srcAttr, tr.dstAttr, tr.attr), 1))
    val temp1 = temp.reduceByKey((a, b) => a + b).filter(el => (el._2.toInt >= thr && (el._1._1 != el._1._2))).map(el => el._1)
    return temp1
  }

  //constructor of candidates
  //CSP

  def candidateGeneration(freQE: RDD[(String, String, String)]) = {
    val temp1 = freQE.cartesian(freQE).filter(el => el._1 != el._2 && boolCondition(el._1, el._2) && Math.abs(el._1._3.toInt - el._2._3.toInt) <= 4)
    val temp2 = temp1.map(el => constructTheGraph(el)).map(el => makeItUndirect(el))
    temp2.collect().foreach(el => el.toPrinit())
  }

  def boolCondition(arc1: (String, String, String), arc2: (String, String, String)): Boolean = {
    var ret = false
    if ((arc1._1 == arc2._2 && arc1._2 != arc2._1) || (arc1._2 == arc2._1 && arc1._1 != arc2._2) || (arc1._1 == arc2._1 && arc1._2 != arc2._2) || (arc1._2 == arc2._2 && arc1._1 != arc2._1)) {
      ret = true
    }
    return ret
  }

  def constructTheGraph(couple: ((String, String, String), (String, String, String))): MyGraph = {
    var G = new MyGraph()
    if ((couple._1._1 == couple._2._2) && (couple._1._2 == couple._2._1) && (couple._1._1 != couple._1._2) && (couple._2._1 != couple._2._2)) {
      //cycle
      var V1 = new VertexAF(couple._1._1)
      var V2 = new VertexAF(couple._2._2)
      G = new MyGraph()
      V1.addEdge(V2, couple._1._3)
      V2.addEdge(V1, couple._2._3)
      G.addNode(V1)
      G.addNode(V2)

    }
    if ((couple._1._2 == couple._2._2) && (couple._1._1 != couple._1._2) && (couple._1._2 != couple._2._1) && (couple._1._1 != couple._2._1) && (couple._1._1 != couple._2._2)) {
      var V0 = new VertexAF(couple._2._1)
      var V1 = new VertexAF(couple._1._1)
      var V2 = new VertexAF(couple._1._2)
      G = new MyGraph()
      V0.addEdge(V1, couple._2._3)
      V1.addEdge(V2, couple._1._3)
      G.addNode(V0)
      G.addNode(V1)
      G.addNode(V2)
    }
    if ((couple._1._1 == couple._2._2) && (couple._1._1 != couple._1._2) && (couple._1._1 != couple._2._1) && (couple._1._1 !=  couple._2._1) && (couple._1._2 !=  couple._2._2)){
      var V0 = new VertexAF(couple._2._1)
      var V1 = new VertexAF(couple._1._1)
      var V2 = new VertexAF(couple._1._2)
      G = new MyGraph()
      V0.addEdge(V1,couple._2._3)
      V1.addEdge(V2,couple._1._3)
      G.addNode(V0)
      G.addNode(V1)
      G.addNode(V2)

    }
    if ((couple._1._1 == couple._2._1) && (couple._1._1 != couple._1._2) && (couple._1._1 != couple._2._2) && (couple._1._2 != couple._2._1) && (couple._1._2 != couple._2._2) ){
      var V0 = new VertexAF(couple._2._1)
      var V1 = new VertexAF(couple._1._1)
      var V2 = new VertexAF(couple._1._2)
      var G = new MyGraph()
      V0.addEdge(V1,couple._1._3)
      V1.addEdge(V2,couple._2._3)
      G.addNode(V0)
      G.addNode(V1)
      G.addNode(V2)
    }
    return G
  }

  def makeItUndirect(inGraph:MyGraph): MyGraph={
    /*E' possibile che in input arrivino dei grafi vuoti -> nella creazione vengono creati prima*/
   // println("Grafo orientato ")
    //inGraph.toPrinit()
    var un_G = new MyGraph();
    //println("Bella")
    var S:VertexAF=null;
    var D:VertexAF=null;
    var nodes=inGraph.nodes
    for(el <- nodes){ /*mi prendo i nodi, poi per ogni nodo mi prendo un arco*/
      //println("Esamindando "+el.vid)
      un_G.toPrinit()
      if(un_G.nodes.count(f => f.vid == el.vid)>=1){
        S=un_G.nodes.filter(f => f.vid==el.vid).head
        //println("Trovato S -> "+el.vid)
      }
      else {
        S = new VertexAF(el.vid)
        //println("Creo S -> "+el.vid)
        un_G.addNode(S)
      }
      for(el1 <- el.adjencies){ /*occhio caso cicli*/
        //println("Trovo "+el1._1.vid)
        if(un_G.nodes.count(f => f.vid == el1._1.vid)>=1){
          D=un_G.nodes.filter(f => f.vid==el1._1.vid).head
          //println("Trovato D -> "+el1._1.vid)
        }
        else {
          D = new VertexAF(el1._1.vid)
          //println("Creo D -> " + el1._1.vid)
          un_G.addNode(D)
        }
        if(S!=null && D!=null){
          S.addEdge(D, el1._2)
          D.addEdge(S, el1._2)
        }
      }
    }
    return un_G
  }
}
