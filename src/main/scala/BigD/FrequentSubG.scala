package BigD

import org.apache.spark.rdd.RDD

import scala.collection.mutable



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
    val temp1 = freQE.cartesian(freQE).filter(el => boolCondition(el._1, el._2) && Math.abs(el._1._3.toInt - el._2._3.toInt) <= 4)
    val temp2 = temp1.flatMap(el => constructTheGraph(el)).filter(el => el.nodes.length>0).map(el => makeItUndirect(el))
    temp2.collect()
    //Possibile ritorno del RDD
  }

  def boolCondition(arc1: (String, String, String), arc2: (String, String, String)): Boolean = {
    var ret = false
    if ((arc1._1 == arc2._2 && arc1._2 != arc2._1) || (arc1._2 == arc2._1 && arc1._1 != arc2._2) || (arc1._1 == arc2._1 && arc1._2 != arc2._2) || (arc1._2 == arc2._2 && arc1._1 != arc2._1)) {
      ret = true
    }
    return ret
  }

  def constructTheGraph(couple: ((String, String, String), (String, String, String))): mutable.MutableList[MyGraph] = {
    var G = new MyGraph()
    //println("CANDIDATI "+couple)
    var listRis:mutable.MutableList[MyGraph]=mutable.MutableList.empty[MyGraph]
    //print(" CASO 1 ")
    //f ((fEdgesSet[i][0] == fEdgesSet[j][1]) and (fEdgesSet[i][1] == fEdgesSet [j][0]) and (fEdgesSet[i][0] != fEdgesSet [i][1]) and (fEdgesSet[j][0] != fEdgesSet [j][1])):
    if ((couple._1._1 == couple._2._2) && (couple._1._2 == couple._2._1) && (couple._1._1 != couple._1._2) && (couple._2._1 != couple._2._2)) {
      //cycle
      //print(" IN CASO 1 ")
      var V1 = new VertexAF(couple._1._1)
      var V2 = new VertexAF(couple._2._2)
      G = new MyGraph()
      V1.addEdge(V2, couple._1._3)
      V2.addEdge(V1, couple._2._3)
      G.addNode(V1)
      G.addNode(V2)
      listRis:+=(G)

    }
    //print(" CASO 2 ")
    //((fEdgesSet[i][1] == fEdgesSet[j][1])(fEdgesSet[i][0] != fEdgesSet[i][1])(fEdgesSet[i][1] != fEdgesSet[j][0]) and (fEdgesSet[i][0] != fEdgesSet[j][0]) and (fEdgesSet[i][0] != fEdgesSet[j][1]) ):
    if ((couple._1._2 == couple._2._2) && (couple._1._1 != couple._1._2) && (couple._1._2 != couple._2._1) && (couple._1._1 != couple._2._1) && (couple._1._1 != couple._2._2)) {
      //print(" IN CASO 2 ")
      var V0 = new VertexAF(couple._2._1)
      var V1 = new VertexAF(couple._1._1)
      var V2 = new VertexAF(couple._1._2)
      G = new MyGraph()
      V0.addEdge(V2, couple._2._3)
      V1.addEdge(V2, couple._1._3)
      G.addNode(V0)
      G.addNode(V1)
      G.addNode(V2)
      listRis:+=(G)
    }
    //print(" CASO 3 ")
    // ((fEdgesSet[i][0] == fEdgesSet[j][1])(fEdgesSet[i][0] != fEdgesSet[i][1]) (fEdgesSet[i][0] != fEdgesSet[j][0])(fEdgesSet[i][1] != fEdgesSet[j][0]) and (fEdgesSet[i][1] != fEdgesSet[j][1]) ):
    if ((couple._1._1 == couple._2._2) && (couple._1._1 != couple._1._2) && (couple._1._1 != couple._2._1) && (couple._1._2 !=  couple._2._1) && (couple._1._2 !=  couple._2._2)){
      //print(" IN CASO 3 ")
      var V0 = new VertexAF(couple._2._1)
      var V1 = new VertexAF(couple._1._1)
      var V2 = new VertexAF(couple._1._2)
      G = new MyGraph()
      V0.addEdge(V1,couple._2._3)
      V1.addEdge(V2,couple._1._3)
      G.addNode(V0)
      G.addNode(V1)
      G.addNode(V2)
      listRis:+=(G)

    }
    //print(" CASO 4 ")
    // ((fEdgesSet[i][0] == fEdgesSet[j][0]) (fEdgesSet[i][0] != fEdgesSet[i][1]) (fEdgesSet[i][0] != fEdgesSet[j][1]) and (fEdgesSet[i][1] != fEdgesSet[j][0]) and (fEdgesSet[i][1] != fEdgesSet[j][1]) ):
    if ((couple._1._1 == couple._2._1) && (couple._1._1 != couple._1._2) && (couple._1._1 != couple._2._2) && (couple._1._2 != couple._2._1) && (couple._1._2 != couple._2._2) ){
      //print(" IN CASO 4 ")
      var V0 = new VertexAF(couple._1._1)
      var V1 = new VertexAF(couple._1._2)
      var V2 = new VertexAF(couple._2._2)
      var G = new MyGraph()
      V0.addEdge(V1,couple._1._3)
      V0.addEdge(V2,couple._2._3)
      G.addNode(V0)
      G.addNode(V1)
      G.addNode(V2)
      listRis:+=(G)
    }
    //printf(" CASO 5")
    if ((couple._1._2==couple._2._1) && (couple._1._1!=couple._1._2) && (couple._2._1!=couple._2._2) && (couple._1._1!=couple._2._1) && (couple._1._1!=couple._2._2)){
      //print(" IN CASO 5")
      var V0 = new VertexAF(couple._1._1)
      var V1 = new VertexAF(couple._1._2)
      var V2 = new VertexAF(couple._2._2)
      var G = new MyGraph()
      V0.addEdge(V1,couple._1._3)
      V1.addEdge(V2,couple._2._3)
      G.addNode(V0)
      G.addNode(V1)
      G.addNode(V2)
      listRis:+=(G)
    }
    /**println(" RIS OBTAINED ")
    for(el <- listRis){
      println("NEXT #")
      println(el.toPrinit())
    }*/
    return listRis
  }

  def makeItUndirect(inGraph:MyGraph): (MyGraph,String)={
   // println("Grafo orientato ")
    //inGraph.toPrinit()
    var un_G = new MyGraph();
    var strugglers:mutable.MutableList[(String,String)]=mutable.MutableList.empty[(String,String)]
    var couple:mutable.MutableList[(String,String)]=mutable.MutableList.empty[(String,String)]
    //println("Bella")
    var S:VertexAF=null;
    var D:VertexAF=null;
    var nodes=inGraph.nodes
    for(el <- nodes) {
      /*mi prendo i nodi, poi per ogni nodo mi prendo un arco*/
      //println("Esamindando "+el.vid)
      //un_G.toPrinit()

      if (un_G.nodes.count(f => f.vid == el.vid) >= 1) { //Verifica la presenza del nodo nel grafo, se non c'è lo creo vuoto
        S = un_G.nodes.filter(f => f.vid == el.vid).head
        //println("Trovato S -> "+el.vid)
      }
      else {
        S = new VertexAF(el.vid)
        //println("Creo S -> "+el.vid)
        un_G.addNode(S)
      }
      for (el1 <- el.adjencies) {
         if(!couple.contains(el.vid,el1._1.vid) && !couple.contains(el1._1.vid,el.vid)){
            /*occhio caso cicli*/
            //println("Trovo "+el1._1.vid)
            if (un_G.nodes.count(f => f.vid == el1._1.vid) >= 1) {
              D = un_G.nodes.filter(f => f.vid == el1._1.vid).head
              //println("Trovato D -> "+el1._1.vid)
            }
            else {
              D = new VertexAF(el1._1.vid)
              //println("Creo D -> " + el1._1.vid)
              un_G.addNode(D)
            }
            if (S != null && D != null) {
              S.addEdge(D, el1._2)
              D.addEdge(S, el1._2)
            }
           couple:+=(el.vid,el1._1.vid)
         } else {
           if(couple.contains(el.vid,el1._1.vid)){
             strugglers:+=(el1._1.vid,el.vid)
           }
           else {
             strugglers:+=(el.vid,el1._1.vid)
           }

         }


      }
    }
    //var app=un_G.nodes.head
    //un_G.DFSVisit(app)
    var code=""
    if(un_G.nodes.length!=0){
      code=un_G.minDFS(strugglers,inGraph.nodes.clone())
      println("DFSCODE "+code)
    }
    //**ASSEGNAMENTO del DFSCODE al grafo in input*//
    inGraph.dfscode=code
    return inGraph
  }
}
