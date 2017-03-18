package BigD

import scala.collection.mutable.MutableList
/**
  * Created by francesco on 10/03/17.
  * AGGIUNGERE DFSCODE
  * AGGIUNGERE L'estesione supplementare
  */
class MyGraph() extends  Serializable{
  var nodes: MutableList[VertexAF]=MutableList.empty[VertexAF];
  def addNode(el:VertexAF) {
    nodes+:= el
  }

  def toPrinit() {
    for(el <- nodes){
      el.toPrint()
    }
  }

  //TODO verificare correttezza
  def DFSVisit(source: VertexAF): MutableList[(Int,Int,String,String,String)] ={
    /*Deve ritornare una lista di edges */
    var visit: MutableList[(Int,Int,String,String,String)]=MutableList.empty[(Int,Int,String,String,String)]
    var time=0
    var discovery=collection.mutable.Map[String,Int]()
    var couple= MutableList.empty[(String,String)]
    discovery(source.vid)=time
    println("INIZIO DA "+source.vid)
    DFS(discovery,source,time,visit,couple)
    print(visit.mkString("\n"))
    return visit

  }

  /*Label uniche -> unica visita DFS*/
  def DFS(discovery: collection.mutable.Map[String,Int],node: VertexAF,time:Int,visit :MutableList[(Int,Int,String,String,String)],couple: MutableList[(String,String)]) {
    discovery(node.vid)=time
    println("Sono in -> "+node.vid)
    var sorted_neighb=node.adjencies.sortBy(el=>el._1.vid)
    sorted_neighb.foreach(el => println(el._1.vid))
    for(cand <- sorted_neighb){
      if(!discovery.keys.exists(p => p==cand._1.vid)) {
        println("Ho scoperto -> "+cand._1.vid)
        discovery(cand._1.vid) = time
        couple.+=:(node.vid,cand._1.vid) //padre-> figlio
        couple.+=:(cand._1.vid,node.vid) //padre-> figlio
        visit.+=:((time, time + 1, node.vid, cand._2, cand._1.vid))
        DFS(discovery, cand._1,time+1,visit,couple)
      }
      else{
        if(!couple.contains(node.vid,cand._1.vid)){
          println("BACKTRACCKKK")
          visit.+=:((discovery(node.vid),discovery(cand._1.vid),node.vid,cand._2,cand._1.vid))
        }
      }

    }
  }

  def minDFS(): Unit ={
    var source=this.nodes.sortBy(el=>el.vid).head
    println("NEW FUCKING VISIT")
    var visit=DFSVisit(source)
    var sortedVisit=mergeSorted(visit)
    //TODO sorting between backedges/frontedges

  }

  /*def mergeSorted(visit:MutableList[(Int,Int,String,String,String)]): Unit ={

  }*/
  /*unique source point -> sorting the list*/

}
