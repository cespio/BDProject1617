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
  def DFSVisit(graph: MyGraph,source: VertexAF): Unit ={
    /*Deve ritornare una lista di edges */
    var visit: MutableList[(Int,Int,String,String,String)]=null;
    var time=0
    var discovery=collection.mutable.Map[String,Int]()
    discovery(source.vid)=time
    visit=DFS(graph,discovery,time,visit)

  }

  def DFS(graph: MyGraph,discovery: collection.mutable.Map[String,Int],time:Int,visit :MutableList[(Int,Int,String,String,String)]): MutableList[(Int,Int,String,String,String)] ={
    return collection.mutable.MutableList[(Int,Int,String,String,String)]()
  }
  /*unique source point -> sorting the list*/

}
