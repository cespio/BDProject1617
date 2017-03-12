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

  def toPrinit(): Unit ={
    for(el <- nodes){
      println("\n")
      el.toPrint()
      println("\n")
    }
  }
}
