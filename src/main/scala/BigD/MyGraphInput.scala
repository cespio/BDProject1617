package BigD

import scala.collection.mutable.MutableList

/**
  * Created by france on 07/05/17.
  */
class MyGraphInput extends Serializable{
  /*Implementare una map -> key,value dove la key è l'id del nodo e il value è l'oggetto relativo a quell'id */
  var nodes=MutableList.empty[VertexAFInput]
  def addNode(node:VertexAFInput) {
    nodes.+=(node)
  }

  def getNode(k:String):VertexAFInput={
    return nodes.filter(p=>p.vid==k).head
  }

  def keyPres(k:String):Boolean={
    return  nodes.count(p=>p.vid==k)==1
  }

  def toPrint(){
    for(el <- nodes){
      print(el.toPrint())
    }
  }

}
