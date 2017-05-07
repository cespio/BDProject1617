package BigD

import scala.collection.mutable.MutableList

/**
  * Created by france on 07/05/17.
  */
class MyGraphInput extends Serializable{
  /*Implementare una map -> key,value dove la key è l'id del nodo e il value è l'oggetto relativo a quell'id */
  var mapNodes=scala.collection.mutable.HashMap.empty[String,VertexAFInput]
  def addNode(k:String,node:VertexAFInput) {
    mapNodes+=(k->node)
  }

  def getNode(k:String):VertexAFInput={
    return mapNodes.get(k).head
  }

  def keyPres(k:String):Boolean={
    return  mapNodes.contains(k)
  }

  def toPrint(){
    for(el <- mapNodes.keys){
      print(mapNodes(el).toPrint())
    }
  }
}
