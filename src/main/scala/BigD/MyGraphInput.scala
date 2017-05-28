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

  def retreiveDomainCouple(edge:(String,String,String)): MutableList[(String,String)]  ={
    var tmp1=nodes.filter(n=>n.label==edge._1)
    var tmp2=nodes.filter(n=>n.label==edge._2)
    var ret:MutableList[(String,String)]=MutableList.empty[(String,String)]
    for(el<-tmp1){
      for(el1 <- tmp2){
        if(el.adjencies.contains(el1,edge._3)){
          ret += ((edge._1,el.vid))
          ret += ((edge._2,el1.vid))
        }

      }
    }
    return ret
  }

  def edgeBool(ids:String,idd:String,w:String):Boolean={
    var nodeS=nodes.filter(n=>n.vid==ids).head
    return nodeS.adjencies.count(n=> n._1.vid==idd && n._2==w)==1
  }

}
