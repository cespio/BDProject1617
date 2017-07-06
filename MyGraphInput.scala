import scala.collection.mutable.MutableList

/*
*  BigData project 2017, Frequent Pattern Mining on Labeled Oriented Single Graph
*  @authors { Francesco Contaldo, Alessandro Rizzuto}
*
* */

//Class used to represent the input graph
class MyGraphInput extends Serializable{
  var nodes=MutableList.empty[VertexAFInput] //Adjencies
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

  //Given an edge it return the all the nodes in the input graph with those two label
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

  //given source and destination, and two possible domain for those two labels, this function verify what nodes in the domain have
  //an edge with the labels source and des
  def retrevieTwo(sor:String,listS:MutableList[String],des:String,listD:MutableList[String],weight:String):MutableList[(String,String)]={
    var ret:MutableList[(String,String)]=MutableList.empty[(String,String)]
    for(el<-listS){
      for(el1 <- listD){
        if(this.nodes.filter(p=>p.vid==el).head.adjencies.count( k=>k._1.vid==el1 && k._2==weight)>0){
          ret += ((sor,el))
          ret += ((des,el1))
        }

      }
    }
    return ret

  }

   //verfiy if a given edge appear in the input graph
  def edgeBool(ids:String,idd:String,w:String):Boolean={
    var nodeS=nodes.filter(n=>n.vid==ids).head
    return nodeS.adjencies.count(n=> n._1.vid==idd && n._2==w)==1
  }

}
