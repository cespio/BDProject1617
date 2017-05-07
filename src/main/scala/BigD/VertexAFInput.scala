package BigD

import scala.collection.mutable.MutableList

/**
  * Created by france on 07/05/17.
  */
class VertexAFInput (id:String,labelIn:String) extends Serializable {
  /*praticamente uguale all'implementazione base di vertexAF ma solo che c'è un campo in più per l'id*/
  var label:String=label
  var adjencies: MutableList[(VertexAFInput,String)]=MutableList.empty[(VertexAFInput,String)]
  val vid:String=id;

  def addEdge(dest:VertexAFInput,hour:String) {
    adjencies+:=(dest,hour)
  }
  def getAdjencies():MutableList[(VertexAFInput,String)] ={
    return adjencies
  }
  //override def toString: String = super.toString
  def toPrint() {
    printf("\nVertex "+vid+" adjencyList -> ")
    for(el <- adjencies){
      printf(el._1.label+" "+el._1.vid+" h: "+el._2+" ")
    }
    printf("\n")
  }
}

