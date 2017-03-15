package BigD

import scala.collection.mutable.MutableList

/**
  * Created by francesco on 10/03/17.
  */
class VertexAF (id:String) extends Serializable{
  var adjencies: MutableList[(VertexAF,String)]=MutableList.empty[(VertexAF,String)]
  val vid:String=id;
  def addEdge(dest:VertexAF,hour:String) {
    adjencies+:=(dest,hour)
  }
  def getAdjencies():MutableList[(VertexAF,String)] ={
    return adjencies
  }

  //override def toString: String = super.toString
  def toPrint() {
    printf("\nVertex "+vid+" adjencyList -> ")
    for(el <- adjencies){
      printf(" "+el._1.vid+" h: "+el._2+" ")
    }
    printf("\n")
  }
}
