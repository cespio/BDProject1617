import scala.collection.mutable.MutableList

/*
*  BigData project 2017, Frequent Pattern Mining on Labeled Oriented Single Graph
*  @authors { Francesco Contaldo, Alessandro Rizzuto}
*
* */

//Class used to represent a single input nodes
class VertexAFInput (id:String,labelIn:String) extends Serializable {
  var label:String=labelIn
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

