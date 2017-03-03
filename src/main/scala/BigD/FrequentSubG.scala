package BigD

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

/**
  * Created by francesco on 02/03/17.
  */
class FrequentSubG (graph_arg: Graph[String,String],thr_arg:Int,size_arg:Int) extends Serializable {
  var graph : Graph[String,String]=graph_arg
  val thr=thr_arg
  val size=size_arg
  //function do define
  //frequent edges
  def frequentEdges(): RDD[(String,String,String)] ={
    //si potrebbe applicare anche qui un principio di map reduce
    val temp = graph.triplets.map(tr => ((tr.srcAttr,tr.dstAttr,tr.attr),1))
    val temp1 = temp.reduceByKey( (a,b) => a+b).filter(el => el._2>=thr).map(el => el._1)
    return temp1
  }
  //constructor of candidates
  //CSP
}
