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
    val temp1 = temp.reduceByKey( (a,b) => a+b).filter(el => (el._2.toInt>=thr && (el._1._1  != el._1._2))).map(el => el._1)
    return temp1
  }
  //constructor of candidates
  //CSP

  def candidateGeneration(freQE: RDD[(String,String,String)]) ={
    val temp1 =freQE.cartesian(freQE).filter( el => el._1 != el._2 && boolCondition(el._1,el._2) && Math.abs(el._1._3.toInt - el._2._3.toInt)<=4)
    temp1.collect.foreach(println(_))
  }

  def boolCondition(arc1: (String,String,String), arc2: (String,String,String)): Boolean = {
    var ret=false
    if((arc1._1 == arc2._2 && arc1._2 != arc2._1) || (arc1._2==arc2._1 && arc1._1!=arc2._2) || (arc1._1==arc2._1 && arc1._2!=arc2._2) || (arc1._2==arc2._2 && arc1._1!=arc2._1)){
      ret=true
    }
    return ret
  }
}
