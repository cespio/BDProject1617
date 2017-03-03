package BigD

import org.apache.spark.graphx.{Graph, VertexId}
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
  def frequentEdges(thr:Int): Unit ={
    //si potrebbe applicare anche qui un principio di map reduce
    var temp1: RDD[((VertexId,VertexId,String), Int)] = graph.edges.map(e => ((e.srcId,e.dstId,e.attr),1))
    var temp2: RDD[(String,Int)] = temp1.map(s => (graph.vertices.filter(el => el._1 == s._1._1).map(el => el._2).first(), s._2))
    //var temp2: RDD[Any] = temp1.map(s => graph.vertices.values.take(0))
    //val s=graph.vertices.filter( el => el._1==10).map( el => el._2).first()
    temp2.collect.foreach(println(_))

  }
  //constructor of candidates
  //CSP
}
