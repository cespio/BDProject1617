package BigD

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

import scala.collection.mutable
import scala.util.control.Breaks._


/**
  * Created by francesco on 02/03/17.
  */
class FrequentSubG (graph_arg:MyGraphInput,thr_arg:Int,size_arg:Int) extends Serializable{
  var graph: MyGraphInput = graph_arg
  val thr = thr_arg
  val size = size_arg

  def candidateGeneration(freQE: RDD[(String, String, String)]):RDD[MyGraph]={
   //val temp1 = freQE.cartesian(freQE)
    var temp1 = freQE.cartesian(freQE).filter(el => (el._1._1 == el._2._2 && el._1._2 != el._2._1) || (el._1._2 == el._2._1 && el._1._1 != el._2._2) || (el._1._1 == el._2._1 && el._1._2 != el._2._2) || (el._1._2 == el._2._2 && el._1._1 != el._2._1))//.filter(el => boolCondition(el._1, el._2) && Math.abs(el._1._3.toLong - el._2._3.toLong) <= 4)
    val temp2 = temp1.flatMap(el => constructTheGraph(el)).filter(el => el.nodes.length > 0).map(el => el.makeItUndirect())
    return temp2
    //Possibile ritorno del RDD
  }

  def boolCondition(arc1: (String, String, String), arc2: (String, String, String)): Boolean = {
    var ret = false
    if ((arc1._1 == arc2._2 && arc1._2 != arc2._1) || (arc1._2 == arc2._1 && arc1._1 != arc2._2) || (arc1._1 == arc2._1 && arc1._2 != arc2._2) || (arc1._2 == arc2._2 && arc1._1 != arc2._1)) {
      ret = true
    }
    return ret
  }

  def constructTheGraph(couple: ((String, String, String), (String, String, String))): mutable.MutableList[MyGraph] = {
    var G = new MyGraph()
    var listRis: mutable.MutableList[MyGraph] = mutable.MutableList.empty[MyGraph]
    //print(" CASO 1 ")
    //f ((fEdgesSet[i][0] == fEdgesSet[j][1]) and (fEdgesSet[i][1] == fEdgesSet [j][0]) and (fEdgesSet[i][0] != fEdgesSet [i][1]) and (fEdgesSet[j][0] != fEdgesSet [j][1])):
    if ((couple._1._1 == couple._2._2) && (couple._1._2 == couple._2._1) && (couple._1._1 != couple._1._2) && (couple._2._1 != couple._2._2)) {
      //print(" IN CASO 1 ")
      var V1 = new VertexAF(couple._1._1)
      var V2 = new VertexAF(couple._2._2)
      G = new MyGraph()
      V1.addEdge(V2, couple._1._3)
      V2.addEdge(V1, couple._2._3)
      G.addNode(V1)
      G.addNode(V2)
      listRis :+= (G)

    }
    //print(" CASO 2 ")
    //((fEdgesSet[i][1] == fEdgesSet[j][1])(fEdgesSet[i][0] != fEdgesSet[i][1])(fEdgesSet[i][1] != fEdgesSet[j][0]) and (fEdgesSet[i][0] != fEdgesSet[j][0]) and (fEdgesSet[i][0] != fEdgesSet[j][1]) ):
    if ((couple._1._2 == couple._2._2) && (couple._1._1 != couple._1._2) && (couple._1._2 != couple._2._1) && (couple._1._1 != couple._2._1) && (couple._1._1 != couple._2._2)) {
      //print(" IN CASO 2 ")
      var V0 = new VertexAF(couple._2._1)
      var V1 = new VertexAF(couple._1._1)
      var V2 = new VertexAF(couple._1._2)
      G = new MyGraph()
      V0.addEdge(V2, couple._2._3)
      V1.addEdge(V2, couple._1._3)
      G.addNode(V0)
      G.addNode(V1)
      G.addNode(V2)
      listRis :+= (G)
    }
    //print(" CASO 3 ")
    // ((fEdgesSet[i][0] == fEdgesSet[j][1])(fEdgesSet[i][0] != fEdgesSet[i][1]) (fEdgesSet[i][0] != fEdgesSet[j][0])(fEdgesSet[i][1] != fEdgesSet[j][0]) and (fEdgesSet[i][1] != fEdgesSet[j][1]) ):
    if ((couple._1._1 == couple._2._2) && (couple._1._1 != couple._1._2) && (couple._1._1 != couple._2._1) && (couple._1._2 != couple._2._1) && (couple._1._2 != couple._2._2)) {
      //print(" IN CASO 3 ")
      var V0 = new VertexAF(couple._2._1)
      var V1 = new VertexAF(couple._1._1)
      var V2 = new VertexAF(couple._1._2)
      G = new MyGraph()
      V0.addEdge(V1, couple._2._3)
      V1.addEdge(V2, couple._1._3)
      G.addNode(V0)
      G.addNode(V1)
      G.addNode(V2)
      listRis :+= (G)

    }
    //print(" CASO 4 ")
    // ((fEdgesSet[i][0] == fEdgesSet[j][0]) (fEdgesSet[i][0] != fEdgesSet[i][1]) (fEdgesSet[i][0] != fEdgesSet[j][1]) and (fEdgesSet[i][1] != fEdgesSet[j][0]) and (fEdgesSet[i][1] != fEdgesSet[j][1]) ):
    if ((couple._1._1 == couple._2._1) && (couple._1._1 != couple._1._2) && (couple._1._1 != couple._2._2) && (couple._1._2 != couple._2._1) && (couple._1._2 != couple._2._2)) {
      //print(" IN CASO 4 ")
      var V0 = new VertexAF(couple._1._1)
      var V1 = new VertexAF(couple._1._2)
      var V2 = new VertexAF(couple._2._2)
      var G = new MyGraph()
      V0.addEdge(V1, couple._1._3)
      V0.addEdge(V2, couple._2._3)
      G.addNode(V0)
      G.addNode(V1)
      G.addNode(V2)
      listRis :+= (G)
    }
    //printf(" CASO 5")
    if ((couple._1._2 == couple._2._1) && (couple._1._1 != couple._1._2) && (couple._2._1 != couple._2._2) && (couple._1._1 != couple._2._1) && (couple._1._1 != couple._2._2)) {
      //print(" IN CASO 5")
      var V0 = new VertexAF(couple._1._1)
      var V1 = new VertexAF(couple._1._2)
      var V2 = new VertexAF(couple._2._2)
      var G = new MyGraph()
      V0.addEdge(V1, couple._1._3)
      V1.addEdge(V2, couple._2._3)
      G.addNode(V0)
      G.addNode(V1)
      G.addNode(V2)
      listRis :+= (G)
    }
    return listRis
  }

  /*Candidate Generation complex*/
  def extension(candidate: RDD[MyGraph], freqE: RDD[(String, String, String)]): RDD[MyGraph] = {

    def matching(el: (MyGraph, (String, String, String))): Boolean = {
      var edge = el._2
      for (i <- el._1.nodes) yield {
        for (j <- i.adjencies) yield {
          if ((i.vid == edge._1) || (i.vid == edge._2) || (j._1.vid == edge._1) || (j._1.vid == edge._2)) {
            return true
          }
        }
      }
      return false
    }

    def labeling(el: (MyGraph, (String, String, String))): Boolean = {
      for (i <- el._1.nodes) yield {
        if ((i.vid == el._2._1) && (el._2._1 != el._2._2)) {
          if ((i.vid == el._2._2))
            return false
        }

        if ((i.vid == el._2._2) && (el._2._1 != el._2._2)) {
          if (i.vid == el._2._1)
            return false
        }

        for (j <- i.adjencies) {
          if ((j._1.vid == el._2._1) && (el._2._1 != el._2._2)) {
            if ((j._1.vid == el._2._2) || (i.vid == el._2._2))
              return false
          }

          if ((j._1.vid == el._2._2) && (el._2._1 != el._2._2)) {
            if ((j._1.vid == el._2._1) || (i.vid == el._2._1))
              return false
          }
        }
      }
      return true
    }

    def categorize(el: (MyGraph, (String, String, String))): (MyGraph, (String, String, String), Int) = {
      var temp = 0
      for (i <- el._1.nodes) {
        if (i.vid == el._2._1)
          temp = 1
        if (i.vid == el._2._2)
          temp = 2
        if (temp == 1) {
          if (i.vid == el._2._2)
            temp = 3
        }
        if (temp == 2) {
          if (i.vid == el._2._1)
            temp = 4
        }

        for (j <- i.adjencies) {
          if (j._1.vid == el._2._1)
            temp = 1
          if (j._1.vid == el._2._2)
            temp = 2
          if (temp == 1) {
            if (j._1.vid == el._2._2)
              temp = 3
          }
          if (temp == 2) {
            if (j._1.vid == el._2._1)
              temp = 4
          }
        }
      }
      (el._1, el._2, temp)
    }


    def hourGap(el: (MyGraph, (String, String, String))): Boolean = {
      var min: Int = Int.MaxValue
      var max: Int = Int.MinValue

      for (i <- el._1.nodes) yield {
        for (j <- i.adjencies) {

          if ((j._2.toInt) < min && (j._2 != null))
            min = j._2.toInt

          if (j._2.toInt > max && (j._2 != null))
            max = j._2.toInt
        }
      }

      if (el._2._3.toInt < min)
        min = el._2._3.toInt
      if (el._2._3.toInt > max)
        max = el._2._3.toInt

      if (max - min <= 4)
        return true
      else
        return false
    }


    var partialGraphs = candidate.cartesian(freqE).filter(el => matching(el)).filter(el => labeling(el)).filter(el => hourGap(el)).map(el => categorize(el))

    var toExtend = partialGraphs.map(el => extendTheGraph(el))

    //partialGraphs.collect().map(el=>println(el._1.visualRoot(), el._2, el._3))
    return toExtend

  }

  def extendTheGraph(triplet: (MyGraph, (String, String, String), Int)): MyGraph = {

    var clone: MyGraph = triplet._1.myclone()
    /*println("----------------------")
    println("PRIMA ESTENSIONE GRAFO")
    println("----------------------")
    println(clone.toPrinit(), triplet._2)*/

    if (triplet._3 == 1) {
      breakable {
        for (i <- clone.nodes) {
          if (i.vid == triplet._2._1) {
            var newVertex = new VertexAF(triplet._2._2)
            i.addEdge(newVertex, triplet._2._3)
            clone.addNode(newVertex)
            break
          }
        }
      }

    }

    if (triplet._3 == 2) {
      breakable {
        for (i <- clone.nodes) {
          if (i.vid == triplet._2._2) {
            var newVertex = new VertexAF(triplet._2._1)
            newVertex.addEdge(i, triplet._2._3)
            clone.addNode(newVertex)
            break
          }
        }
      }
    }

    if (triplet._3 == 3) {
      breakable {
        for (i <- clone.nodes) {
          if (i.vid == triplet._2._1) {
            for (j <- i.adjencies) {
              if (j._1.vid == triplet._2._2) {
                i.addEdge(j._1, triplet._2._3)
                break
              }
            }
          }
        }
      }
    }

    if (triplet._3 == 4) {
      breakable {
        for (i <- clone.nodes) {
          if (i.vid == triplet._2._2) {
            for (j <- i.adjencies) {
              if (j._1.vid == triplet._2._1) {
                j._1.addEdge(i, triplet._2._3)
                break
              }
            }
          }
        }
      }
    }

    /*println("-----------------------")
    println("DOPO L'ESTENSIONE GRAFO")
    println("-----------------------")
    println(clone.toPrinit())*/
    clone
  }



  //non se più necessario reduced couple
  def CSPMapReduce(inputGraph: MyGraphInput, toVerify: MyGraph):mutable.MutableList[List[String]]={
    //ritorno le coopie del mio grafo

    var domainCoup=mutable.MutableList.empty[mutable.MutableList[(String,String)]]
    var domainLabel=mutable.MutableList.empty[List[String]]

    var couples = toVerify.allCouples()
    println(couples)
    for(el <- couples){
      domainCoup+=inputGraph.retreiveDomainCouple(el)
    }
    var domainCoupF=domainCoup.flatten.distinct
    println(domainCoupF)
    var i=0
    for(el <- toVerify.nodes){
      if(i==0)
        domainLabel=domainCoupF.filter( ed => ed._1==el.vid).map(ed => List(ed._2))
      else{
        var tmp1=domainCoupF.filter(ed => ed._1==el.vid).map(ed => ed._2).toList
        var tmp2=domainLabel.flatMap(l1 => tmp1.map(a => l1++List(a)))
        domainLabel=tmp2
      }
      i=1
    }

    return domainLabel
  }


  def checkGraph(toVerify:MyGraph, dom:List[String], input: MyGraphInput):Int={

    for(el <- toVerify.nodes){
      var index1=toVerify.nodes.indexOf(el)
      var n1=dom(index1)
      for(nested <- el.adjencies){
        var index2=toVerify.nodes.indexOf(toVerify.nodes.filter(el1 => el1.vid==nested._1.vid).head)
        var n2=dom(index2)
        if(!input.edgeBool(dom(index1),dom(index2),nested._2)){

          return 0
        }
      }
    }
    return 1
  }
}