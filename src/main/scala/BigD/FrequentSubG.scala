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

  //function do define
  //frequent edges


  //constructor of candidates simple
  //CSP

  def candidateGeneration(freQE: RDD[(String, String, String)]):RDD[MyGraph]={
   //val temp1 = freQE.cartesian(freQE)
    var temp1 = freQE.cartesian(freQE).filter(el => (el._1._1 == el._2._2 && el._1._2 != el._2._1) || (el._1._2 == el._2._1 && el._1._1 != el._2._2) || (el._1._1 == el._2._1 && el._1._2 != el._2._2) || (el._1._2 == el._2._2 && el._1._1 != el._2._1))//.filter(el => boolCondition(el._1, el._2) && Math.abs(el._1._3.toLong - el._2._3.toLong) <= 4)
    val temp2 = temp1.flatMap(el => constructTheGraph(el))//.filter(el => el.nodes.length > 0).map(el => makeItUndirect(el))
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
      //cycle
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
    /** println(" RIS OBTAINED ")
      * for(el <- listRis){
      * println("NEXT #")
      * println(el.toPrinit())
      * } */
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


  //TODO verify if it is possible to move this function inside the MyGraph class.
  def makeItUndirect(inGraph: MyGraph): MyGraph = {
    var un_G = new MyGraph();
    var strugglers: mutable.MutableList[(String, String)] = mutable.MutableList.empty[(String, String)]
    var couple: mutable.MutableList[(String, String)] = mutable.MutableList.empty[(String, String)]
    var S: VertexAF = null;
    var D: VertexAF = null;
    var nodes = inGraph.nodes
    for (el <- nodes) {
      if (un_G.nodes.count(f => f.vid == el.vid) >= 1) {
        S = un_G.nodes.filter(f => f.vid == el.vid).head
      }
      else {
        S = new VertexAF(el.vid)
        un_G.addNode(S)
      }
      for (el1 <- el.adjencies) {
        if (!couple.contains(el.vid, el1._1.vid) && !couple.contains(el1._1.vid, el.vid)) {
          if (un_G.nodes.count(f => f.vid == el1._1.vid) >= 1) {
            D = un_G.nodes.filter(f => f.vid == el1._1.vid).head
          }
          else {
            D = new VertexAF(el1._1.vid)
            un_G.addNode(D)
          }
          if (S != null && D != null) {
            S.addEdge(D, el1._2)
            D.addEdge(S, el1._2)
          }
          couple :+= (el.vid, el1._1.vid)
        } else {
          if (couple.contains(el.vid, el1._1.vid)) {
            strugglers :+= (el1._1.vid, el.vid)
          }
          else {
            strugglers :+= (el.vid, el1._1.vid)
          }

        }
      }
    }
    var code = ""

    if (un_G.nodes.length != 0)
      code = un_G.minDFS(strugglers, inGraph.nodes.clone())
    //**ASSEGNAMENTO del DFSCODE al grafo in input*//
    print(code)
    inGraph.dfscode = code
    return inGraph
  }


  //non se più necessario reduced couple
  def CSPMapReduce(inputGraph: MyGraphInput, toVerify: MyGraph ):Int={
    //ritorno le coopie del mio grafo
    var couples = toVerify.allCouples()
    /*//dal grafo di input mi prendo le triple ((ids,at),(idd,at),at)) che compaino nel grafo e che rispettano la coopia
    //bisogna capire se conviene così, o con l'RDD nel main creato (approccio precdente))
    var temp=inputGraph.subgraph(epred= e => couples.indexOf((e.srcAttr,e.dstAttr,e.attr))>0)
    //Creao una lista di RDD, ogni label ha il suo rdd, poi proddocartesiano
    var listRDD:mutable.MutableList[RDD[Int]]=mutable.MutableList.empty[RDD[Int]]
    var domainRDD:RDD[List[Int]]=null
    for(el <- toVerify.nodes) {
      listRDD :+= temp.vertices.filter(v => v._2 == el.vid).map(v => v._1.toInt)
    }
    for(i <- 0 to listRDD.length-1){
      println(i)
      if(i==0) {
        domainRDD = listRDD.get(i).get.map(el => List(el))
      }
      else {
        var tmp=domainRDD.cartesian(listRDD.get(i).head).map(el=> el._1 ++ List(el._2))
        domainRDD=tmp
      }
    }

    /*var ret=domainRDD.map(el => checkGraph(toVerify,el,inputGraph))*/
    //var ret=inputGraph.map(el => checkGraph(toVerify,el,inputGraph))
    //ret.collect().foreach(print(_))
    var array=domainRDD.collect()
    for(dom <- array){
      println("INIZIO LISTA")
      dom.foreach(println(_))
      print("FINE LISTA")
      for(el <- toVerify.nodes){
        var index1=toVerify.nodes.indexOf(el)
        var n1=dom(index1)
        for(nested <- el.adjencies){
          //mi serve l'indice di el,l'indice di nested._1.id,per poi recuperare l'ordine in cui sono inseriti gli elementi nel dominio
          //e poi controllar el'esistenza di su inputgraph -> there exist an edge between these two nodes with this arch weight?
          //recupero l'id di el
          var index2=toVerify.nodes.indexOf(toVerify.nodes.filter(el1 => el1.vid==nested._1.vid).head)
          var n2=dom(index2)
          println(index1)
          println(index2)
          if(inputGraph.triplets.filter( e => e.srcId.toString==n1 && e.dstId.toString==n2 && e.attr==nested._2).count()==0){
            print("NO")
          }
        }
      }
      print("SI")

    }
    //DOMAINRDD hai tutte le possibili combinazioni di domini
    //bisognerebbe strutturare una map((dominiocandidato),grafo,input)) -> ritornare zero o uno e poi sommare
    //la funzione sopracitata ritorna 1 se il candidato compare nel grafo
    */

    return 0
  }


  def checkGraph(toVerify:MyGraph, dom:List[Int], inputGraph:Graph[String,String]){
    println("INIZIO LISTA")
    dom.foreach(println(_))
    print("FINE LISTA")
    for(el <- toVerify.nodes){
      var index1=toVerify.nodes.indexOf(el)
      var n1=dom(index1)
      for(nested <- el.adjencies){
        //mi serve l'indice di el,l'indice di nested._1.id,per poi recuperare l'ordine in cui sono inseriti gli elementi nel dominio
        //e poi controllar el'esistenza di su inputgraph -> there exist an edge between these two nodes with this arch weight?
        //recupero l'id di el
        var index2=toVerify.nodes.indexOf(toVerify.nodes.filter(el1 => el1.vid==nested._1.vid).head)
        var n2=dom(index2)
        println(index1)
        println(index2)
        if(inputGraph.triplets.filter( e => e.srcId.toString==n1 && e.dstId.toString==n2 && e.attr==nested._2).count()==0){
          "0"
        }
      }
    }
    "1"
  }
}


//2*DOMINI*//
//3*RECUPERO NODI DA GRAPHX E CONTO CON I CANDDATI DOMINI*//
//4*IL NUMERO DI POSSIBILI ASSEGNAMENTI DIVERSI é il numero di soluzioni**//
//5*REVERSE MAP REDUCE. k=function(count) val DFS**/
