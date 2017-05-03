package BigD

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import scala.collection.mutable
import scala.util.control.Breaks._


/**
  * Created by francesco on 02/03/17.
  */
class FrequentSubG (graph_arg: org.apache.spark.graphx.Graph[String,String],thr_arg:Int,size_arg:Int) extends Serializable {
  var graph: org.apache.spark.graphx.Graph[String, String] = graph_arg
  val thr = thr_arg
  val size = size_arg

  //function do define
  //frequent edges
  def frequentEdges(): RDD[(String, String, String)] = {
    //si potrebbe applicare anche qui un principio di map reduce
    val temp = graph.triplets.map(tr => ((tr.srcAttr, tr.dstAttr, tr.attr), 1))
    val temp1 = temp.reduceByKey((a, b) => a + b).filter(el => (el._2.toInt >= thr && (el._1._1 != el._1._2))).map(el => el._1)
    return temp1
  }

  //constructor of candidates simple
  //CSP

  def candidateGeneration(freQE: RDD[(String, String, String)]): RDD[MyGraph] = {
    val temp1 = freQE.cartesian(freQE).filter(el => boolCondition(el._1, el._2) && Math.abs(el._1._3.toInt - el._2._3.toInt) <= 4)
    val temp2 = temp1.flatMap(el => constructTheGraph(el)).filter(el => el.nodes.length > 0).map(el => makeItUndirect(el))
    //temp2.collect()
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
    //println("CANDIDATI "+couple)
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
    toExtend.collect()
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
    // println("Grafo orientato ")
    //inGraph.toPrinit()
    var un_G = new MyGraph();
    var strugglers: mutable.MutableList[(String, String)] = mutable.MutableList.empty[(String, String)]
    var couple: mutable.MutableList[(String, String)] = mutable.MutableList.empty[(String, String)]
    //println("Bella")
    var S: VertexAF = null;
    var D: VertexAF = null;
    var nodes = inGraph.nodes
    for (el <- nodes) {
      /*mi prendo i nodi, poi per ogni nodo mi prendo un arco*/
      //println("Esamindando "+el.vid)
      //un_G.toPrinit()

      if (un_G.nodes.count(f => f.vid == el.vid) >= 1) {
        //Verifica la presenza del nodo nel grafo, se non c'è lo creo vuoto
        S = un_G.nodes.filter(f => f.vid == el.vid).head
        //println("Trovato S -> "+el.vid)
      }
      else {
        S = new VertexAF(el.vid)
        //println("Creo S -> "+el.vid)
        un_G.addNode(S)
      }
      for (el1 <- el.adjencies) {
        if (!couple.contains(el.vid, el1._1.vid) && !couple.contains(el1._1.vid, el.vid)) {
          /*occhio caso cicli*/
          //println("Trovo "+el1._1.vid)
          if (un_G.nodes.count(f => f.vid == el1._1.vid) >= 1) {
            D = un_G.nodes.filter(f => f.vid == el1._1.vid).head
            //println("Trovato D -> "+el1._1.vid)
          }
          else {
            D = new VertexAF(el1._1.vid)
            //println("Creo D -> " + el1._1.vid)
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
    //var app=un_G.nodes.head
    //un_G.DFSVisit(app)
    var code = ""
    if (un_G.nodes.length != 0) {
      code = un_G.minDFS(strugglers, inGraph.nodes.clone())
      //println("DFSCODE "+code)
    }
    //**ASSEGNAMENTO del DFSCODE al grafo in input*//
    inGraph.dfscode = code
    return inGraph
  }

  def CSPMapReduce(inputGraph: Graph[String, String], toVerify: MyGraph, reducedCouple: RDD[((String,String),List[(String,String)])]){
    //1*COPPIE MAP REDUCE*//
    var couples = toVerify.allCouples()//RDD of my couples -> need of key (dfscode,allcouples)
    var filteredCouples = reducedCouple.filter( el => couples.indexOf(el._1)>0) // coppie filtrate //TODO aggiungere il weight al "allcouples"
    //creazione dei domini
    var app:Graph[String,String]=inputGraph
    var temp=app.subgraph(epred= e => couples.indexOf((e.srcAttr,e.dstAttr))>0)
    temp.triplets.collect().foreach(print(_))
    /*for(el <- couples){
      println(el)
      var temp=app.subgraph(epred= e => (e.srcAttr,e.dstAttr)==el)
      temp.triplets.collect().foreach(println(_))
      app=temp

    }*/
    //Filter the couple with the regard of the big reduced already computed from reducedCouple
    //var dfscouplesDomain = dfscouples.map(el => (computeCSP(el._2,inputGraph,reducedCouple),el._1)) //ritorno il DFS,numero di volte in cui appare













    //2*DOMINI*//
    //RDD con key(coppia label)
    //3*RECUPERO NODI DA GRAPHX E CONTO CON I CANDDATI DOMINI*//
    //4*IL NUMERO DI POSSIBILI ASSEGNAMENTI DIVERSI é il numero di soluzioni**//
    //5*REVERSE MAP REDUCE. k=function(count) val DFS**/

  }
//
  /*def finalCount(inputGraph: Graph[String, String], candGraph: MyGraph):{
    var allCouples = candGraph.allCouples
  }*///

  def computeCSP( coupleGraph: mutable.MutableList[(String,String)], inputGraph:Graph[String,String], reducedCouple: RDD[((String,String),List[(String,String)])]){
    //filtraggio delle coppie
    var filteredReducedCouple=reducedCouple.filter(el => coupleGraph.indexOf(el._1)>0)
    filteredReducedCouple.collect().foreach(println(_))
    //coppie filtrate

    //creazione dei domini e poi verifica

  }
}