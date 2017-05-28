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
    //var temp1 = freQE.cartesian(freQE).map(el => (makekey(el),el)).reduceByKey((x,y)=>x).map(el=>el._2).filter(el=> (el._1._1!=el._1._2)&&(el._2._1!=el._2._2))//.filter(el=> Math.abs(el._1._3.toLong - el._2._3.toLong) <= 4)
    //var temp1 = freQE.cartesian(freQE).filter(el => el._1!=el._2).filter(el=> Math.abs(el._1._3.toLong - el._2._3.toLong) <= 4)
    //var prova2=freQE.cartesian(freQE).filter(el => compare(el._1,el._2))
    //println("PROVA2 "+prova2.count())
    var temp1=freQE.cartesian(freQE).filter(el => compare(el._1,el._2)).filter(el=> Math.abs(el._1._3.toInt - el._2._3.toInt) <= 4)
    var temp2 = temp1.flatMap(el => constructTheGraph(el)).map(el => el.makeItUndirect())
    //var prova = temp1.map(el=> constructTheGraph(el))
   // println("JJJ "+prova.count())
   // prova.collect().foreach(el=>println(el.length))
    return temp2
    //Possibile ritorno del RDD
  }

  def makekey(el:((String,String,String),(String,String,String))):String= {
    var a = el._1._1 ++ el._1._2 ++ el._1._3 ++ el._2._1 ++ el._2._2 ++ el._2._3
    return a.sorted
  }

  def compare(fst:(String,String,String),snd:(String,String,String)):Boolean={
    var a=fst._1+fst._2+fst._3
    var b=snd._1+snd._2+snd._3
    return a <= b
  }

  def boolCondition(arc1: (String, String, String), arc2: (String, String, String)): Boolean = {
    var ret = false
    if ((arc1._1 == arc2._2 && arc1._2 != arc2._1) || (arc1._2 == arc2._1 && arc1._1 != arc2._2) || (arc1._1 == arc2._1 && arc1._2 != arc2._2) || (arc1._2 == arc2._2 && arc1._1 != arc2._1)) {
      ret = true
    }
    return ret
  }

  def constructTheGraph(couple: ((String, String, String), (String, String, String))): mutable.MutableList[MyGraph] = {
    var listRis: mutable.MutableList[MyGraph] = mutable.MutableList.empty[MyGraph]

    //print(" CASO 1 ")
    // ((fEdgesSet[i][0]==fEdgesSet[j][1])(fEdgesSet[i][1]==fEdgesSet [j][0])(fEdgesSet[i][0]!=fEdgesSet[i][1])(fEdgesSet[j][0]=fEdgesSet[j][1])):
    if (couple._1._1!=couple._1._2 && couple._2._1!=couple._2._2 && (couple._1._1 == couple._2._2) && (couple._1._2 == couple._2._1) && (couple._1._1 != couple._1._2) && (couple._2._1 != couple._2._2)) {
      //println("("++couple._1._1++","++couple._1._2++","++couple._1._3 ++") ("++ couple._2._1++","++couple._2._2++","++couple._2._3++")");print("A")
      var V1 = new VertexAF(couple._1._1)
      var V2 = new VertexAF(couple._1._2)
      var G = new MyGraph()
      V1.addEdge(V2, couple._1._3)
      V2.addEdge(V1, couple._2._3)
      G.addNode(V1)
      G.addNode(V2)
      listRis += (G)

    }
    //print(" CASO 2 ")
    //((fEdgesSet[i][1] == fEdgesSet[j][1])(fEdgesSet[i][0] != fEdgesSet[i][1])(fEdgesSet[i][1] != fEdgesSet[j][0]) and (fEdgesSet[i][0] != fEdgesSet[j][0]) and (fEdgesSet[i][0] != fEdgesSet[j][1]) ):
    if (couple._1._1!=couple._1._2 && couple._2._1!=couple._2._2 && (couple._1._2 == couple._2._2) && (couple._1._1 != couple._1._2) && (couple._1._2 != couple._2._1) && (couple._1._1 != couple._2._1) && (couple._1._1 != couple._2._2)) {
      //println("("++couple._1._1++","++couple._1._2++","++couple._1._3 ++") ("++ couple._2._1++","++couple._2._2++","++couple._2._3++")");print("B")
      var V0 = new VertexAF(couple._2._1)
      var V1 = new VertexAF(couple._1._1)
      var V2 = new VertexAF(couple._1._2)
      var G = new MyGraph()
      V0.addEdge(V2, couple._2._3)
      V1.addEdge(V2, couple._1._3)
      G.addNode(V0)
      G.addNode(V1)
      G.addNode(V2)
      G.maxH=Math.max(couple._1._3.toInt,couple._2._3.toInt)
      G.minH=Math.min(couple._1._3.toInt,couple._2._3.toInt)
      listRis += (G)
    }
    //print(" CASO 3 ")
    // ((fEdgesSet[i][0] == fEdgesSet[j][1])(fEdgesSet[i][0] != fEdgesSet[i][1]) (fEdgesSet[i][0] != fEdgesSet[j][0])(fEdgesSet[i][1] != fEdgesSet[j][0]) and (fEdgesSet[i][1] != fEdgesSet[j][1]) ):
    if (couple._1._1!=couple._1._2 && couple._2._1!=couple._2._2 && (couple._1._1 == couple._2._2) && (couple._1._1 != couple._1._2) && (couple._1._1 != couple._2._1) && (couple._1._2 != couple._2._1) && (couple._1._2 != couple._2._2)) {
      //println("("++couple._1._1++","++couple._1._2++","++couple._1._3 ++") ("++ couple._2._1++","++couple._2._2++","++couple._2._3++")");print("C")
      var V0 = new VertexAF(couple._2._1)
      var V1 = new VertexAF(couple._1._1)
      var V2 = new VertexAF(couple._1._2)
      var G = new MyGraph()
      V0.addEdge(V1, couple._2._3)
      V1.addEdge(V2, couple._1._3)
      G.addNode(V0)
      G.addNode(V1)
      G.addNode(V2)
      G.maxH=Math.max(couple._1._3.toInt,couple._2._3.toInt)
      G.minH=Math.min(couple._1._3.toInt,couple._2._3.toInt)
      listRis += (G)

    }
    //print(" CASO 4 ")
    // ((fEdgesSet[i][0] == fEdgesSet[j][0]) (fEdgesSet[i][0] != fEdgesSet[i][1]) (fEdgesSet[i][0] != fEdgesSet[j][1]) and (fEdgesSet[i][1] != fEdgesSet[j][0]) and (fEdgesSet[i][1] != fEdgesSet[j][1]) ):
    if (couple._1._1!=couple._1._2 && couple._2._1!=couple._2._2 && (couple._1._1 == couple._2._1) && (couple._1._1 != couple._1._2) && (couple._1._1 != couple._2._2) && (couple._1._2 != couple._2._1) && (couple._1._2 != couple._2._2)) {
      //println("("++couple._1._1++","++couple._1._2++","++couple._1._3 ++") ("++ couple._2._1++","++couple._2._2++","++couple._2._3++")");print("D")
      var V0 = new VertexAF(couple._1._1)
      var V1 = new VertexAF(couple._1._2)
      var V2 = new VertexAF(couple._2._2)
      var G = new MyGraph()
      V0.addEdge(V1, couple._1._3)
      V0.addEdge(V2, couple._2._3)
      G.addNode(V0)
      G.addNode(V1)
      G.addNode(V2)
      G.maxH=Math.max(couple._1._3.toInt,couple._2._3.toInt)
      G.minH=Math.min(couple._1._3.toInt,couple._2._3.toInt)
      listRis += (G)
    }
    //printf(" CASO 5")
    /*if ((couple._1._2 == couple._2._1) && (couple._1._1 != couple._1._2) && (couple._2._1 != couple._2._2) && (couple._1._1 != couple._2._1) && (couple._1._1 != couple._2._2)) {
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
    }*/
    return listRis
  }

  def newExtension(candidate: RDD[MyGraph], freqE: RDD[(String, String, String)]): RDD[MyGraph] = {

    //var verifica=candidate.cartesian(freqE).map(el => complexExt(el._1,el._2)).map(el => (el._2.dfscode,el._1)).reduceByKey((a,b)=>a+b).collect().foreach(el=> println("Per questo grafo  "+el._1+" "+el._2))
    var toExtend=candidate.cartesian(freqE).map(el => complexExt(el._1,el._2)).filter(el => el._1==1).map(el => el._2)//.foreach(el=>println(el.dfscode))
    return toExtend
  }

  def complexExt(cand:MyGraph,edge:(String,String,String)): (Int,MyGraph) ={
    //TODO massimo e minimo come in data mining -> la modifica della struttura myGraph
    var candidate=cand.myclone()
    candidate.maxH=cand.maxH
    candidate.minH=cand.minH
    candidate.dfscode=cand.dfscode
    var hourPass=0
    var flag=0
    var nEdge=edge._3.toInt
    var sor:mutable.MutableList[VertexAF]=candidate.nodes.filter(el=> el.vid==edge._1)
    var des:mutable.MutableList[VertexAF]=candidate.nodes.filter(el=> el.vid==edge._2)
    if((candidate.maxH-candidate.minH)==4){
      if(nEdge<=candidate.maxH && nEdge >=candidate.minH){
        hourPass=1
      }
    }
    else{
      if(nEdge< candidate.minH && (candidate.maxH-nEdge)<=4){
        hourPass=1
        candidate.minH=nEdge
      }
      else{
        if(nEdge>candidate.maxH && (nEdge-candidate.minH)<=4){
          hourPass=1
          candidate.maxH=nEdge
        }
      }

    }
    if(hourPass==1) {
      if (sor.length == 1 && des.length == 0 && flag==0) { //se matcho sorgente e non destinazione
        flag = 1
        var toadd = new VertexAF(edge._2)
        //ci potrebbe stare il clone come no
        //println(candidate.dfscode+" caso 1 "+edge)
        var nodo = sor.head
        candidate.addNode(toadd)
        nodo.addEdge(toadd,edge._3)

      }
      if (sor.length == 0 && des.length == 1 && flag==0) { //se matcho sorgente e non destinazione
        flag = 1
        var toadd = new VertexAF(edge._1)
        var nodo = des.head
        //println(candidate.dfscode+" caso 2 "+edge)
        candidate.addNode(toadd)
        toadd.addEdge(nodo, edge._3)

      }
      /*if (sor.length == 1 && des.length == 1 && flag==0) { //se matcho sorgente e non destinazione

        //devo verificare prima che non siano linkati tra di loro
        if (sor.head.adjencies.count(el => el._1.vid == des.head.vid) == 0) {
          flag = 1
          //println(candidate.dfscode+" caso 3 "+edge)
          sor.head.addEdge(des.head, edge._3)
        }
        else{
         /* if(des.head.adjencies.count(el => el._1.vid == sor.head.vid) == 0){
            println(candidate.dfscode+" caso 3b "+edge)
            des.head.addEdge(sor.head, edge._3)
            f
            lag=1
          }*/
        }

      }*/
    }
    return (flag,candidate)
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

    var filteredFE=freqE.filter(el => el._1!=el._2)
    var partialGraphs = candidate.cartesian(filteredFE).filter(el => matching(el)).filter(el => labeling(el)).filter(el => hourGap(el)).map(el => categorize(el))
    println("LUNGHEZZA partial graph "+partialGraphs.count())
    var toExtend = partialGraphs.flatMap(el => extendTheGraph(el))
    println("ZIONE -> "+toExtend.count())

    //partialGraphs.collect().map(el=>println(el._1.visualRoot(), el._2, el._3))
    return toExtend

  }

  def extendTheGraph(triplet: (MyGraph, (String, String, String), Int)): mutable.MutableList[MyGraph] = {
    var listR: mutable.MutableList[MyGraph]=mutable.MutableList.empty[MyGraph]
    /*println("----------------------")
    println("PRIMA ESTENSIONE GRAFO")
    println("----------------------")
    println(clone.toPrinit(), triplet._2)*/

    if (triplet._3 == 1) {
      var clone: MyGraph = triplet._1
      breakable {
        for (i <- clone.nodes) {
          if (i.vid == triplet._2._1) {
            var newVertex = new VertexAF(triplet._2._2)
            i.addEdge(newVertex, triplet._2._3)
            clone.addNode(newVertex)
           // println("Caso 1, sorg = "+i.vid+", dest = "+triplet._2._2+", tripla = "+triplet._2+", DFS = "+clone.dfscode)
            listR:+=clone
            break
          }
        }
      }

    }

    if (triplet._3 == 2) {
      var clone: MyGraph = triplet._1
      breakable {
        for (i <- clone.nodes) {
          if (i.vid == triplet._2._2) {
            var newVertex = new VertexAF(triplet._2._1)
            clone.addNode(newVertex)
            newVertex.addEdge(i, triplet._2._3)
            //println("Caso 2, sorg = "+i.vid+", dest = "+triplet._2._2+", tripla = "+triplet._2+", DFS = "+clone.dfscode)
            listR:+=clone
            break
          }
        }
      }
    }

    if (triplet._3 == 3) {
      var clone: MyGraph = triplet._1
      breakable {
        for (i <- clone.nodes) {
          if (i.vid == triplet._2._1) {
            for (j <- i.adjencies) {
              if (j._1.vid == triplet._2._2) {
                i.addEdge(j._1, triplet._2._3)
               // println("Caso 3, sorg = "+i.vid+", dest = "+triplet._2._2+", tripla = "+triplet._2+", DFS = "+clone.dfscode)
                listR:+=clone
                break
              }
            }
          }
        }
      }
    }

    if (triplet._3 == 4) {
      var clone: MyGraph = triplet._1
      breakable {
        for (i <- clone.nodes) {
          if (i.vid == triplet._2._2) {
            for (j <- i.adjencies) {
              if (j._1.vid == triplet._2._1) {
                j._1.addEdge(i, triplet._2._3)
              //  println("Caso 1, sorg = "+i.vid+", dest = "+triplet._2._2+", tripla = "+triplet._2+", DFS = "+clone.dfscode)
                listR:+=clone
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
    return listR
  }



  //non se più necessario reduced couple
  def CSPMapReduce(inputGraph: MyGraphInput, toVerify: MyGraph):mutable.MutableList[List[String]]={
    //ritorno le coopie del mio grafo

    var domainCoup=mutable.MutableList.empty[mutable.MutableList[(String,String)]]
    var domainLabel=mutable.MutableList.empty[List[String]]

    var couples = toVerify.allCouples()
    for(el <- couples){
      domainCoup+=inputGraph.retreiveDomainCouple(el)
    }
    var domainCoupF=domainCoup.flatten.distinct

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

    return domainLabel.map(el=>el.distinct)
  }


  def checkGraph(toVerify:MyGraph, dom:List[String], input: MyGraphInput):Int={
    //println("DOM "+dom)
    if(dom.length!=toVerify.nodes.length){
      return  0
    }
    for(el <- toVerify.nodes){
      var index1=toVerify.nodes.indexOf(el)
      var n1=dom(index1)
      for(nested <- el.adjencies){
        var index2=toVerify.nodes.indexOf(toVerify.nodes.filter(el1 => el1.vid==nested._1.vid).head)
        var n2=dom(index2)
        var rit=input.edgeBool(dom(index1),dom(index2),nested._2)
        //println(rit)
        if(!rit){

          return 0
        }
      }
    }
    return 1
  }
}