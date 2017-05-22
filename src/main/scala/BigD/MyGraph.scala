package BigD

import scala.collection.mutable
import scala.collection.mutable.MutableList
/**
  * Created by francesco  and alessandro on 10/03/17.
  *
  *
  **/
class MyGraph() extends Serializable {
  var nodes: MutableList[VertexAF]=MutableList.empty[VertexAF];
  var dfscode: String="" /*Se la stringa è vuota DFSCode non ancora implementato*/
  var maxH=0
  var minH=0
  def addNode(el:VertexAF) {
    nodes+:= el
  }
//prova/
  def minDFS(strugglers:MutableList[(String,String)],inG:MutableList[VertexAF]): String ={
    var source=this.nodes.sortBy(el=>el.vid).head
    var visit=DFSVisit(source,strugglers,inG)
    mergeSorted(visit,0,visit.length-1)
    var s=visitToString(visit)
    return s
//
  }

  def DFSVisit(source: VertexAF,strugglers: MutableList[(String,String)],inG:MutableList[VertexAF]): MutableList[(Int,Int,String,String,String)] ={
    /*Deve ritornare una lista di edges */
    var visit: MutableList[(Int,Int,String,String,String)]=MutableList.empty[(Int,Int,String,String,String)]
    var time=0
    var discovery=collection.mutable.Map[String,Int]()
    var couple= MutableList.empty[(String,String)]
    discovery(source.vid)=time
    //println("INIZIO DA "+source.vid)
    DFS(discovery,source,time+1,visit,couple,strugglers,inG)
    //print(visit.mkString("\n"))
    return visit

  }

  /*Label uniche -> unica visita DFS*/
  /*Impostare anzichè il time la lunghezza dei nodi scoperti*/
  def DFS(discovery: collection.mutable.Map[String,Int],node: VertexAF,time:Int,visit :MutableList[(Int,Int,String,String,String)],couple: MutableList[(String,String)],strugglers: MutableList[(String,String)],inG: MutableList[VertexAF]) {
    var sorted_neighb=node.adjencies.sortBy(el=>el._1.vid)
    //println("SONO IN "+node.vid)
    for(cand <- sorted_neighb){
     // println("HO "+cand._1.vid)
      if(!discovery.keys.exists(p => p==cand._1.vid)) {
        discovery(cand._1.vid) = discovery.count(el => true)
        couple.+=:(node.vid,cand._1.vid) //padre-> figlio
        couple.+=:(cand._1.vid,node.vid) //padre->
        var adJAtt=inG.filter( el => el.vid==node.vid).head.adjencies.filter( el => el._1.vid==cand._1.vid)
       // println("minchia pure")
        if(adJAtt.length>0){
          visit.+=:((discovery(node.vid),discovery(cand._1.vid),node.vid,cand._2,cand._1.vid))
          if(strugglers.contains(cand._1.vid,node.vid)){
            visit.+=:((discovery(cand._1.vid),discovery(node.vid),cand._1.vid,cand._2,node.vid))
          }

        }
        else{
          visit.+=:((discovery(cand._1.vid),discovery(node.vid),cand._1.vid,cand._2,node.vid))
        }
        //visit.+=:((discovery(node.vid), discovery(cand._1.vid), node.vid, cand._2, cand._1.vid)) //Mancano i reverse
        DFS(discovery,cand._1,time+1,visit,couple,strugglers,inG)
      }
      else{
        if(!couple.contains(node.vid,cand._1.vid) && !couple.contains(cand._1.vid,node.vid)){
          couple.+=:(node.vid,cand._1.vid) //padre-> figlio
          couple.+=:(cand._1.vid,node.vid) //padre-> figlio
          /*Devo verificare se forwared edges o backedges nel grafo originale*/
          //Non ho però il riferimento al grafo originale
          //println("BACKINTHEDAYS")
          var adJAtt=inG.filter( el => el.vid==node.vid).head.adjencies.filter( el => el._1.vid==cand._1.vid)
          if(adJAtt.length>0){
            visit.+=:((discovery(node.vid),discovery(cand._1.vid),node.vid,cand._2,cand._1.vid))
          }
          else{
            visit.+=:((discovery(cand._1.vid),discovery(node.vid),cand._1.vid,cand._2,node.vid))
          }
        }
        else{
          if(strugglers.contains(cand._1.vid,node.vid)){
            visit.+=:((discovery(cand._1.vid),discovery(node.vid),cand._1.vid,cand._2,node.vid))
          }
        }
      }

    }
  }


  //Ordine lessicografico non viene utilizzato -> quindi solo sorting con backedges and frontedges
  def mergeSorted(visit:MutableList[(Int,Int,String,String,String)],head:Int,tail:Int) {
    if(head<tail) {
      var mid = (head + tail) / 2
      mergeSorted(visit,head,mid)
      mergeSorted(visit,mid+1,tail)
      merge(visit,head,mid,tail)
    }

  }
  /*unique source point -> sorting the list*/
  def merge(visit:MutableList[(Int,Int,String,String,String)],head:Int,mid:Int,tail:Int): Unit ={
    var i=head;
    var j=mid+1;
    var app:MutableList[(Int,Int,String,String,String)]=MutableList.empty[(Int,Int,String,String,String)]
    while(i<=mid && j<=tail){
      if(confrontEdges(visit.get(i).head,visit.get(j).head)){
        app:+=visit(i)
        i+=1
      }
      else{
        app:+=visit(j)
        j+=1
      }
    }
    while(i<=mid) {
      app:+=visit(i)
      i += 1
    }
    while(j<=tail) {
      app:+=visit(j)
      j += 1
    }
    var k=head
    while(k<=tail) {
      visit(k) = app(k - head)
      k += 1
    }

  }

  //TODO check it !!!!
  def confrontEdges(a:(Int,Int,String,String,String),b:(Int,Int,String,String,String)): Boolean = {
    if (a._1 < a._2 && b._1 < b._2 && a._2 < b._2) {
      //forwardedges
      return true
    }
    if (a._1 > a._2 && b._1 > b._2 && ( a._1 < b._1 || (a._1 == b._1 && a._2 < b._2))) {
        return true
    }
    else {
      if (a._1 > a._2 && b._1 < b._2) {
        //A backedges and B forwardedges
        if (a._1 < b._2) {
          return true;
        }
        else {
          if (a._1 < a._2 && b._1 > b._2) {
            //#A forwardedges and B backedges
            if (a._2 <= b._1) {
              return true;
            }
          }
        }
      }
      return false;
    }
  }

  def visitToString(mutableList: MutableList[(Int,Int,String,String,String)]):String ={
    var str=""
    for(el <- mutableList){
        str = str+""+el._1+el._2+el._3+el._4+el._5

    }
    return str
  }


  def toPrinit() {
    for(el <- nodes){
      el.toPrint()
    }
  }

  /*def toFlat(): MutableList[(String,String)] ={
    var ris:MutableList[(String,String)]=MutableList.empty[(String,String)]
    for(el <-this.nodes){
      for(el1 <- el.adjencies){
        ris+:=(el.vid,el1._1.vid)
      }
    }
    return ris
  }*/


  def myclone(): MyGraph = {
    var ret:MyGraph=new MyGraph()
    for(el <- this.nodes){
      var nod:VertexAF=new VertexAF(el.vid)
      ret.addNode(nod)
    }
    /*Abbiamo inserito tutti i nodi vuoti*/
    for(el  <- this.nodes){
      /*Trovarmi il corrispondente el in ret*/
      var app=ret.nodes.filter( p => (p.vid == el.vid)).head
      for(el1 <- el.adjencies){
        var toAdd=ret.nodes.filter(p =>  (p.vid==el1._1.vid)).head
        app.addEdge(toAdd,el1._2)
      }
    }
    //ret.dfscode=this.dfscode
    return ret
  }

  def allCouples(): MutableList[(String, String,String)] = {
    var couples: MutableList[(String, String, String)] = MutableList.empty[(String, String, String)]
    for (el <- this.nodes) {
      for (nested <- el.adjencies){
        couples+:=(el.vid, nested._1.vid, nested._2) //sorgente destinazione e peso
      }
    }
    return couples
  }


  //TODO verify if it is possible to move this function inside the MyGraph class.
  def makeItUndirect():MyGraph={
    var un_G = new MyGraph();
    var strugglers: mutable.MutableList[(String, String)] = mutable.MutableList.empty[(String, String)]
    var couple: mutable.MutableList[(String, String)] = mutable.MutableList.empty[(String, String)]
    var S: VertexAF = null;
    var D: VertexAF = null;
    var nodes = this.nodes
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
    if (un_G.nodes.length != 0) {
      code = un_G.minDFS(strugglers, this.nodes.clone())
    }
    this.dfscode = code
    return this
  }

}


