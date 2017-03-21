package BigD

import scala.collection.mutable.MutableList
/**
  * Created by francesco on 10/03/17.
  * AGGIUNGERE DFSCODE
  * AGGIUNGERE L'estesione supplementare
  */
class MyGraph() extends  Serializable{
  var nodes: MutableList[VertexAF]=MutableList.empty[VertexAF];
  def addNode(el:VertexAF) {
    nodes+:= el
  }

  def toPrinit() {
    for(el <- nodes){
      el.toPrint()
    }
  }

  //TODO verificare correttezza
  def DFSVisit(source: VertexAF): MutableList[(Int,Int,String,String,String)] ={
    /*Deve ritornare una lista di edges */
    var visit: MutableList[(Int,Int,String,String,String)]=MutableList.empty[(Int,Int,String,String,String)]
    var time=0
    var discovery=collection.mutable.Map[String,Int]()
    var couple= MutableList.empty[(String,String)]
    discovery(source.vid)=time
    println("INIZIO DA "+source.vid)
    DFS(discovery,source,time,visit,couple)
    print(visit.mkString("\n"))
    return visit

  }

  /*Label uniche -> unica visita DFS*/
  def DFS(discovery: collection.mutable.Map[String,Int],node: VertexAF,time:Int,visit :MutableList[(Int,Int,String,String,String)],couple: MutableList[(String,String)]): Int= {
    var ntime=0
    discovery(node.vid)=time
    ntime=time
    //println("Sono in -> "+node.vid)
    var sorted_neighb=node.adjencies.sortBy(el=>el._1.vid)
    //sorted_neighb.foreach(el => println(el._1.vid))
    for(cand <- sorted_neighb){
      if(!discovery.keys.exists(p => p==cand._1.vid)) {

        //println("Ho scoperto -> "+cand._1.vid)
        discovery(cand._1.vid) = ntime
        couple.+=:(node.vid,cand._1.vid) //padre-> figlio
        couple.+=:(cand._1.vid,node.vid) //padre-> figlio
        visit.+=:((ntime, ntime + 1, node.vid, cand._2, cand._1.vid))
        ntime=DFS(discovery, cand._1,ntime+1,visit,couple)
      }
      else{
        if(!couple.contains(node.vid,cand._1.vid)){
          visit.+=:((discovery(node.vid),discovery(cand._1.vid),node.vid,cand._2,cand._1.vid))
        }
      }

    }
    return time
  }

  def minDFS(): String ={
    var source=this.nodes.sortBy(el=>el.vid).head
    //println("NEW FUCKING VISIT")
    var visit=DFSVisit(source)
    mergeSorted(visit,0,visit.length-1)
    var s=visitToString(visit)
    //println("\nDFSCODE -> "+s)
    return s

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
  //TODO Risolvere condizione con TODO || Junit to Boolean
  def confrontEdges(a:(Int,Int,String,String,String),b:(Int,Int,String,String,String)): Boolean = {
    if (a._1 < a._2 && b._1 < b._2 && a._2 < b._2) {
      //forwardedges
      return true;
    }
    /*if (a._1 > a._2 && b._1 > b._2) {
      if (a._1 < b._1 || a._1 == b._1){ //&& a._2 < b._2) {
        return true;
      }
    }*/
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
}


