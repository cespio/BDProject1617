import scala.collection.mutable
import scala.collection.mutable.MutableList
/*
*  BigData project 2017, Frequent Pattern Mining on Labeled Oriented Single Graph
*  @authors { Francesco Contaldo, Alessandro Rizzuto}
*
* */
//Class used to represent a candidate graph
class MyGraph() extends Serializable {
  var nodes: MutableList[VertexAF]=MutableList.empty[VertexAF]; /*List of nodes present in the graph*/
  var dfscode: String="" /*empty dfs code, not yet calculated*/
  /*Hour range for edge relation*/
  var maxH:Int=0
  var minH:Int=0
  def addNode(el:VertexAF) {
    nodes+:= el
  }

  def minDFS(strugglers:MutableList[(String,String)],inG:MutableList[VertexAF]): String ={
    var source=this.nodes.sortBy(el=>el.vid).head
    var visit=DFSVisit(source,strugglers,inG)
    /*Using of a costum total order among the edges*/
    var visit1=visit.sortWith((a,b) => (a._1<b._1 || (a._1==b._1 && a._2<b._2)))
    var s=visitToString(visit1)
    return s
  }

  //DFSvisit
  def DFSVisit(source: VertexAF,strugglers: MutableList[(String,String)],inG:MutableList[VertexAF]): MutableList[(Int,Int,String,String,String)] ={
    //DFS vitisit to return all the edges withe relative order
    var visit: MutableList[(Int,Int,String,String,String)]=MutableList.empty[(Int,Int,String,String,String)]
    var time=0
    var discovery=collection.mutable.Map[String,Int]()
    var couple= MutableList.empty[(String,String)]
    discovery(source.vid)=time
    DFS(discovery,source,time+1,visit,couple,strugglers,inG)
    return visit

  }

  //DFSvisit
  def DFS(discovery: collection.mutable.Map[String,Int],node: VertexAF,time:Int,visit :MutableList[(Int,Int,String,String,String)],couple: MutableList[(String,String)],strugglers: MutableList[(String,String)],inG: MutableList[VertexAF]) {
    var sorted_neighb=node.adjencies.sortBy(el=>el._1.vid)
    for(cand <- sorted_neighb){
      if(!discovery.keys.exists(p => p==cand._1.vid)) {
        discovery(cand._1.vid) = discovery.count(el => true)
        //Used to store catch what edges has been discovered
        couple.+=:(node.vid,cand._1.vid)
        couple.+=:(cand._1.vid,node.vid)
        var adJAtt=inG.filter( el => el.vid==node.vid).head.adjencies.filter( el1 => el1._1.vid==cand._1.vid && el1._2==cand._2)
        if(adJAtt.length>0){
          visit.+=:((discovery(node.vid),discovery(cand._1.vid),node.vid,cand._2,cand._1.vid))
          if(strugglers.contains(cand._1.vid,node.vid)){ //struggler contain the edges that advance from makeitundirectional
            var w=inG.filter(el=>el.vid==cand._1.vid).head.adjencies.filter(el=>el._1.vid==node.vid).head._2
            //Adding edge to the visit
            visit.+=:((discovery(cand._1.vid),discovery(node.vid),cand._1.vid,w,node.vid))
          }
        }
        else{
          //Adding edge to the visit
          visit.+=:((discovery(cand._1.vid),discovery(node.vid),cand._1.vid,cand._2,node.vid))
        }
        DFS(discovery,cand._1,time+1,visit,couple,strugglers,inG)
      }
      else{
        //Look for backward edges
        if(!couple.contains(node.vid,cand._1.vid) && !couple.contains(cand._1.vid,node.vid)){
          couple.+=:(node.vid,cand._1.vid)
          couple.+=:(cand._1.vid,node.vid)
          var adJAtt=inG.filter( el => el.vid==node.vid).head.adjencies.filter( el1 => el1._1.vid==cand._1.vid && el1._2==cand._2)
          if(adJAtt.length>0){
            visit.+=:((discovery(node.vid),discovery(cand._1.vid),node.vid,cand._2,cand._1.vid))
          }
          else{
            visit.+=:((discovery(cand._1.vid),discovery(node.vid),cand._1.vid,cand._2,node.vid))
          }
        }
        else{
          if(strugglers.contains(cand._1.vid,node.vid)){
            //Look for backward strugglers
            var w=inG.filter(el=>el.vid==cand._1.vid).head.adjencies.filter(el=>el._1.vid==node.vid).head._2
            visit.+=:((discovery(cand._1.vid),discovery(node.vid),cand._1.vid,w,node.vid))
          }
        }
      }

    }
  }

  /*unique source point -> sorting the list*/
  def mergeSorted(visit:MutableList[(Int,Int,String,String,String)],head:Int,tail:Int) {
    if(head<tail) {
      var mid = (head + tail) / 2
      mergeSorted(visit,head,mid)
      mergeSorted(visit,mid+1,tail)
      merge(visit,head,mid,tail)
    }

  }

  def merge(visit:MutableList[(Int,Int,String,String,String)],head:Int,mid:Int,tail:Int): Unit ={
    var i=head;
    var j=mid+1;
    var app:MutableList[(Int,Int,String,String,String)]=MutableList.empty[(Int,Int,String,String,String)]
    while(i<=mid && j<=tail){
      if(confrontEdges(visit.get(i).head,visit.get(j).head)){
        app+=visit(i)
        i+=1
      }
      else{
        app+=visit(j)
        j+=1
      }
    }
    while(i<=mid) {
      app+=visit(i)
      i += 1
    }
    while(j<=tail) {
      app+=visit(j)
      j += 1
    }
    var k=head
    while(k<=tail) {
      visit(k)=app(k - head)
      k+=1
    }

  }

  //Partially ordered relation, it has been used simple lexicographical order instead
  def confrontEdges(a:(Int,Int,String,String,String),b:(Int,Int,String,String,String)): Boolean = {
    if (a._1 < a._2 && b._1 < b._2 && a._2 < b._2) {
      return true
    }
    if (a._1 > a._2 && b._1 > b._2) {
      if( (a._1 < b._1) || (a._1==b._1 && a._2 < b._2)) {
        return true
      }
    }
    else {
      if (a._1 > a._2 && b._1 < b._2) {
        if (a._1 < b._2) {
          return true
        }
        else {
          if (a._1 < a._2 && b._1 > b._2) {
            if (a._2 <= b._1) {
              return true
            }
          }
        }
      }
    }
    return false
  }


  //Transforming the visit to string
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


  //create a clone of the current object
  def myclone(): MyGraph = {
    var ret:MyGraph=new MyGraph()
    ret.minH=this.minH
    ret.maxH=this.maxH
    for(el <- this.nodes){
      var nod:VertexAF=new VertexAF(el.vid)
      ret.addNode(nod)
    }
    for(el  <- this.nodes){
      var app=ret.nodes.filter( p => (p.vid == el.vid)).head
      for(el1 <- el.adjencies){
        var toAdd=ret.nodes.filter(p =>  (p.vid==el1._1.vid)).head
        app.addEdge(toAdd,el1._2)
      }
    }
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


  //Makes the node undirected and use the result of the transormation to calculate the DFScode
  def makeItUndirect():MyGraph={
    var un_G = new MyGraph();
    var strugglers: mutable.MutableList[(String, String)] = mutable.MutableList.empty[(String, String)]
    var couple: mutable.MutableList[(String, String)] = mutable.MutableList.empty[(String, String)]
    var S: VertexAF = null;
    var D: VertexAF = null;
    var nodes = this.nodes
    for (el <- nodes.sortBy(el=>el.vid)) {

      if (un_G.nodes.count(f => f.vid == el.vid) >= 1) {
        S = un_G.nodes.filter(f => f.vid == el.vid).head
      }
      else {
        S = new VertexAF(el.vid)
        un_G.addNode(S)
      }
      for (el1 <- el.adjencies.sortBy(el=>el._1.vid)) {
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
            //if is present a cycle, remember it for the DFSvisit
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
      //Perform the DFSCode
      code = un_G.minDFS(strugglers, this.nodes.clone())
    }
    this.dfscode = code
    return this
  }

}
