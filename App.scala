import java.io.File

import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}
import scala.util.matching.Regex
import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.io.BufferedWriter
import java.io.FileWriter
import scala.sys.process._
import java.io.IOException
import java.io.IOException
import java.io.PrintWriter
/*
*  BigData project 2017, Frequent Pattern Mining on Labeled Oriented Single Graph
*  @authors { Francesco Contaldo, Alessandro Rizzuto}
*
* */


object App extends Serializable{

  def main(args : Array[String]) {
    /*SparkConfiguration*/
    val conf = new SparkConf().setAppName("BigD")//.set("spark.shuffle.compress","false")
    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    var testG=new MyGraph()
    /*Acquiring the input*/
    println("Give me the threshold -> ")
    val thr=Console.readInt()
    println("Give me the size threshold-> ")
    val size=Console.readInt()
    /*Creating the graph from the input dot file*/
    var graph=builtGraphfromFile("data/graphGenOut.dot")
    val frequentO=new FrequentSubG(graph,thr,size)
    /*RDD containing the frequentedges*/
    var frequentEdges: RDD[(String,String,String)]=frequentEdgesAPP(graph,sc,thr)
    /*First step generate the candidate with size at least two edges*/
    var candidateGen:RDD[MyGraph]=frequentO.candidateGeneration(frequentEdges)
    /*Clearing the one with the double DFSCode Exploiting MAP REDUCE*/
    candidateGen=candidateGen.map(el=>(el.dfscode,el)).reduceByKey((a,b)=>a).map(el=>el._2)
    /*Map Reduce used to perform the solution of the CSP problem*/
    var ris1=candidateGen.map(el => (el.dfscode, graph, el, (frequentO.CSPMapReduce(graph, el)))).flatMap(el => el._4.map(a => (el._1, el._2, el._3, a))).map(el => (el._1, frequentO.checkGraph(el._3, el._4, el._2)))
    ris1=ris1.reduceByKey((x, y) => x + y).filter(el=>el._2>= thr)
    /*Updating the new candiateGen*/
    candidateGen=candidateGen.map(el => (el.dfscode, el)).join(ris1).map(ris => ris._2._1)
    /*Number of iterations at least two*/
    var itera=2
    var candidate2:RDD[MyGraph]=null
    var candidatePre:RDD[MyGraph]=candidateGen
    frequentEdges.cache() //Caching of the frequent edges
    while(itera<size && candidatePre.count()!=0){
      candidate2=frequentO.newExtension(candidateGen,frequentEdges) //Generates the new candidates
      candidate2=candidate2.map(el=>el.makeItUndirect()).map(el=>(el.dfscode,el)).reduceByKey((a,b)=>a).map(el=>el._2) //Calculate all the new dfscode
      //MapReduce for CSP
      ris1=candidate2.map(el => (el.dfscode, graph, el, (frequentO.CSPMapReduce(graph, el)))).flatMap(el => el._4.map(a => (el._1, el._2, el._3, a))).map(el => (el._1, frequentO.checkGraph(el._3, el._4, el._2)))
      ris1=ris1.reduceByKey((x, y) => x + y).filter(el=>el._2>= thr) //Discarding the ones that do not overcome the threshold
      candidatePre=candidate2.map(el => (el.dfscode, el)).join(ris1).map(ris => ris._2._1)
      if(candidatePre.count()!=0)
        candidateGen=candidatePre
      itera=itera+1
    }
    println("NUMBER OF TOTAL SOLUTIONS -> "+candidateGen.count())
    //ris1.unpersist()
    //candidate2.unpersist()
    //candidatePre.unpersist()
    //frequentEdges.unpersist()
    var checkPresence = new java.io.File("Results").exists
    if (checkPresence == true)
      Seq("sh", "-c", "rm -r Results").!!
    var d : File = new File("Results")
    d.mkdir()

    ///Merging the results
    var results = candidateGen.collect().sortBy(x => x.dfscode) //Collecting the results
    var added = collection.mutable.Set[String]()
    var index = 0
    var flag  = 0
    for (i <- Range(0,results.length)){
        flag=0
        var tmp=List.empty[MyGraph]
        if(!added.contains(results(i).dfscode)) {
          added.add(results(i).dfscode)
          tmp = tmp ++ List(results(i))
          for (j <- Range(i + 1, results.length)) {
            if (!added.contains(results(j).dfscode)) {
              var jarJar = jaroSimilarity(results(i).dfscode, results(j).dfscode)
              if ((jarJar >= 0.9 && itera == 3) || (jarJar >= 0.8 && itera > 3)) {
                flag=1
                added.add(results(j).dfscode)
                tmp = tmp ++ List(results(j))
              }
            }
          }
          var toP = merging(tmp)
          printSolution(toP,index)
          index+=1
        }
    }
    println("NUMBER OF MERGED SOLUTIONS -> "+index)
  }

  //Function that is responsible for the calculation of the frequent edges, using an hashmap
  def frequentEdgesAPP(graph:MyGraphInput,sc:SparkContext,thr:Int): RDD[(String, String, String)] = {
    var frequent=scala.collection.mutable.HashMap.empty[(String,String,String),Int]
    for(k <- graph.nodes){
      for(el <- k.adjencies){
        var edge:(String,String,String)=(k.label,el._1.label,el._2)
        if(edge._1!=edge._2) {
          if (frequent.contains(edge) == false)
            frequent += (edge -> 1)
          else {
            var n = frequent(edge) + 1
            frequent.update(edge, n)
          }
        }
      }
    }
    var frequentArr=frequent.filter(p=>p._2>=thr).keys.toArray.clone()
    var temp1=sc.parallelize(frequentArr)
    return temp1
  }

  //Construct the graph from the file, no more with graphX but exploiting the new class vertexAfInput and MyGraphInput
  def builtGraphfromFile(fileName :String): MyGraphInput ={
    var graphIn:MyGraphInput=new MyGraphInput()
    var nodeS:VertexAFInput=null
    var nodeD:VertexAFInput=null
    val patternE=new Regex("[0-9]+ [\\[label=]")
    for(line <- scala.io.Source.fromFile("data/graphGenOut.dot").getLines()){
      if(line!="digraph prova{" && line!="}"){
        if(patternE.findFirstMatchIn(line)==None){
          var splitted=line.trim().split("[^0-9]+")
          if (graphIn.keyPres(splitted(0))==false) { //if not present in the input graph
            nodeS=new VertexAFInput(splitted(0),"label")
            graphIn.addNode(nodeS)
          }
          else
            nodeS=graphIn.getNode(splitted(0))
          if (graphIn.keyPres(splitted(1))==false){//if false,mean that the nodes does not exist
            nodeD=new VertexAFInput(splitted(1),"label")
            graphIn.addNode(nodeD)
          }
          else
            nodeD=graphIn.getNode(splitted(1))
          nodeS.addEdge(nodeD,splitted(2))
        }
        else {
          var splitted = line.trim().split(" ")
          nodeD=graphIn.getNode(splitted(0))
          nodeD.label=splitted(5)
        }
      }
    }
    return graphIn
  }

  def printSolution(graph: MyGraph, inde : Long) {

    try {
      val writer = new PrintWriter("Results/ris"+inde+".dot", "UTF-8")
      for(el <- graph.nodes) {
        for (el1 <- el.adjencies) {
          var content = graph.nodes.indexOf(el) + " -> " + graph.nodes.indexOf(el1._1) + " [label=\"" + el1._2 + "\"]\n"
          writer.println(content)
        }
      }
      writer.close()
    } catch {
      case e: IOException => print("ERROR")
    }
  }

  //Function that performs jaro similarity
  def jaroSimilarity(dfs1 : String, dfs2 : String): Double ={
    var lenS : Int = dfs1.length()
    var lenT : Int = dfs2.length()
    if (lenS == 0 && lenT == 0)
      return 1
    var match_distance  = (Math.max(lenS, lenT) / 2) - 1
    var s_matches = new Array[Boolean](lenS)
    var t_matches = new Array[Boolean](lenT)
    var matches = 0
    var transpositions = 0
    for (i <- Range(0,lenS)) {
      var start = Math.max(0, i - match_distance)
      var end = Math.min(i + match_distance + 1, lenT)
      var j : Int = 0
      var skip = false
      breakable {
        for (j <- Range(start, end)) {
          skip = false
          if (t_matches(j) == true)
            skip = true
          if (dfs1(i) != dfs2(j))
            skip = true
          if (!skip) {
            s_matches(i) = true
            t_matches(j) = true
            matches += 1
            break
          }
        }
      }
    }
    if (matches == 0)
      return 0
    else{
      var k = 0
      var skip = false
      for (i <- Range(0,lenS)) {
        skip = false
        if (s_matches(i) == false)
         skip =  true
        if(!skip) {
          while (!t_matches(k)) {
            k += 1
          }
          if (dfs1(i) != dfs2(k))
            transpositions += 1
          k += 1
        }
      }
      return ((matches.toDouble / lenS.toDouble) + (matches.toDouble / lenT.toDouble) + ((matches - transpositions.toFloat/2).toDouble / matches.toDouble)) / 3
    }
  }

  //Function that create a MultiGraph joining together multiple graphs, expolit a HashMap for storing the arcs
  def merging(toBeMerged: List[MyGraph] ): MyGraph = {
    var merged = scala.collection.mutable.HashMap.empty[String, List[(String, String)]]
    for (k <- toBeMerged) {
      for (t <- k.nodes){
        var sorg = t.vid
        for (z <- t.adjencies){
          var dest:(String, String) = (z._1.vid, z._2)
          if(!merged.contains(dest._1)){
            merged += (dest._1 -> List.empty[(String,String)])
          }

          if (merged.contains(sorg) == false){
            merged += (sorg -> List(dest))
          }
          else{
            var flag = 0
            var values = merged(sorg)
            for (i <- values) {
              if (i == dest)
                flag == 1
            }
            if (flag != 1){
              var newel=List(dest)++values
              merged += (sorg -> newel.distinct)
            }
          }
        }
      }
    }
    var results = new MyGraph()
    for (d <- merged.keys){
      var newNode = new VertexAF(d)
      results.addNode(newNode)
    }
    for (l <- merged.keys){
      var sorg = results.nodes.filter(el => el.vid == l).head
      var l1 = merged(l)
      for(g <- l1) {
        var des = results.nodes.filter(el => el.vid == g._1).head
        sorg.addEdge(des, g._2)
      }
    }
    return results
  }


}

/*var jarJarA = jarJar.map(el => (el._1.dfscode, el._2.dfscode, jaroSimilarity(el._1.dfscode, el._2.dfscode), el._1, el._2)).filter(app => (app._3 >= 0.8 && itera > 3) || (app._3 >= 0.9 && itera == 3)).flatMap(el => List((el._1, List(el._4, el._5)), (el._2, List(el._4, el._5))))
      println("jarJArAAA length "+jarJarA.count())
      jarJarA.unpersist()
      var rdbk = jarJarA.reduceByKey((x, y) => x ++ y).map(el => merging(el._2))//.map(el=> (el.sortBy(_.dfscode),""))//.map(el => merging(el._2))
      //rdbk.collect().foreach(el => {print("INIZIO \n");el._1.foreach(el=> print(" "+el.dfscode+" "));print("FINE")})
     // var rdbk1 = rdbk.reduceByKey((x,y) => x).map( el => el._1).map( el => merging(el))
      var rdbk1 = rdbk.collect()
      println("NUMBER OF MERGED SOLUTIONS -> " + rdbk1.length)
      if(rdbk1.length!=0){
        for(el <- Range(0,rdbk1.length)){
          printSolution(rdbk1(el),el)
        }
        //rdbk.map( el =>  printSolution(el,100))
        //var t= rdbk.collect().foreach( el =>  printSolution(el,100))
      }
      else {
        val app = candidateGen.zipWithIndex()
        app.map(el => printSolution(el._1, el._2))
      }*/

//var jarJar = candidateGen.cartesian(candidateGen).filter( copp => copp._1.dfscode < copp._2.dfscode)
//println("jarJAr length "+jarJar.count())
