package similarity;

import model.{EdgeProperty, VertexProperty}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Jaccard {

  def jaccardSimilarityOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: Long): List[Long] = {
    /** 
    Given a patient ID, compute the Jaccard similarity w.r.t. to all other patients. 
    Return a List of patient IDs ordered by the highest to the lowest similarity.
    For ties, random order is okay
    */

    val neighborEvents=graph.collectNeighborIds(EdgeDirection.Out)

    val allpatientneighbor=neighborEvents.filter(f=> f._1.toLong <= 1000 & f._1.toLong != patientID)

    val thispatientneighborset=neighborEvents.filter(f=>f._1.toLong==patientID).map(f=>f._2).flatMap(f=>f).collect().toSet

    val patientscore=allpatientneighbor.map(f=>(f._1,jaccard(thispatientneighborset,f._2.toSet)))

    patientscore.takeOrdered(10)(Ordering[Double].reverse.on(x=>x._2)).map(_._1.toLong).toList



    /** Remove this placeholder and implement your code */

  }

  def jaccardSimilarityAllPatients(graph: Graph[VertexProperty, EdgeProperty]): RDD[(Long, Long, Double)] = {

    val sc = graph.edges.sparkContext
    val neighborEvents=graph.collectNeighborIds(EdgeDirection.Out)

    val allpatientneighbor=neighborEvents.filter(f=> f._1.toLong <= 1000)
    val cartesianneighbor=allpatientneighbor.cartesian(allpatientneighbor).filter(f=>f._1._1<f._2._1)
    cartesianneighbor.map(f=>(f._1._1,f._2._1,jaccard(f._1._2.toSet,f._2._2.toSet)))


  }

  def jaccard[A](a: Set[A], b: Set[A]): Double = {
    if (a.isEmpty || b.isEmpty){return 0.0}
    a.intersect(b).size/a.union(b).size.toDouble
  }
}
