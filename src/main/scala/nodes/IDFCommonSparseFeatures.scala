package nodes

import breeze.linalg.SparseVector
import nodes.util.SparseFeatureVectorizer
import org.apache.spark.rdd.RDD
import workflow.{Transformer, Estimator}

import scala.reflect.ClassTag

/**
 * An Estimator that chooses the most frequently observed sparse features when training,
 * and produces a transformer which multiplies features by their IDF and builds a sparse vector out of them
 *
 * Deterministically orders the feature mappings first by decreasing number of appearances,
 * then by earliest appearance in the RDD
 *
 * @param idfFunc The function to use to calculate idf from the total number of documents it appeared in
 * @param numFeatures The number of features to keep
 */
case class IDFCommonSparseFeatures[T : ClassTag](idfFunc: Double => Double, numFeatures: Int) extends Estimator[Seq[(T, Double)], SparseVector[Double]] {
  // Ordering that compares (feature, frequency) pairs according to their frequencies
  val ordering = new Ordering[(T, (Int, Long))] {
    def compare(x: (T, (Int, Long)), y: (T, (Int, Long))): Int = {
      if (x._2._1 == y._2._1) {
        x._2._2.compare(y._2._2)
      } else {
        x._2._1.compare(y._2._1)
      }
    }
  }

  override def fit(data: RDD[Seq[(T, Double)]]): IDFFeatureVectorizer[T] = {
    val featureOccurrences = data.flatMap(identity).zipWithUniqueId().map(x => (x._1._1, (1, x._2)))
    // zip with unique ids and take the smallest unique id for a given feature to get
    // a deterministic ordering
    val featureFrequenciesWithUniqueId = featureOccurrences.reduceByKey {
      (x, y) => (x._1 + y._1, Math.min(x._2, y._2))
    }
    val mostCommonFeatures = featureFrequenciesWithUniqueId.top(numFeatures)(ordering)
    val featureSpace = mostCommonFeatures.zipWithIndex.map(x => (x._1._1, (x._2, idfFunc(x._1._2._1)))).toMap
    new IDFFeatureVectorizer(featureSpace)
  }
}

/** A transformer which given a feature space, maps features of the form (feature id, value)
 * into a sparse vector of (feature index, value * feature IDF)
 */
class IDFFeatureVectorizer[T](featureSpace: Map[T, (Int, Double)]) extends Transformer[Seq[(T, Double)], SparseVector[Double]] {
  private def transformVector(in: Seq[(T, Double)], featureSpaceMap: Map[T, (Int, Double)]): SparseVector[Double] = {
    val features = in.map(f => (featureSpaceMap.get(f._1), f._2))
        .filter(_._1.isDefined)
        .map(f => (f._1.get._1, f._2.toDouble * f._1.get._2))
    SparseVector(featureSpaceMap.size)(features:_*)
  }

  override def apply(in: Seq[(T, Double)]): SparseVector[Double] = {
    transformVector(in, featureSpace)
  }
}
