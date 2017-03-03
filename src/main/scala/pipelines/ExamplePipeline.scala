package keystoneml.pipelines

import keystoneml.evaluation.MulticlassClassifierEvaluator
import keystoneml.loaders.NewsgroupsDataLoader
import keystoneml.nodes.learning.NaiveBayesEstimator
import keystoneml.nodes.nlp._
import keystoneml.nodes.stats.TermFrequency
import keystoneml.nodes.util.{CommonSparseFeatures, MaxClassifier}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import keystoneml.workflow.Optimizer
import breeze.linalg.SparseVector

object ExamplePipeline extends Logging {
  val appName = "ExamplePipeline"

  def run(sc: SparkContext, conf: ExampleConfig) {

    val trainData = NewsgroupsDataLoader(sc, conf.trainLocation)
    val numClasses = NewsgroupsDataLoader.classes.length

    // Build the classifier estimator
    logInfo("Training classifier")

    val predictor = Trim andThen
        LowerCase() andThen
        Tokenizer() andThen
        NGramsFeaturizer(1 to conf.nGrams) andThen
        TermFrequency(x => 1) andThen
        (CommonSparseFeatures[Seq[String]](conf.commonFeatures), trainData.data) andThen
        (NaiveBayesEstimator[SparseVector[Double]](numClasses), trainData.data, trainData.labels) andThen
        MaxClassifier

    // Evaluate the classifier
    logInfo("Evaluating classifier")

    val testData = NewsgroupsDataLoader(sc, conf.testLocation)
    val testLabels = testData.labels
    val testResults = predictor(testData.data)
    val eval = new MulticlassClassifierEvaluator(numClasses).evaluate(testResults, testLabels)

    logInfo("\n" + eval.summary(NewsgroupsDataLoader.classes))
  }

  case class ExampleConfig(
    trainLocation: String = "",
    testLocation: String = "",
    commonFeatures: Int = 100000,
    nGrams: Int = 2)

  def parse(args: Array[String]): ExampleConfig = new OptionParser[ExampleConfig](appName) {
    head(appName, "0.1")
    opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
    opt[String]("testLocation") required() action { (x,c) => c.copy(testLocation=x) }
    opt[Int]("commonFeatures") action { (x,c) => c.copy(commonFeatures=x) }
    opt[Int]("nGrams") action { (x,c) => c.copy(nGrams=x) }
  }.parse(args, ExampleConfig()).get

  /**
   * The actual driver receives its configuration parameters from spark-submit usually.
   * @param args
   */
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName(appName)
    conf.setIfMissing("spark.master", "local[2]") // This is a fallback if things aren't set via spark submit.

    val sc = new SparkContext(conf)

    val appConfig = parse(args)
    run(sc, appConfig)

    sc.stop()
  }

}
