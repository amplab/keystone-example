package pipelines

import breeze.stats.distributions.{RandBasis, ThreadLocalRandomGenerator}
import evaluation.MulticlassClassifierEvaluator
import loaders.{CsvDataLoader, LabeledData}
import nodes.learning.BlockLeastSquaresEstimator
import nodes.stats.{LinearRectifier, PaddedFFT, RandomSignNode}
import nodes.util._
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.{SparkConf, SparkContext}
import pipelines._
import scopt.OptionParser
import workflow.Pipeline


object MnistPipeline extends Serializable with Logging {
  val appName = "MnistRandomFFT"

  def run(sc: SparkContext, conf: MnistRandomFFTConfig) {
    // This is a property of the MNIST Dataset (digits 0 - 9)
    val numClasses = 10

    val randomSignSource = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(conf.seed)))

    // The number of pixels in an MNIST image (28 x 28 = 784)
    // Because the mnistImageSize is 784, we get 512 PaddedFFT features per FFT.
    val mnistImageSize = 784

    val startTime = System.nanoTime()

    val train = LabeledData(
      CsvDataLoader(sc, conf.trainLocation, conf.numPartitions)
        // The pipeline expects 0-indexed class labels, but the labels in the file are 1-indexed
        .map(x => (x(0).toInt - 1, x(1 until x.length)))
          .cache())
    val labels = ClassLabelIndicatorsFromIntLabels(numClasses).apply(train.labels)

    val featurizer = Pipeline.gather {
      Seq.fill(conf.numFFTs) {
        RandomSignNode(mnistImageSize, randomSignSource) andThen PaddedFFT() andThen LinearRectifier(0.0)
      }
    } andThen VectorCombiner()

    val pipeline = featurizer
        .andThen(new BlockLeastSquaresEstimator(conf.blockSize, 1, conf.lambda.getOrElse(0)), train.data, labels)
        .andThen(MaxClassifier)

    // Train the model
    val model = pipeline

    val test = LabeledData(
      CsvDataLoader(sc, conf.testLocation, conf.numPartitions)
        // The pipeline expects 0-indexed class labels, but the labels in the file are 1-indexed
        .map(x => (x(0).toInt - 1, x(1 until x.length)))
          .cache())

    // Calculate train error
    val trainEval = MulticlassClassifierEvaluator(model(train.data), train.labels, numClasses)
    // Calculate test error
    val testEval = MulticlassClassifierEvaluator(model(test.data), test.labels, numClasses)

    logInfo("\nTrain Eval:\n" + trainEval.summary((0 to 9).toArray.map(_.toString)))
    logInfo("\nTest Eval:\n" + testEval.summary((0 to 9).toArray.map(_.toString)))

    val endTime = System.nanoTime()
    logInfo(s"Pipeline took ${(endTime - startTime)/1e9} s")
  }

  case class MnistRandomFFTConfig(
                                     trainLocation: String = "",
                                     testLocation: String = "",
                                     numFFTs: Int = 4,
                                     blockSize: Int = 2048,
                                     numPartitions: Int = 10,
                                     lambda: Option[Double] = None,
                                     seed: Long = 0)

  def parse(args: Array[String]): MnistRandomFFTConfig = new OptionParser[MnistRandomFFTConfig](appName) {
    head(appName, "0.1")
    help("help") text("prints this usage text")
    opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
    opt[String]("testLocation") required() action { (x,c) => c.copy(testLocation=x) }
    opt[Int]("numFFTs") action { (x,c) => c.copy(numFFTs=x) }
    opt[Int]("blockSize") validate { x =>
      // Bitwise trick to test if x is a power of 2
      if (x % 512 == 0) {
        success
      } else  {
        failure("Option --blockSize must be divisible by 512")
      }
    } action { (x,c) => c.copy(blockSize=x) }
    opt[Int]("numPartitions") action { (x,c) => c.copy(numPartitions=x) }
    opt[Double]("lambda") action { (x,c) => c.copy(lambda=Some(x)) }
    opt[Long]("seed") action { (x,c) => c.copy(seed=x) }
  }.parse(args, MnistRandomFFTConfig()).get

  /**
   * The actual driver receives its configuration parameters from spark-submit usually.
   * @param args
   */
  def main(args: Array[String]) = {
    val appConfig = parse(args)

    val conf = new SparkConf().setAppName(appName)
    conf.setIfMissing("spark.master", "local[2]")
    val sc = new SparkContext(conf)
    run(sc, appConfig)

    sc.stop()
  }
}
