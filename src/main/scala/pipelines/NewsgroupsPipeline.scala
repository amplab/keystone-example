package pipelines

import evaluation.MulticlassClassifierEvaluator
import loaders.{LabeledData, NewsgroupsDataLoader}
import nodes.{TreebankTokenizer, SparseVectorCombiner, IDFCommonSparseFeatures}
import nodes.learning.NaiveBayesEstimator
import nodes.nlp._
import nodes.stats.TermFrequency
import nodes.util._
import org.apache.spark.mllib.keystoneml.LogisticRegressionEstimator
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import weka.core.stemmers.{SnowballStemmer, LovinsStemmer}
import workflow.{Pipeline, Transformer, Optimizer}

/**
 * tokenize: only whitespace, whitespace & periods, whitespace & all punctuation

Stopwords: don’t filter, filter before n-grams

n-grams: unigrams, unigrams & bigrams, unigrams & bigrams & trigrams

term freq: x, log(x+1), sqrt(x), 1

# of common features

 */

object NewsgroupsPipeline extends Logging {
  val appName = "ExamplePipeline"

  def run(sc: SparkContext, conf: ExampleConfig) {

    val trainLabeledData = LabeledData(NewsgroupsDataLoader(sc, conf.trainLocation).labeledData.cache())
    val trainData = trainLabeledData.data
    val trainLabels = trainLabeledData.labels
    val stopwords = Set("a", "about", "above", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also","although","always","am","among", "amongst", "amoungst", "amount",  "an", "and", "another", "any","anyhow","anyone","anything","anyway", "anywhere", "are", "around", "as",  "at", "back","be","became", "because","become","becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom","but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven","else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own","part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thickv", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "the")



    val numClasses = NewsgroupsDataLoader.classes.length
    val numExamples = trainData.count()

    val stemmer = new LovinsStemmer()
    // Build the classifier estimator
    logInfo("Training classifier")
    /*val unoptimizedPipeline = Trim andThen
        LowerCase() andThen
        Tokenizer() andThen
        Transformer(_.filterNot(stopwords.contains)) andThen
        NGramsFeaturizer(1 to 2) andThen
        TermFrequency() andThen
        (IDFCommonSparseFeatures(x => math.log(numExamples/x), conf.commonFeatures), trainData) andThen
        (NaiveBayesEstimator(numClasses), trainData, trainLabels) andThen
        MaxClassifier*/
    /*val unigramPipeline = Trim andThen
        LowerCase() andThen
        Tokenizer("[\\s.]+") andThen
        TermFrequency(x => x) andThen
        (IDFCommonSparseFeatures(x => math.log(numExamples/x), conf.commonFeatures), trainData)
    val bigramPipeline = Trim andThen
        LowerCase() andThen
        Tokenizer("[\\s.]+") andThen
        //Transformer(_.filterNot(stopwords.contains)) andThen
        NGramsFeaturizer(2 to 2) andThen
        TermFrequency(x => x) andThen
        (IDFCommonSparseFeatures(x => math.log(numExamples/x), conf.commonFeatures), trainData)

    val unoptimizedPipeline = Pipeline.gather(Seq(unigramPipeline, bigramPipeline)) andThen
        SparseVectorCombiner() andThen new Cacher() andThen
        (LogisticRegressionEstimator(numClasses, regParam = 0.1, numIters = 40), trainData, trainLabels) andThen
        Transformer(_.toInt)*/
    val unoptimizedPipeline = Tokenizer("[\\s]+") andThen
        TermFrequency(x => x) andThen
        (IDFCommonSparseFeatures(x => math.log(numExamples/x), conf.commonFeatures), trainData) andThen
        (LogisticRegressionEstimator(numClasses, regParam = 5, numIters = 20), trainData, trainLabels)


    val predictor = Optimizer.execute(unoptimizedPipeline)

    // Evaluate the classifier
    logInfo("Evaluating classifier")

    val trainResults = predictor(trainData)
    val trainEval = MulticlassClassifierEvaluator(trainResults, trainLabels, numClasses)

    val testData = LabeledData(NewsgroupsDataLoader(sc, conf.testLocation).labeledData.cache())
    val testLabels = testData.labels
    val testResults = predictor(testData.data)
    val testEval = MulticlassClassifierEvaluator(testResults, testLabels, numClasses)

    logInfo("\nTrain Eval:\n" + trainEval.summary(NewsgroupsDataLoader.classes))
    logInfo("\nTest Eval:\n" + testEval.summary(NewsgroupsDataLoader.classes))
  }

  case class ExampleConfig(
    trainLocation: String = "",
    testLocation: String = "",
    commonFeatures: Int = 30000)

  def parse(args: Array[String]): ExampleConfig = new OptionParser[ExampleConfig](appName) {
    head(appName, "0.1")
    opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
    opt[String]("testLocation") required() action { (x,c) => c.copy(testLocation=x) }
    opt[Int]("commonFeatures") action { (x,c) => c.copy(commonFeatures=x) }
  }.parse(args, ExampleConfig()).get

  /**
   * The actual driver receives its configuration parameters from spark-submit usually.
   * @param args
   */
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName(appName)
    conf.setIfMissing("spark.master", "local[4]") // This is a fallback if things aren't set via spark submit.

    val sc = new SparkContext(conf)

    val appConfig = parse(args)
    run(sc, appConfig)

    sc.stop()
  }

}
