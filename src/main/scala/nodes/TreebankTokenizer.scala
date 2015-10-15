package nodes

import java.io.StringReader
import scala.collection.JavaConversions._

import edu.stanford.nlp.process.{WordTokenFactory, PTBTokenizer}
import workflow.Transformer

case class TreebankTokenizer(options: String = "") extends Transformer[String, Seq[String]] {
  def apply(in: String): Seq[String] = {
    val reader = new StringReader(in)
    val tokenizer = new PTBTokenizer(reader, new WordTokenFactory, options)
    val out = tokenizer.tokenize().map(_.word())
    reader.close()
    out
  }
}
