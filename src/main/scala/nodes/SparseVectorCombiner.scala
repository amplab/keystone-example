package nodes

import breeze.linalg.SparseVector
import workflow.Transformer

import scala.reflect.ClassTag

case class SparseVectorCombiner[T : ClassTag]()(implicit zero: breeze.storage.Zero[T])
    extends Transformer[Seq[SparseVector[T]], SparseVector[T]] {
  def apply(in: Seq[SparseVector[T]]): SparseVector[T] = SparseVector.vertcat(in:_*)
}
