package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Elem, Tuple}
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.ListBuffer

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Filter]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Filter protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    condition: RexNode
) extends skeleton.Filter[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  /**
    * Function that, evaluates the predicate [[condition]]
    * on a (non-NilTuple) tuple produced by the [[input]] operator
    */
  lazy val predicate: Tuple => Boolean = {
    val evaluator = eval(condition, input.getRowType)
    (t: Tuple) => evaluator(t).asInstanceOf[Boolean]
  }

  /**
   * @inheritdoc
   */
  def execute(): IndexedSeq[Column] = {

    var selectVector = ListBuffer[Boolean]()

    var columns = input.execute()
    columns = columns.dropRight(1)
    // all columns are the same size, so just taking size of the first one
    for (i <- columns(0).indices){
      var t : Tuple = IndexedSeq[Elem]()
      for (c <- columns) {
        t = t :+ c(i)
      }
      if (predicate(t)) {
        selectVector = selectVector += true
      }else {
        selectVector = selectVector += false
      }
    }

    columns = columns :+ selectVector.toIndexedSeq

    columns
  }
}
