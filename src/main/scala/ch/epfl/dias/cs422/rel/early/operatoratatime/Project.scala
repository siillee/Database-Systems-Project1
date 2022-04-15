package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Project]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Project protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    projects: java.util.List[_ <: RexNode],
    rowType: RelDataType
) extends skeleton.Project[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, projects, rowType)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  /**
    * Function that, when given a (non-NilTuple) tuple produced by the [[input]] operator,
    * it returns a new tuple composed of the evaluated projections [[projects]]
    */
  lazy val evaluator: Tuple => Tuple = {
    eval(projects.asScala.toIndexedSeq, input.getRowType)
  }

  var evaluatedTuples : ListBuffer[Tuple] = ListBuffer[Tuple]()
  var resultColumns : ListBuffer[Column] = ListBuffer[Column]()

  /**
   * @inheritdoc
   */
  def execute(): IndexedSeq[Column] = {

    var columns = input.execute()
    val selectVector = columns.last
    columns = columns.dropRight(1)

    if (columns.isEmpty || columns.head.isEmpty){
      return (resultColumns += IndexedSeq[Elem]()).toIndexedSeq
    }

    // get tuples from columns
    for (i <- columns.head.indices) {
      var t = ListBuffer[Elem]()
      for (c <- columns) {
        t += c(i)
      }
      evaluatedTuples += evaluator(t.toIndexedSeq)
    }

    for (i <- evaluatedTuples.head.indices){
      resultColumns += IndexedSeq[Elem]()
    }

    for (i <- resultColumns.indices){
      for (t <- evaluatedTuples) {
        resultColumns(i) = resultColumns(i) :+ t(i)
      }
    }

    resultColumns += selectVector

    resultColumns.toIndexedSeq
  }
}
