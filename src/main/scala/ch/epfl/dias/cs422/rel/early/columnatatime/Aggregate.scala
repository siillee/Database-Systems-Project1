package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.Breaks.{break, breakable}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {
  /**
    * Hint 1: See superclass documentation for semantics of groupSet and aggCalls
    * Hint 2: You do not need to implement each aggregate function yourself.
    * You can use reduce method of AggregateCall
    * Hint 3: In case you prefer a functional solution, you can use
    * groupMapReduce
    */

  var tupleList : ListBuffer[Tuple] = ListBuffer[Tuple]()
  var finalTupleList : ListBuffer[Tuple] = ListBuffer[Tuple]()
  val fieldIndices : List[Integer] = groupSet.asScala.toList
  var resultColumns : ListBuffer[Column] = ListBuffer[Column]()

  var resultHomogenousColumns : IndexedSeq[HomogeneousColumn] = IndexedSeq[HomogeneousColumn]()
  // length of tuple after aggregate functions are applied (number of columns)
  var len : Int = 0

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[HomogeneousColumn] = {

    val columns = input.execute()

    // takes care of the emptyAgg test because in that case the program cannot unwrap the last(boolean) column
    // but the output is still expected to be one boolean column (the selection vector)
    if (columns.isEmpty || columns(0).isEmpty){
      return resultHomogenousColumns :+ toHomogeneousColumn(IndexedSeq[Elem](true))
    }

    val boolColumn = unwrap[Boolean](columns.last)
    // get tuples from columns ; only the tuples which have TRUE in the selection vector

    for (i <- 0 until columns(0).size) {
      breakable {
        if (!boolColumn(i)) {
          break
        }
        var t: Tuple = IndexedSeq[Elem]()
        for (c <- columns) {
          val tmpC = asIterable(c).toIndexedSeq
          t = t :+ tmpC(i)
        }
        tupleList = tupleList += t
      }
    }

    tupleList = tupleList.map(t => t.dropRight(1))

    // cases where groupSet is empty
    if (groupSet.isEmpty){
      var tmpTuple = ListBuffer[Elem]()
      if (tupleList.isEmpty){
        for (aggCall <- aggCalls) {
          tmpTuple = tmpTuple :+ aggCall.emptyValue
        }
      }else {
        for (aggCall <- aggCalls) {
          tmpTuple = tmpTuple :+ tupleList.map(t => aggCall.getArgument(t)).reduce((x, y) => aggCall.reduce(x, y))
        }
      }
      len = tmpTuple.length
      finalTupleList += tmpTuple.toIndexedSeq
    }else {
      // grouping of tuples by the groupSet key
      val grouped = tupleList.groupBy(t => {
        var key = ListBuffer[Elem]()
        for (i <- fieldIndices){
          key = key += t(i)
        }
        key.toList
      })


      for (kv <- grouped){
        var tmpTuple  = ListBuffer[Elem]()
        tmpTuple = tmpTuple ++ kv._1
        for (aggCall <- aggCalls) {
          tmpTuple = tmpTuple :+ kv._2.map(t => aggCall.getArgument(t)).reduce((x, y) => aggCall.reduce(x ,y))
        }
        len = tmpTuple.length
        finalTupleList += tmpTuple.toIndexedSeq
      }
    }

    var selectVector = IndexedSeq[Elem]()
    for (t <- finalTupleList){
      selectVector = selectVector :+ true
    }


    for (i <- 0 until len){
      resultColumns = resultColumns += IndexedSeq[Elem]()
    }


    for (i <- resultColumns.indices){
      for (t <- finalTupleList) {
        resultColumns(i) = resultColumns(i) :+ t(i)
      }
    }

    resultColumns = resultColumns :+ selectVector

    for (c <- resultColumns){
      resultHomogenousColumns = resultHomogenousColumns :+ toHomogeneousColumn(c)
    }
    resultHomogenousColumns
  }
}
