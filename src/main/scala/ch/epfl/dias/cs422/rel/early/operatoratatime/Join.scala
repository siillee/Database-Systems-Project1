package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Elem, Tuple}
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {
  /**
    * Hint: you need to use methods getLeftKeys and getRightKeys
    * to implement joins
    */

  val leftKeys : IndexedSeq[Int] = getLeftKeys
  val rightKeys : IndexedSeq[Int] = getRightKeys

  var leftTuples : ListBuffer[Tuple] = ListBuffer[Tuple]()
  var rightTuples : ListBuffer[Tuple] = ListBuffer[Tuple]()
  var finalTupleList : ListBuffer[Tuple] = ListBuffer[Tuple]()

  var resultColumns : ListBuffer[Column] = ListBuffer[Column]()

  var len : Int = 0

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[Column] = {

    val leftInputColumns = left.execute()
    val rightInputColumns = right.execute()

    if (leftInputColumns.isEmpty || rightInputColumns.isEmpty){
      return (resultColumns :+ IndexedSeq[Column]()).toIndexedSeq
    }

    // get tuples from columns ; only the tuples which have TRUE in the selection vector
    for (i <- leftInputColumns(0).indices) {
      breakable {
        if (!leftInputColumns.last(i).asInstanceOf[Boolean]) {
          break
        }
        var t = ListBuffer[Elem]()
        for (c <- leftInputColumns) {
          t += c(i)
        }
        leftTuples += t.toIndexedSeq
      }
    }
    for (i <- rightInputColumns(0).indices) {
      breakable {
        if (!rightInputColumns.last(i).asInstanceOf[Boolean]) {
          break
        }
        var t = ListBuffer[Elem]()
        for (c <- rightInputColumns) {
          t += c(i)
        }
        rightTuples += t.toIndexedSeq
      }
    }

    leftTuples = leftTuples.map(t => t.dropRight(1))
    rightTuples = rightTuples.map(t => t.dropRight(1))

    val map = leftTuples.groupBy(t => getKey(t, left = true))

    for (t <- rightTuples) {
      if (map.contains(getKey(t, left = false))) {
        val tmp = map.get(getKey(t, left = false))
        for (tpl <- tmp.get) {
          finalTupleList += (tpl ++ t)
          len = (tpl ++ t).length
        }
      }
    }

    var selectVector = ListBuffer[Elem]()
    for (t <- finalTupleList){
      selectVector += true
    }

    // initializing resultColumns
    for (i <- 0 until len){
      resultColumns += IndexedSeq[Elem]()
    }

    // get columns from tuples
    for (i <- resultColumns.indices){
      for (t <- finalTupleList) {
        resultColumns(i) = resultColumns(i) :+ t(i)
      }
    }

    resultColumns += selectVector.toIndexedSeq
    resultColumns.toIndexedSeq

  }


  def getKey(t : Tuple, left : Boolean): List[Elem] = {

    var key = ListBuffer[Elem]()
    if (left) {
      for (i <- leftKeys) {
        key += t(i)
      }
    }else {
      for (i <- rightKeys) {
        key += t(i)
      }
    }
    key.toList
  }
}


