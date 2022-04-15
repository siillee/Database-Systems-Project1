package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {
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
  var resultHomogenousColumns : ListBuffer[HomogeneousColumn] = ListBuffer[HomogeneousColumn]()

  var len : Int = 0

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[HomogeneousColumn] = {

    val leftInputColumns = left.execute()
    val rightInputColumns = right.execute()

    val leftBoolColumn = unwrap[Boolean](leftInputColumns.last)
    val rightBoolColumn = unwrap[Boolean](rightInputColumns.last)

    if (leftInputColumns(0).isEmpty || rightInputColumns(0).isEmpty){
      return IndexedSeq[HomogeneousColumn](IndexedSeq[Elem]())
    }

    // get tuples from columns ; only the tuples which have TRUE in the selection vector
    for (i <- 0 until leftInputColumns(0).size) {
      breakable {
        if (!leftBoolColumn(i)) {
          break
        }
        var t: Tuple = IndexedSeq[Elem]()
        for (c <- leftInputColumns) {
          val tmpC = asIterable(c).toIndexedSeq
          t = t :+ tmpC(i)
        }
        leftTuples += t
      }
    }
    for (i <- 0 until rightInputColumns(0).size) {
      breakable {
        if (!rightBoolColumn(i)) {
          break
        }
        var t: Tuple = IndexedSeq[Elem]()
        for (c <- rightInputColumns) {
          val tmpC = asIterable(c).toIndexedSeq
          t = t :+ tmpC(i)
        }
        rightTuples += t
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

    for (c <- resultColumns){
      resultHomogenousColumns += toHomogeneousColumn(c)
    }
    resultHomogenousColumns.toIndexedSeq

  }


  def getKey(t : Tuple, left : Boolean): List[Elem] = {

    var key = ListBuffer[Elem]()
    if (left) {
      for (i <- leftKeys) {
        key = key += t(i)
      }
    }else {
      for (i <- rightKeys) {
        key = key += t(i)
      }
    }
    key.toList
  }
}
