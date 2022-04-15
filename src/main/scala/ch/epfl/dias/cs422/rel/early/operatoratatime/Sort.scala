package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Elem, Tuple}
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.Breaks.{break, breakable}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {
  /**
    * Hint: See superclass documentation for info on collation i.e.
    * sort keys and direction
    */

  var tupleList: ListBuffer[Tuple] = ListBuffer[Tuple]()
  var resultColumns: ListBuffer[Column] = ListBuffer[Column]()

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[Column] = {


    val columns = input.execute()

    // get tuples from columns
    for (i <- columns(0).indices) {
      breakable {
        if (!columns.last(i).asInstanceOf[Boolean]) {
          break
        }
        var t = ListBuffer[Elem]()
        for (c <- columns) {
          t += c(i)
        }
        tupleList += t.toIndexedSeq
      }
    }

    // sort the tuples
    tupleList = tupleList.sortWith(sortRule)
    if (offset.nonEmpty) {
      tupleList = tupleList.drop(offset.get)
    }
    if (fetch.nonEmpty) {
        tupleList = tupleList.take(fetch.get)
    }

    // get columns from tuples
    for (i <- columns.indices){
      resultColumns += IndexedSeq[Elem]()
    }

    for (t <- tupleList) {
      for (i <- resultColumns.indices){
        resultColumns(i) = resultColumns(i) :+ t(i)
      }
    }

    resultColumns.toIndexedSeq

  }

  def sortRule(t1 : Tuple, t2 : Tuple) : Boolean = {

    for (i <- collation.getFieldCollations.asScala.toList) {
      val index = i.getFieldIndex
      breakable {
        if (t1(index).equals(t2(index))) {
          break
        }else {
          if (i.getDirection.isDescending){
            return RelFieldCollation.compare(t1(index).asInstanceOf[Comparable[Elem]], t2(index).asInstanceOf[Comparable[Elem]],
              RelFieldCollation.NullDirection.LAST.nullComparison) > 0
          }else {
            return RelFieldCollation.compare(t1(index).asInstanceOf[Comparable[Elem]], t2(index).asInstanceOf[Comparable[Elem]],
              RelFieldCollation.NullDirection.LAST.nullComparison) < 0
          }
        }
      }
    }

    false
  }
}
