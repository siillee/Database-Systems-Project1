package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.Breaks.{break, breakable}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {
  /**
    * Hint: See superclass documentation for info on collation i.e.
    * sort keys and direction
    */

  var resultHomogenousColumns : ListBuffer[HomogeneousColumn] = ListBuffer[HomogeneousColumn]()
  var tupleList : ListBuffer[Tuple] = ListBuffer[Tuple]()
  var resultColumns: ListBuffer[Column] = ListBuffer[Column]()

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[HomogeneousColumn] = {


    val columns = input.execute()
    val boolColumn = unwrap[Boolean](columns.last)

    // get tuples from columns
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
        tupleList += t
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

    // initializing resultColumns
    for (i <- columns.indices){
      resultColumns += IndexedSeq[Elem]()
    }

    // get columns from tuples
    for (t <- tupleList) {
      for (i <- resultColumns.indices){
        resultColumns(i) = resultColumns(i) :+ t(i)
      }
    }

    for (c <- resultColumns){
      resultHomogenousColumns += toHomogeneousColumn(c)
    }
    resultHomogenousColumns.toIndexedSeq
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
