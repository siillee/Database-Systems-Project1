package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, Tuple}
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.Breaks.{break, breakable}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {
  /**
    * Hint: See superclass documentation for info on collation i.e.
    * sort keys and direction
    */

  var tupleList : ListBuffer[Tuple] = ListBuffer[Tuple]()
  var i : Int = -1

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()

    var t = input.next()
    while (t.nonEmpty){
      tupleList += t.get
      t = input.next()
    }
    tupleList = tupleList.sortWith(sortRule)
    if (offset.nonEmpty) {
      tupleList = tupleList.drop(offset.get)
    }

    if (fetch.nonEmpty) {
      tupleList = tupleList.take(fetch.get)
    }

  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {

    i = i + 1
    if (i < tupleList.size) {
      Option.apply(tupleList(i))
    } else{
      Option.empty
    }

  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
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
