package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, Tuple}
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.ListBuffer

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {
  /**
    * Hint: you need to use methods getLeftKeys and getRightKeys
    * to implement joins
    */

  val leftKeys : IndexedSeq[Int] = getLeftKeys
  val rightKeys : IndexedSeq[Int] = getRightKeys

  var leftTuples : ListBuffer[Tuple] = ListBuffer[Tuple]()
  var rightTuples : ListBuffer[Tuple] = ListBuffer[Tuple]()
  var resultTuples : ListBuffer[Tuple] = ListBuffer[Tuple]()

  var i : Int = -1

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    left.open()
    right.open()

    var t = left.next()
    while (t.nonEmpty) {
      leftTuples = leftTuples += t.get
      t = left.next()
    }

    t = right.next()
    while (t.nonEmpty) {
      rightTuples = rightTuples += t.get
      t = right.next()
    }

    val map = leftTuples.groupBy(t => getKey(t, left = true))

    for (t <- rightTuples) {
      if (map.contains(getKey(t, left = false))) {
        val tmp = map.get(getKey(t, left = false))
        for (tpl <- tmp.get) {
          resultTuples = resultTuples += (tpl ++ t)
        }
      }
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {

    i = i + 1
    if (i < resultTuples.size) {
      Option.apply(resultTuples(i))
    } else {
      Option.empty
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    left.close()
    right.close()
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
