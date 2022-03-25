package ch.epfl.dias.cs422.rel.early.volcano.late

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, LateTuple}
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.ListBuffer

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator]]
  */
class LateJoin(
               left: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
               right: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
               condition: RexNode
             ) extends skeleton.Join[
  ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
](left, right, condition)
  with ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator {
  /**
    * Hint: you need to use methods getLeftKeys and getRightKeys
    * to implement joins
    */

  val leftKeys : IndexedSeq[Int] = getLeftKeys
  val rightKeys : IndexedSeq[Int] = getRightKeys

  var leftTuples : ListBuffer[LateTuple] = ListBuffer[LateTuple]()
  var rightTuples : ListBuffer[LateTuple] = ListBuffer[LateTuple]()
  var resultTuples : ListBuffer[LateTuple] = ListBuffer[LateTuple]()

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
          resultTuples = resultTuples += LateTuple(t.vid, tpl.value ++ t.value)
        }
      }
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[LateTuple] = {

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

  def getKey(t : LateTuple, left : Boolean): List[Elem] = {

    var key = ListBuffer[Elem]()
    if (left) {
      for (i <- leftKeys) {
        key = key += t.value(i)
      }
    }else {
      for (i <- rightKeys) {
        key = key += t.value(i)
      }
    }
    key.toList
  }
}
