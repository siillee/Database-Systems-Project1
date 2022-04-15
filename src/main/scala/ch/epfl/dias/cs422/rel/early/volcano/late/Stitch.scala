package ch.epfl.dias.cs422.rel.early.volcano.late

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.LateTuple

import scala.collection.mutable.ListBuffer

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Stitch]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator]]
  */
class Stitch protected(
                              left: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
                              right: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
                            ) extends skeleton.Stitch[
  ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
](left, right)
  with ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator {

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
      leftTuples += t.get
      t = left.next()
    }

    t = right.next()
    while (t.nonEmpty) {
      rightTuples += t.get
      t = right.next()
    }

    val map = leftTuples.groupBy(t => t.vid)

    for (tr <- rightTuples){
      for (tpl <- map(tr.vid)) {
        val newTpl = LateTuple(tr.vid, tpl.value ++ tr.value)
        resultTuples += newTpl
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
}
