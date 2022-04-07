package ch.epfl.dias.cs422.rel.early.volcano.late

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.builder.skeleton.logical.LogicalFetch
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, LateTuple, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
import ch.epfl.dias.cs422.helpers.store.late.LateStandaloneColumnStore
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.CollectionHasAsScala

class Fetch protected (
                              input: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
                              fetchType: RelDataType,
                              column: LateStandaloneColumnStore,
                              projects: Option[java.util.List[_ <: RexNode]],
                            ) extends skeleton.Fetch[
  ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
](input, fetchType, column, projects)
  with ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator {
  lazy val evaluator: Tuple => Tuple =
    eval(projects.get.asScala.toIndexedSeq, fetchType)

  var tupleList : ListBuffer[LateTuple] = ListBuffer[LateTuple]()
  var resultTuples : ListBuffer[LateTuple] = ListBuffer[LateTuple]()
  var i : Int = -1

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()

    var t = input.next()
    while(t.nonEmpty) {
      tupleList = tupleList += t.get
      t = input.next()
    }


    var clmn = column.getColumn.map(c => IndexedSeq[Elem](c))

    if (projects.nonEmpty) {
      if (!projects.get.isEmpty){
        clmn = clmn.map(c => evaluator(c))
      }else {
        resultTuples = tupleList
        return
      }
    }

    for (t <- tupleList) {
      val elem = Option.apply(clmn(t.vid.toInt))
      if (elem.nonEmpty) {
        resultTuples = resultTuples += LateTuple(t.vid, t.value ++ elem.get)
      }else {
        resultTuples = resultTuples += t
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
    input.close()
  }
}

object Fetch {
  def create(
              input: Operator,
              fetchType: RelDataType,
              column: LateStandaloneColumnStore,
              projects: Option[java.util.List[_ <: RexNode]]
            ): LogicalFetch = {
    new Fetch(input, fetchType, column, projects)
  }
}

