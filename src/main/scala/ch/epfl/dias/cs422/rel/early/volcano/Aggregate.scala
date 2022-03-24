package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, Tuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {
  /**
    * Hint 1: See superclass documentation for semantics of groupSet and aggCalls
    * Hint 2: You do not need to implement each aggregate function yourself.
    * You can use reduce method of AggregateCall
    * Hint 3: In case you prefer a functional solution, you can use
    * groupMapReduce
    */

  var tupleList : ListBuffer[Tuple] = ListBuffer[Tuple]()
  var resultTuples : ListBuffer[Tuple] = ListBuffer[Tuple]()
  val fieldIndices : List[Integer] = groupSet.asScala.toList
  var i : Int = -1

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()

    // collection of the tuples from input
    var t = input.next()
    while (t.nonEmpty){
      tupleList = tupleList += t.get
      t = input.next()
    }

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
      resultTuples += tmpTuple.toIndexedSeq
      return
    }

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
      resultTuples += tmpTuple.toIndexedSeq
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
    input.close()
  }
}
