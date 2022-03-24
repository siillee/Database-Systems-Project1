package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.store.{RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Scan protected (
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  protected val scannable: Store = tableToStore(
    table.unwrap(classOf[ScannableTable])
  )

  private var prog = getRowType.getFieldList.asScala.map(_ => 0)

  private var scannableRowStore : RowStore = null
  private var i : Int = -1
  /**
    * @inheritdoc
    */
  override def open(): Unit = {

    scannableRowStore = scannable.asInstanceOf[RowStore]
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    i = i + 1
    if (i >= scannableRowStore.getRowCount)
      return Option.empty[Tuple]
    Option.apply(scannableRowStore.getRow(i))
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {

  }
}
