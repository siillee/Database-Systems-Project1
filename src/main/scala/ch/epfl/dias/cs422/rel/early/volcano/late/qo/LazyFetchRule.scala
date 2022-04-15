package ch.epfl.dias.cs422.rel.early.volcano.late.qo

import ch.epfl.dias.cs422.helpers.builder.skeleton.logical.{LogicalFetch, LogicalStitch}
import ch.epfl.dias.cs422.helpers.qo.rules.skeleton.LazyFetchRuleSkeleton
import ch.epfl.dias.cs422.helpers.store.late.LateStandaloneColumnStore
import ch.epfl.dias.cs422.helpers.store.late.rel.late.volcano.LateColumnScan
import org.apache.calcite.plan.{RelOptRuleCall, RelRule}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
  * RelRule (optimization rule) that finds an operator that stitches a new column
  * to the late materialized tuple and transforms stitching into a fetch operator.
  *
  * To use this rule: LazyFetchRule.Config.DEFAULT.toRule()
  *
  * @param config configuration parameters of the optimization rule
  */
class LazyFetchRule protected (config: RelRule.Config)
  extends LazyFetchRuleSkeleton(
    config
  ) {
  override def onMatchHelper(call: RelOptRuleCall): RelNode = {

    val projects : java.util.List[RexNode] = null

    val first = call.rel[RelNode](1)
    val second = call.rel[LateColumnScan](2)

    LogicalFetch.create(first, second.getRowType, second.getColumn, Option.apply(projects), classOf[LogicalFetch])
  }
}

object LazyFetchRule {

  /**
    * Instance for a [[LazyFetchRule]]
    */
  val INSTANCE = new LazyFetchRule(
    // By default, get an empty configuration
    RelRule.Config.EMPTY
      // and match:
      .withOperandSupplier((b: RelRule.OperandBuilder) =>
        // A node of class classOf[LogicalStitch]
        b.operand(classOf[LogicalStitch])
          // that has inputs:
          .inputs(
            b1 =>
              // A node that is a LateColumnScan
              b1.operand(classOf[RelNode])
                // of any inputs
                .anyInputs(),
            b2 =>
              // A node that is a LateColumnScan
              b2.operand(classOf[LateColumnScan])
              // of any inputs
              .anyInputs()
          )
      )
  )
}
