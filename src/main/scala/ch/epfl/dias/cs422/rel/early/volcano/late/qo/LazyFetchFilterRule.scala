package ch.epfl.dias.cs422.rel.early.volcano.late.qo

import ch.epfl.dias.cs422.helpers.builder.skeleton.logical.{LogicalFetch, LogicalStitch}
import ch.epfl.dias.cs422.helpers.qo.rules.skeleton.LazyFetchFilterRuleSkeleton
import ch.epfl.dias.cs422.helpers.store.late.rel.late.volcano.LateColumnScan
import org.apache.calcite.plan.{RelOptRuleCall, RelRule}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.{LogicalFilter, LogicalProject}
import org.apache.calcite.rex.{RexNode, RexUtil}

/**
  * RelRule (optimization rule) that finds a reconstruct operator that
  * stitches a filtered column (scan then filter) with the late materialized
  * tuple and transforms stitching into a fetch operator followed by a filter.
  *
  * To use this rule: LazyFetchProjectRule.Config.DEFAULT.toRule()
  *
  * @param config configuration parameters of the optimization rule
  */
class LazyFetchFilterRule protected (config: RelRule.Config)
  extends LazyFetchFilterRuleSkeleton(
    config
  ) {

  override def onMatchHelper(call: RelOptRuleCall): RelNode = {

    var projects : java.util.List[RexNode] = null

    val first = call.rel[RelNode](1)
    val second = call.rel[LogicalFilter](2)
    val third = call.rel[LateColumnScan](3)

    first match {
      case project: LogicalProject =>
        projects = project.getProjects
      case _ =>
    }
    val fetch = LogicalFetch.create(first, third.getRowType, third.getColumn, Option.apply(projects), classOf[LogicalFetch])

    LogicalFilter.create(fetch, RexUtil.shift(second.getCondition, first.getRowType.getFieldCount))
  }
}

object LazyFetchFilterRule {

  /**
    * Instance for a [[LazyFetchFilterRule]]
    */
  val INSTANCE = new LazyFetchFilterRule(
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
              b2.operand(classOf[LogicalFilter])
                // of any inputs
                .oneInput(
                  b3 =>
                    b3.operand(classOf[LateColumnScan])
                      .anyInputs()
                )
          )
      )
  )
}
