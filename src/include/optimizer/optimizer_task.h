//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule.h
//
// Identification: src/include/optimizer/optimizer_task.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>
#include <set>

#include "expression/abstract_expression.h"
#include "common/internal_types.h"

namespace peloton {
namespace expression {
class AbstractExpression;
}
namespace optimizer {

template <class Node, class OperatorType, class OperatorExpr>
class OptimizeContext;

template <class Node, class OperatorType, class OperatorExpr>
class Memo;

template <class Node, class OperatorType, class OperatorExpr>
class Rule;

template <class Node, class OperatorType, class OperatorExpr>
struct RuleWithPromise;

template <class Node, class OperatorType, class OperatorExpr>
class RuleSet;

template <class Node, class OperatorType, class OperatorExpr>
class Group;

template <class Node, class OperatorType, class OperatorExpr>
class GroupExpression;

template <class Node, class OperatorType, class OperatorExpr>
class OptimizerMetadata;

enum class OpType;
class Operator;
class OperatorExpression;
class PropertySet;
enum class RewriteRuleSetName : uint32_t;
using GroupID = int32_t;

enum class OptimizerTaskType {
  OPTIMIZE_GROUP,
  OPTIMIZE_EXPR,
  EXPLORE_GROUP,
  EXPLORE_EXPR,
  APPLY_RULE,
  OPTIMIZE_INPUTS,
  DERIVE_STATS,
  REWRITE_EXPR,
  APPLY_REWIRE_RULE,
  TOP_DOWN_REWRITE,
  BOTTOM_UP_REWRITE
};

/**
 * @brief The base class for tasks in the optimizer
 */
template <class Node, class OperatorType, class OperatorExpr>
class OptimizerTask {
 public:
  OptimizerTask(std::shared_ptr<OptimizeContext<Node,OperatorType,OperatorExpr>> context,
                OptimizerTaskType type)
      : type_(type), context_(context) {}

  /**
   * @brief Construct valid rules with their promises for a group expression,
   * promises are used to determine the order of applying the rules. We
   * currently use the promise to enforce that physical rules to be applied
   * before logical rules
   *
   * @param group_expr The group expressions to apply rules
   * @param context The current optimize context
   * @param rules The candidate rule set
   * @param valid_rules The valid rules to apply in the current rule set will be
   *  append to valid_rules, with their promises
   */
  static void ConstructValidRules(GroupExpression<Node,OperatorType,OperatorExpr> *group_expr,
                                  OptimizeContext<Node,OperatorType,OperatorExpr> *context,
                                  std::vector<std::unique_ptr<Rule<Node,OperatorType,OperatorExpr>>> &rules,
                                  std::vector<RuleWithPromise<Node,OperatorType,OperatorExpr>> &valid_rules);

  virtual void execute() = 0;

  void PushTask(OptimizerTask<Node,OperatorType,OperatorExpr> *task);

  inline Memo<Node,OperatorType,OperatorExpr> &GetMemo() const;

  inline RuleSet<Node,OperatorType,OperatorExpr> &GetRuleSet() const;

  virtual ~OptimizerTask(){};

 protected:
  OptimizerTaskType type_;
  std::shared_ptr<OptimizeContext<Node,OperatorType,OperatorExpr>> context_;
};

/**
 * @brief Optimize a group given context. Which will 1. Generatate all logically
 *  equivalent operator trees if not already explored 2. Cost all physical
 *  operator trees given the current context
 */
class OptimizeGroup : public OptimizerTask<Operator,OpType,OperatorExpression> {
 public:
  OptimizeGroup(Group<Operator,OpType,OperatorExpression> *group,
                std::shared_ptr<OptimizeContext<Operator,OpType,OperatorExpression>> context)
      : OptimizerTask(context, OptimizerTaskType::OPTIMIZE_GROUP),
        group_(group) {}
  virtual void execute() override;

 private:
  Group<Operator,OpType,OperatorExpression> *group_;
};

/**
 * @brief Optimize an expression by constructing all logical and physical
 *  transformation and applying those rules. Note that we sort all rules by
 * their
 *  promises so that a physical transformation rule is applied before a logical
 *  transformation rule
 */
class OptimizeExpression : public OptimizerTask<Operator,OpType,OperatorExpression> {
 public:
  OptimizeExpression(GroupExpression<Operator,OpType,OperatorExpression> *group_expr,
                     std::shared_ptr<OptimizeContext<Operator,OpType,OperatorExpression>> context)
      : OptimizerTask(context, OptimizerTaskType::OPTIMIZE_EXPR),
        group_expr_(group_expr) {}
  virtual void execute() override;

 private:
  GroupExpression<Operator,OpType,OperatorExpression> *group_expr_;
};

/**
 * @brief Generate all logical transformation rules by applying logical
 * transformation rules to logical operators in the group until saturated
 */
class ExploreGroup : public OptimizerTask<Operator,OpType,OperatorExpression> {
 public:
  ExploreGroup(Group<Operator,OpType,OperatorExpression> *group,
               std::shared_ptr<OptimizeContext<Operator,OpType,OperatorExpression>> context)
      : OptimizerTask(context, OptimizerTaskType::EXPLORE_GROUP),
        group_(group) {}
  virtual void execute() override;

 private:
  Group<Operator,OpType,OperatorExpression> *group_;
};

/**
 * @brief Apply logical transformation rules to a group expression, if a new
 * pattern
 * in the same group is found, also apply logical transformation rule for it.
 */
class ExploreExpression : public OptimizerTask<Operator,OpType,OperatorExpression> {
 public:
  ExploreExpression(GroupExpression<Operator,OpType,OperatorExpression> *group_expr,
                    std::shared_ptr<OptimizeContext<Operator,OpType,OperatorExpression>> context)
      : OptimizerTask(context, OptimizerTaskType::EXPLORE_EXPR),
        group_expr_(group_expr) {}
  virtual void execute() override;

 private:
  GroupExpression<Operator,OpType,OperatorExpression> *group_expr_;
};

/**
 * @brief Apply rule, if the it's a logical transformation rule, we need to
 *  explore (apply logical rules) or optimize (apply logical & physical rules)
 *  to the new group expression based on the explore flag. If the rule is a
 *  physical implementation rule, we directly cost the physical expression
 */
class ApplyRule : public OptimizerTask<Operator,OpType,OperatorExpression> {
 public:
  ApplyRule(GroupExpression<Operator,OpType,OperatorExpression> *group_expr,
            Rule<Operator,OpType,OperatorExpression> *rule,
            std::shared_ptr<OptimizeContext<Operator,OpType,OperatorExpression>> context, bool explore = false)
      : OptimizerTask(context, OptimizerTaskType::APPLY_RULE),
        group_expr_(group_expr),
        rule_(rule),
        explore_only(explore) {}
  virtual void execute() override;

 private:
  GroupExpression<Operator,OpType,OperatorExpression> *group_expr_;
  Rule<Operator,OpType,OperatorExpression> *rule_;
  bool explore_only;
};

/**
 * @brief Cost a physical expression. Cost the root operator first then get the
 *  lowest cost of each of the child groups. Finally enforce properties to meet
 *  the requirement in the context. We apply pruning by terminating if the
 *  current expression's cost is larger than the upper bound of the current
 *  group
 */
class OptimizeInputs : public OptimizerTask<Operator,OpType,OperatorExpression> {
 public:
  OptimizeInputs(GroupExpression<Operator,OpType,OperatorExpression> *group_expr,
                 std::shared_ptr<OptimizeContext<Operator,OpType,OperatorExpression>> context)
      : OptimizerTask(context, OptimizerTaskType::OPTIMIZE_INPUTS),
        group_expr_(group_expr) {}

  OptimizeInputs(OptimizeInputs *task)
      : OptimizerTask(task->context_, OptimizerTaskType::OPTIMIZE_INPUTS),
        output_input_properties_(std::move(task->output_input_properties_)),
        group_expr_(task->group_expr_),
        cur_total_cost_(task->cur_total_cost_),
        cur_child_idx_(task->cur_child_idx_),
        cur_prop_pair_idx_(task->cur_prop_pair_idx_) {}

  virtual void execute() override;

 private:
  std::vector<std::pair<std::shared_ptr<PropertySet>,
                        std::vector<std::shared_ptr<PropertySet>>>>
      output_input_properties_;
  GroupExpression<Operator,OpType,OperatorExpression> *group_expr_;
  double cur_total_cost_;
  int cur_child_idx_ = -1;
  int prev_child_idx_ = -1;
  int cur_prop_pair_idx_ = 0;
};

/**
 * @brief Derive the stats needed to cost a group expression, will check if the
 * child group have the stats, if not, recursively derive the stats. This would
 * lazily collect the stats for the column needed
 */
class DeriveStats : public OptimizerTask<Operator,OpType, OperatorExpression> {
 public:
  DeriveStats(GroupExpression<Operator,OpType,OperatorExpression> *gexpr,
              ExprSet required_cols,
              std::shared_ptr<OptimizeContext<Operator,OpType,OperatorExpression>> context)
      : OptimizerTask(context, OptimizerTaskType::DERIVE_STATS),
        gexpr_(gexpr),
        required_cols_(required_cols) {}

  DeriveStats(DeriveStats *task)
      : OptimizerTask(task->context_, OptimizerTaskType::DERIVE_STATS),
        gexpr_(task->gexpr_),
        required_cols_(task->required_cols_) {}

  virtual void execute() override;

 private:
  GroupExpression<Operator,OpType,OperatorExpression> *gexpr_;
  ExprSet required_cols_;
};


/**
 * @brief Higher abstraction above TopDownRewrite and BottomUpRewrite that
 * implements functionality similar and relied upon by both TopDownRewrite
 * and BottomUpRewrite.
 */
template <class Node, class OperatorType, class OperatorExpr>
class RewriteTask : public OptimizerTask<Node,OperatorType,OperatorExpr> {
 public:
  RewriteTask(OptimizerTaskType type,
              GroupID group_id,
              std::shared_ptr<OptimizeContext<Node,OperatorType,OperatorExpr>> context,
              RewriteRuleSetName rule_set_name)
    : OptimizerTask<Node,OperatorType,OperatorExpr>(context, type),
      group_id_(group_id),
      rule_set_name_(rule_set_name) {}

  virtual void execute() override {
    LOG_ERROR("RewriteTask::execute invoked directly and not on derived");
    PELOTON_ASSERT(0);
  };

 protected:
  std::set<GroupID> GetUniqueChildGroupIDs();
  bool OptimizeCurrentGroup(bool replace_on_match);

  GroupID group_id_;
  RewriteRuleSetName rule_set_name_;
};

/**
 * @brief Apply top-down rewrite pass, take in a rule set which must fulfill
 * that the lower level rewrite in the operator tree will not enable upper
 * level rewrite. An example is predicate push-down. We only push the predicates
 * from the upper level to the lower level.
 */
template <class Node, class OperatorType, class OperatorExpr>
class TopDownRewrite : public RewriteTask<Node,OperatorType,OperatorExpr> {
 public:
  TopDownRewrite(GroupID group_id,
                 std::shared_ptr<OptimizeContext<Node,OperatorType,OperatorExpr>> context,
                 RewriteRuleSetName rule_set_name)
      : RewriteTask<Node,OperatorType,OperatorExpr>(OptimizerTaskType::TOP_DOWN_REWRITE, group_id, context, rule_set_name),
        replace_on_transform_(true) {}

  void SetReplaceOnTransform(bool replace) { replace_on_transform_ = replace; }
  virtual void execute() override;

 private:
  bool replace_on_transform_;
};

/**
 * @brief Apply bottom-up rewrite pass, take in a rule set which must fulfill
 * that the upper level rewrite in the operator tree will not enable lower
 * level rewrite.
 */
template <class Node, class OperatorType, class OperatorExpr>
class BottomUpRewrite : public RewriteTask<Node,OperatorType,OperatorExpr> {
 public:
  BottomUpRewrite(GroupID group_id,
                  std::shared_ptr<OptimizeContext<Node,OperatorType,OperatorExpr>> context,
                  RewriteRuleSetName rule_set_name, bool has_optimized_child)
      : RewriteTask<Node,OperatorType,OperatorExpr>(OptimizerTaskType::BOTTOM_UP_REWRITE, group_id, context, rule_set_name),
        has_optimized_child_(has_optimized_child) {}

  virtual void execute() override;

 private:
  bool has_optimized_child_;
};
}  // namespace optimizer
}  // namespace peloton
