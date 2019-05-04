//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rewriter.cpp
//
// Identification: src/optimizer/rewriter.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "optimizer/optimizer.h"
#include "optimizer/rewriter.h"
#include "common/exception.h"

#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/operator_visitor.h"
#include "optimizer/optimize_context.h"
#include "optimizer/optimizer_task_pool.h"
#include "optimizer/rule.h"
#include "optimizer/rule_impls.h"
#include "optimizer/optimizer_metadata.h"
#include "optimizer/absexpr_expression.h"
#include "expression/abstract_expression.h"
#include "expression/constant_value_expression.h"

using std::vector;
using std::unordered_map;
using std::shared_ptr;
using std::unique_ptr;
using std::move;
using std::pair;
using std::make_shared;

namespace peloton {
namespace optimizer {

using OptimizerMetadataTemplate = OptimizerMetadata<AbsExpr_Container,ExpressionType,AbsExpr_Expression>;

using OptimizeContextTemplate = OptimizeContext<AbsExpr_Container,ExpressionType,AbsExpr_Expression>;

using OptimizerTaskStackTemplate = OptimizerTaskStack<AbsExpr_Container,ExpressionType,AbsExpr_Expression>;

using TopDownRewriteTemplate = TopDownRewrite<AbsExpr_Container,ExpressionType,AbsExpr_Expression>;

using BottomUpRewriteTemplate = BottomUpRewrite<AbsExpr_Container,ExpressionType,AbsExpr_Expression>;

using GroupExpressionTemplate = GroupExpression<AbsExpr_Container,ExpressionType,AbsExpr_Expression>;

using GroupTemplate = Group<AbsExpr_Container,ExpressionType,AbsExpr_Expression>;

Rewriter::Rewriter() : metadata_(nullptr) {
  metadata_ = OptimizerMetadataTemplate(nullptr);
}

void Rewriter::Reset() {
  metadata_ = OptimizerMetadataTemplate(nullptr);
}

void Rewriter::RewriteLoop(int root_group_id) {
  std::shared_ptr<OptimizeContextTemplate> root_context = std::make_shared<OptimizeContextTemplate>(&metadata_, nullptr);
  auto task_stack = std::unique_ptr<OptimizerTaskStackTemplate>(new OptimizerTaskStackTemplate());
  metadata_.SetTaskPool(task_stack.get());

  // Perform rewrite first
  task_stack->Push(new TopDownRewriteTemplate(root_group_id, root_context, RewriteRuleSetName::BOOLEAN_SHORT_CIRCUIT));
  task_stack->Push(new BottomUpRewriteTemplate(root_group_id, root_context, RewriteRuleSetName::TRANSITIVE_TRANSFORM, false));
  task_stack->Push(new BottomUpRewriteTemplate(root_group_id, root_context, RewriteRuleSetName::COMPARATOR_ELIMINATION, false));

  task_stack->Push(new BottomUpRewriteTemplate(root_group_id, root_context, RewriteRuleSetName::NULL_LOOKUP, false));
  auto equiv_task = new TopDownRewriteTemplate(root_group_id, root_context, RewriteRuleSetName::EQUIVALENT_TRANSFORM);
  equiv_task->SetReplaceOnTransform(false); // generate equivalent
  task_stack->Push(equiv_task);

  // Iterate through the task stack
  while (!task_stack->Empty()) {
    auto task = task_stack->Pop();
    task->execute();
  }
}

expression::AbstractExpression* Rewriter::RebuildExpression(int root) {
  auto cur_group = metadata_.memo.GetGroupByID(root);
  auto exprs = cur_group->GetLogicalExpressions();

  // If we optimized a group successfully, then it would have been
  // collapsed to only a single group. If we did not optimize a group,
  // then they are all equivalent, so pick any.
  PELOTON_ASSERT(exprs.size() >= 1);
  auto expr = exprs[0];

  std::vector<GroupID> child_groups = expr->GetChildGroupIDs();
  std::vector<expression::AbstractExpression*> child_exprs;
  for (auto group : child_groups) {
    // Build children first
    expression::AbstractExpression *child = RebuildExpression(group);
    PELOTON_ASSERT(child != nullptr);

    child_exprs.push_back(child);
  }

  AbsExpr_Container c = expr->Op();
  return c.CopyWithChildren(child_exprs);
}

std::shared_ptr<AbsExpr_Expression> Rewriter::ConvertToAbsExpr(const expression::AbstractExpression* expr) {
  // (TODO): remove the Copy invocation when in terrier since terrier uses shared_ptr
  //
  // This Copy() is not very efficient at all. but within Peloton, this is the only way
  // to present a std::shared_ptr to the AbsExpr_Container/Expression classes. In terrier,
  // this Copy() is *definitely* not needed because the AbstractExpression there already
  // utilizes std::shared_ptr properly.
  std::shared_ptr<expression::AbstractExpression> copy = std::shared_ptr<expression::AbstractExpression>(expr->Copy());

  // Create current AbsExpr_Expression
  auto container = AbsExpr_Container(copy);
  auto expression = std::make_shared<AbsExpr_Expression>(container);

  // Convert all the children
  size_t child_count = expr->GetChildrenSize();
  for (size_t i = 0; i < child_count; i++) {
    expression->PushChild(ConvertToAbsExpr(expr->GetChild(i)));
  }

  copy->ClearChildren();
  return expression;
}

std::shared_ptr<GroupExpressionTemplate> Rewriter::RecordTreeGroups(
  const expression::AbstractExpression *expr) {

  std::shared_ptr<AbsExpr_Expression> exp = ConvertToAbsExpr(expr);
  std::shared_ptr<GroupExpressionTemplate> gexpr;
  metadata_.RecordTransformedExpression(exp, gexpr);
  return gexpr;
}

expression::AbstractExpression* Rewriter::RewriteExpression(const expression::AbstractExpression *expr) {
  if (expr == nullptr)
    return nullptr;

  // This is needed in order to provide template classes the correct interface
  // and also handle immutable AbstractExpression.
  std::shared_ptr<GroupExpressionTemplate> gexpr = RecordTreeGroups(expr);
  LOG_DEBUG("Converted tree to internal data structures");

  GroupID root_id = gexpr->GetGroupID();
  RewriteLoop(root_id);
  LOG_DEBUG("Performed rewrite loop pass");

  expression::AbstractExpression *expr_tree = RebuildExpression(root_id);
  LOG_DEBUG("Rebuilt expression tree from memo table");

  Reset();
  LOG_DEBUG("Reset the rewriter");
  return expr_tree;
}

}  // namespace optimizer
}  // namespace peloton
