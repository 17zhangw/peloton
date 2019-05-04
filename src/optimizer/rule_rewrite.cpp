#include <memory>

#include "catalog/column_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/table_catalog.h"
#include "expression/group_marker_expression.h"
#include "optimizer/operators.h"
#include "optimizer/absexpr_expression.h"
#include "optimizer/optimizer_metadata.h"
#include "optimizer/properties.h"
#include "optimizer/rule_rewrite.h"
#include "optimizer/util.h"
#include "type/value_factory.h"

namespace peloton {
namespace optimizer {

using GroupExprTemplate = GroupExpression<AbsExpr_Container,ExpressionType,AbsExpr_Expression>;
using OptimizeContextTemplate = OptimizeContext<AbsExpr_Container,ExpressionType,AbsExpr_Expression>;

// ===========================================================
//
// ComparatorElimination related functions
//
// ===========================================================
ComparatorElimination::ComparatorElimination(RuleType rule, ExpressionType root) {
  type_ = rule;

  auto left = std::make_shared<Pattern<ExpressionType>>(ExpressionType::VALUE_CONSTANT);
  auto right = std::make_shared<Pattern<ExpressionType>>(ExpressionType::VALUE_CONSTANT);
  match_pattern = std::make_shared<Pattern<ExpressionType>>(root);
  match_pattern->AddChild(left);
  match_pattern->AddChild(right);
}

int ComparatorElimination::Promise(GroupExprTemplate *group_expr,
                                   OptimizeContextTemplate *context) const {
  (void)group_expr;
  (void)context;
  return static_cast<int>(RulePriority::MEDIUM);
}

bool ComparatorElimination::Check(std::shared_ptr<AbsExpr_Expression> plan,
                                  OptimizeContextTemplate *context) const {
  (void)context;
  (void)plan;
  return true;
}

void ComparatorElimination::Transform(std::shared_ptr<AbsExpr_Expression> input,
                                      std::vector<std::shared_ptr<AbsExpr_Expression>> &transformed,
                                      UNUSED_ATTRIBUTE OptimizeContextTemplate *context) const {
  (void)transformed;
  (void)context;

  // Extract the AbstractExpression through indirection layer.
  // Since the binding succeeded, there are guaranteed to be two children.
  PELOTON_ASSERT(input->Children().size() == 2);

  auto left = input->Children()[0]->Op().GetExpr();
  auto right = input->Children()[1]->Op().GetExpr();
  auto lv = std::dynamic_pointer_cast<expression::ConstantValueExpression>(left);
  auto rv = std::dynamic_pointer_cast<expression::ConstantValueExpression>(right);
  PELOTON_ASSERT(lv != nullptr && rv != nullptr);

  // Get the Value from ConstantValueExpression
  auto lvalue = lv->GetValue();
  auto rvalue = rv->GetValue();
  if (lvalue.CheckComparable(rvalue)) {
    CmpBool cmp = CmpBool::CmpTrue;
    switch (type_) {
    case RuleType::CONSTANT_COMPARE_EQUAL:
      cmp = lvalue.CompareEquals(rvalue);
      break;
    case RuleType::CONSTANT_COMPARE_NOTEQUAL:
      cmp = lvalue.CompareNotEquals(rvalue);
      break;
    case RuleType::CONSTANT_COMPARE_LESSTHAN:
      cmp = lvalue.CompareLessThan(rvalue);
      break;
    case RuleType::CONSTANT_COMPARE_GREATERTHAN:
      cmp = lvalue.CompareGreaterThan(rvalue);
      break;
    case RuleType::CONSTANT_COMPARE_LESSTHANOREQUALTO:
      // lv <= rv does not have a predefined function so we do the following:
      // (1) Compute truth value of lvalue > rvalue
      // (2) Flip the truth value unless CmpBool::NULL_
      cmp = lvalue.CompareGreaterThan(rvalue);
      if (cmp != CmpBool::NULL_) {
        cmp = (cmp == CmpBool::CmpFalse) ? CmpBool::CmpTrue : CmpBool::CmpFalse;
      }
      break;
    case RuleType::CONSTANT_COMPARE_GREATERTHANOREQUALTO:
      cmp = lvalue.CompareGreaterThanEquals(rvalue);
      break;
    default:
      // Other type_ should not be handled by this rule
      int type = static_cast<int>(type_);
      LOG_ERROR("lvalue compare rvalue with RuleType (%d) not implemented", type);
      PELOTON_ASSERT(0);
      break;
    }

    // Create the replacement
    type::Value val = type::ValueFactory::GetBooleanValue(cmp);
    auto expr = std::make_shared<expression::ConstantValueExpression>(val);
    auto container = AbsExpr_Container(expr);
    auto shared = std::make_shared<AbsExpr_Expression>(container);
    transformed.push_back(shared);
  }

  // If the values cannot be comparable, we leave them as is.
  // We don't throw an error or anything because it is possible this branch
  // may be collapsed due to subsequent optimizations, and it is likely
  // any error will be caught during actual query execution.
  return;
}

// ===========================================================
//
// EquivalentTransform related functions
//
// ===========================================================
EquivalentTransform::EquivalentTransform(RuleType rule, ExpressionType root) {
  type_ = rule;

  auto left = std::make_shared<Pattern<ExpressionType>>(ExpressionType::GROUP_MARKER);
  auto right = std::make_shared<Pattern<ExpressionType>>(ExpressionType::GROUP_MARKER);
  match_pattern = std::make_shared<Pattern<ExpressionType>>(root);
  match_pattern->AddChild(left);
  match_pattern->AddChild(right);
}

int EquivalentTransform::Promise(GroupExprTemplate *group_expr,
                                   OptimizeContextTemplate *context) const {
  (void)group_expr;
  (void)context;
  return static_cast<int>(RulePriority::HIGH);
}

bool EquivalentTransform::Check(std::shared_ptr<AbsExpr_Expression> plan,
                                  OptimizeContextTemplate *context) const {
  (void)context;
  (void)plan;
  return true;
}

void EquivalentTransform::Transform(std::shared_ptr<AbsExpr_Expression> input,
                                      std::vector<std::shared_ptr<AbsExpr_Expression>> &transformed,
                                      UNUSED_ATTRIBUTE OptimizeContextTemplate *context) const {
  (void)transformed;
  (void)context;

  // We expect EquivalentTransform to operate on AND / OR which should
  // have exactly 2 children for the expression to logically make sense.
  PELOTON_ASSERT(input->Children().size() == 2);

  // Create flipped ordering
  auto left = input->Children()[0];
  auto right = input->Children()[1];

  // The children do not strictly matter anymore
  auto type = match_pattern->Type();
  auto expr = std::make_shared<expression::ConjunctionExpression>(type);
  auto shared = std::make_shared<AbsExpr_Expression>(AbsExpr_Container(expr));

  // Create flipped ordering at logical level
  shared->PushChild(right);
  shared->PushChild(left);
  transformed.push_back(shared);
  return;
}


// ===========================================================
//
// Transitive-Transform related functions
//
// ===========================================================
TVEqualityWithTwoCVTransform::TVEqualityWithTwoCVTransform() {
  type_ = RuleType::TV_EQUALITY_WITH_TWO_CV;

  // (A.B = x) AND (A.B = y)
  match_pattern = std::make_shared<Pattern<ExpressionType>>(ExpressionType::CONJUNCTION_AND);

  auto l_eq = std::make_shared<Pattern<ExpressionType>>(ExpressionType::COMPARE_EQUAL);
  auto l_left = std::make_shared<Pattern<ExpressionType>>(ExpressionType::VALUE_TUPLE);
  auto l_right = std::make_shared<Pattern<ExpressionType>>(ExpressionType::VALUE_CONSTANT);
  l_eq->AddChild(l_left);
  l_eq->AddChild(l_right);

  auto r_eq = std::make_shared<Pattern<ExpressionType>>(ExpressionType::COMPARE_EQUAL);
  auto r_left = std::make_shared<Pattern<ExpressionType>>(ExpressionType::VALUE_TUPLE);
  auto r_right = std::make_shared<Pattern<ExpressionType>>(ExpressionType::VALUE_CONSTANT);
  r_eq->AddChild(r_left);
  r_eq->AddChild(r_right);

  match_pattern->AddChild(l_eq);
  match_pattern->AddChild(r_eq);
}

int TVEqualityWithTwoCVTransform::Promise(GroupExprTemplate *group_expr, OptimizeContextTemplate *context) const {
  (void)group_expr;
  (void)context;
  return static_cast<int>(RulePriority::LOW);
}

bool TVEqualityWithTwoCVTransform::Check(std::shared_ptr<AbsExpr_Expression> plan, OptimizeContextTemplate *context) const {
  (void)plan;
  (void)context;
  return true;
}

void TVEqualityWithTwoCVTransform::Transform(std::shared_ptr<AbsExpr_Expression> input,
                                               std::vector<std::shared_ptr<AbsExpr_Expression>> &transformed,
                                               OptimizeContextTemplate *context) const {
  (void)context;
  //TODO(wz2): TVEqualityWithTwoCVTransform should work beyond straight equality

  // Asserting guarantees provided by the GroupExprBindingIterator
  // Structure: (A.B = x) AND (A.B = y)
  PELOTON_ASSERT(input->Children().size() == 2);
  PELOTON_ASSERT(input->Op().GetType() == ExpressionType::CONJUNCTION_AND);

  auto l_eq = input->Children()[0];
  auto r_eq = input->Children()[1];
  PELOTON_ASSERT(l_eq->Children().size() == 2);
  PELOTON_ASSERT(r_eq->Children().size() == 2);
  PELOTON_ASSERT(l_eq->Op().GetType() == ExpressionType::COMPARE_EQUAL);
  PELOTON_ASSERT(r_eq->Op().GetType() == ExpressionType::COMPARE_EQUAL);

  auto l_tv = l_eq->Children()[0];
  auto l_cv = l_eq->Children()[1];
  PELOTON_ASSERT(l_tv->Children().size() == 0);
  PELOTON_ASSERT(l_cv->Children().size() == 0);
  PELOTON_ASSERT(l_tv->Op().GetType() == ExpressionType::VALUE_TUPLE);
  PELOTON_ASSERT(l_cv->Op().GetType() == ExpressionType::VALUE_CONSTANT);

  auto r_tv = r_eq->Children()[0];
  auto r_cv = r_eq->Children()[1];
  PELOTON_ASSERT(r_tv->Children().size() == 0);
  PELOTON_ASSERT(r_cv->Children().size() == 0);
  PELOTON_ASSERT(r_tv->Op().GetType() == ExpressionType::VALUE_TUPLE);
  PELOTON_ASSERT(r_cv->Op().GetType() == ExpressionType::VALUE_CONSTANT);

  auto l_tv_expr = l_tv->Op().GetExpr();
  auto r_tv_expr = r_tv->Op().GetExpr();
  if (l_tv_expr->ExactlyEquals(*r_tv_expr)) {
    // Given the pattern (A.B = x) AND (C.D = y), the IF statement asserts that A.B is the same as C.D
    // TODO(wz2): ExactlyEquals may be too strict, since must match bound_oid, table_name, col_name

    // ExactlyEqual on TupleValueExpression has sufficient check
    auto l_cv_expr = std::dynamic_pointer_cast<expression::ConstantValueExpression>(l_cv->Op().GetExpr());
    auto r_cv_expr = std::dynamic_pointer_cast<expression::ConstantValueExpression>(r_cv->Op().GetExpr());
    auto l_cv_val = l_cv_expr->GetValue();
    auto r_cv_val = r_cv_expr->GetValue();
    if (l_cv_val.CheckComparable(r_cv_val)) {
      // Given a pattern (A.B = x) AND (A.B = y), we perform the following:
      // - Rewrite expression to (A.B = x) if x == y
      // - Rewrite expression to FALSE if x != y (including if x / y is NULL)
      bool is_eq = false;
      if (l_cv_val.CompareEquals(r_cv_val) == CmpBool::CmpTrue) {
        // This means in the pattern (A.B = x) AND (A.B = y) that x == y
        is_eq = true;
      }

      if (is_eq) {
        auto tv_copy = std::make_shared<AbsExpr_Expression>(AbsExpr_Container(l_tv->Op()));
        auto cv_copy = std::make_shared<AbsExpr_Expression>(AbsExpr_Container(l_cv->Op()));
        auto conj_expr = std::make_shared<expression::ComparisonExpression>(ExpressionType::COMPARE_EQUAL, nullptr, nullptr);
        auto abs_expr = std::make_shared<AbsExpr_Expression>(AbsExpr_Container(conj_expr));
        abs_expr->PushChild(tv_copy);
        abs_expr->PushChild(cv_copy);

        transformed.push_back(abs_expr);
      } else {
        auto val = type::ValueFactory::GetBooleanValue(false);
        auto constant = std::make_shared<expression::ConstantValueExpression>(val);
        auto abs_expr = std::make_shared<AbsExpr_Expression>(AbsExpr_Container(constant));
        transformed.push_back(abs_expr);
      }

      return;
    }
  }

  return;
}

TransitiveClosureConstantTransform::TransitiveClosureConstantTransform() {
  type_ = RuleType::TRANSITIVE_CLOSURE_CONSTANT;

  // (A.B = x) AND (A.B = C.D)
  match_pattern = std::make_shared<Pattern<ExpressionType>>(ExpressionType::CONJUNCTION_AND);

  auto l_eq = std::make_shared<Pattern<ExpressionType>>(ExpressionType::COMPARE_EQUAL);
  auto l_left = std::make_shared<Pattern<ExpressionType>>(ExpressionType::VALUE_TUPLE);
  auto l_right = std::make_shared<Pattern<ExpressionType>>(ExpressionType::VALUE_CONSTANT);
  l_eq->AddChild(l_left);
  l_eq->AddChild(l_right);

  auto r_eq = std::make_shared<Pattern<ExpressionType>>(ExpressionType::COMPARE_EQUAL);
  auto r_left = std::make_shared<Pattern<ExpressionType>>(ExpressionType::VALUE_TUPLE);
  auto r_right = std::make_shared<Pattern<ExpressionType>>(ExpressionType::VALUE_TUPLE);
  r_eq->AddChild(r_left);
  r_eq->AddChild(r_right);

  match_pattern->AddChild(l_eq);
  match_pattern->AddChild(r_eq);
}

int TransitiveClosureConstantTransform::Promise(GroupExprTemplate *group_expr, OptimizeContextTemplate *context) const {
  (void)group_expr;
  (void)context;
  return static_cast<int>(RulePriority::LOW);
}

bool TransitiveClosureConstantTransform::Check(std::shared_ptr<AbsExpr_Expression> plan, OptimizeContextTemplate *context) const {
  (void)plan;
  (void)context;
  return true;
}

void TransitiveClosureConstantTransform::Transform(std::shared_ptr<AbsExpr_Expression> input,
                                                   std::vector<std::shared_ptr<AbsExpr_Expression>> &transformed,
                                                   OptimizeContextTemplate *context) const {
  (void)context;
  //TODO(wz2): TransitiveClosureConstant should work beyond straight equality

  // Asserting guarantees provided by the GroupExprBindingIterator
  // Structure: (A.B = x) AND (A.B = C.D)
  PELOTON_ASSERT(input->Children().size() == 2);
  PELOTON_ASSERT(input->Op().GetType() == ExpressionType::CONJUNCTION_AND);

  auto l_eq = input->Children()[0];
  auto r_eq = input->Children()[1];
  PELOTON_ASSERT(l_eq->Children().size() == 2);
  PELOTON_ASSERT(r_eq->Children().size() == 2);
  PELOTON_ASSERT(l_eq->Op().GetType() == ExpressionType::COMPARE_EQUAL);
  PELOTON_ASSERT(r_eq->Op().GetType() == ExpressionType::COMPARE_EQUAL);

  auto l_tv = l_eq->Children()[0];
  auto l_cv = l_eq->Children()[1];
  PELOTON_ASSERT(l_tv->Children().size() == 0);
  PELOTON_ASSERT(l_cv->Children().size() == 0);
  PELOTON_ASSERT(l_tv->Op().GetType() == ExpressionType::VALUE_TUPLE);
  PELOTON_ASSERT(l_cv->Op().GetType() == ExpressionType::VALUE_CONSTANT);

  auto r_tv_l = r_eq->Children()[0];
  auto r_tv_r = r_eq->Children()[1];
  PELOTON_ASSERT(r_tv_l->Children().size() == 0);
  PELOTON_ASSERT(r_tv_r->Children().size() == 0);
  PELOTON_ASSERT(r_tv_l->Op().GetType() == ExpressionType::VALUE_TUPLE);
  PELOTON_ASSERT(r_tv_r->Op().GetType() == ExpressionType::VALUE_TUPLE);

  auto l_tv_expr = l_tv->Op().GetExpr();
  auto r_tv_l_expr = r_tv_l->Op().GetExpr();
  auto r_tv_r_expr = r_tv_r->Op().GetExpr();

  // At this stage, we have the arbitrary structure: (A.B = x) AND (C.D = E.F)
  // TODO(wz2): ExactlyEquals for TupleValue may be too strict, since must match bound_oid, table_name, col_name
  if (r_tv_l_expr->ExactlyEquals(*r_tv_r_expr)) {
    // Handles case where C.D = E.F, which can rewrite to just A.B = x
    transformed.push_back(l_eq);
    return;
  }

  if (!l_tv_expr->ExactlyEquals(*r_tv_l_expr) && !l_tv_expr->ExactlyEquals(*r_tv_r_expr)) {
    // We know that A.B != C.D and A.B != E.F, so no optimization possible
    return;
  }

  auto left_tv_copy = std::make_shared<AbsExpr_Expression>(AbsExpr_Container(l_tv->Op()));
  auto left_val_copy = std::make_shared<AbsExpr_Expression>(AbsExpr_Container(l_cv->Op()));
  auto new_left_eq = std::make_shared<AbsExpr_Expression>(l_eq->Op());
  new_left_eq->PushChild(l_tv);
  new_left_eq->PushChild(l_cv);

  auto right_val_copy = left_val_copy; // save unnecessary make_shared
  auto new_right_eq = std::make_shared<AbsExpr_Expression>(r_eq->Op());

  // At this stage, we have knowledge that C.D != E.F
  if (l_tv_expr->ExactlyEquals(*r_tv_l_expr)) {
    // At this stage, we have knowledge that A.B = C.D
    new_right_eq->PushChild(right_val_copy);
    new_right_eq->PushChild(r_tv_r);
  } else {
    // At this stage, we have knowledge that A.B = E.F
    new_right_eq->PushChild(r_tv_l);
    new_right_eq->PushChild(right_val_copy);
  }

  // Create new root expression
  auto abs_expr = std::make_shared<AbsExpr_Expression>(input->Op());
  abs_expr->PushChild(new_left_eq);
  abs_expr->PushChild(new_right_eq);
  transformed.push_back(abs_expr);
}

}  // namespace optimizer
}  // namespace peloton
