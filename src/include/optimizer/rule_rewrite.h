//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule_rewrite.h
//
// Identification: src/include/optimizer/rule_rewrite.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.h"
#include "optimizer/absexpr_expression.h"

#include <memory>

namespace peloton {
namespace optimizer {

using GroupExprTemplate = GroupExpression<AbsExpr_Container,ExpressionType,AbsExpr_Expression>;
using OptimizeContextTemplate = OptimizeContext<AbsExpr_Container,ExpressionType,AbsExpr_Expression>;

/* Rules are applied from high to low priority */
enum class RulePriority : int {
  HIGH = 3,
  MEDIUM = 2,
  LOW = 1
};

class ComparatorElimination: public Rule<AbsExpr_Container,ExpressionType,AbsExpr_Expression> {
 public:
  ComparatorElimination(RuleType rule, ExpressionType root);

  int Promise(GroupExprTemplate *group_expr, OptimizeContextTemplate *context) const override;
  bool Check(std::shared_ptr<AbsExpr_Expression> plan, OptimizeContextTemplate *context) const override;
  void Transform(std::shared_ptr<AbsExpr_Expression> input,
                 std::vector<std::shared_ptr<AbsExpr_Expression>> &transformed,
                 OptimizeContextTemplate *context) const override;
};

class EquivalentTransform: public Rule<AbsExpr_Container,ExpressionType,AbsExpr_Expression> {
 public:
  EquivalentTransform(RuleType rule, ExpressionType root);

  int Promise(GroupExprTemplate *group_expr, OptimizeContextTemplate *context) const override;
  bool Check(std::shared_ptr<AbsExpr_Expression> plan, OptimizeContextTemplate *context) const override;
  void Transform(std::shared_ptr<AbsExpr_Expression> input,
                 std::vector<std::shared_ptr<AbsExpr_Expression>> &transformed,
                 OptimizeContextTemplate *context) const override;
};

class TransitiveSingleDepthTransform: public Rule<AbsExpr_Container,ExpressionType,AbsExpr_Expression> {
 public:
  TransitiveSingleDepthTransform();

  int Promise(GroupExprTemplate *group_expr, OptimizeContextTemplate *context) const override;
  bool Check(std::shared_ptr<AbsExpr_Expression> plan, OptimizeContextTemplate *context) const override;
  void Transform(std::shared_ptr<AbsExpr_Expression> input,
                 std::vector<std::shared_ptr<AbsExpr_Expression>> &transformed,
                 OptimizeContextTemplate *context) const override;
};

class TransitiveClosureConstantTransform: public Rule<AbsExpr_Container,ExpressionType,AbsExpr_Expression> {
 public:
  TransitiveClosureConstantTransform();

  int Promise(GroupExprTemplate *group_expr, OptimizeContextTemplate *context) const override;
  bool Check(std::shared_ptr<AbsExpr_Expression> plan, OptimizeContextTemplate *context) const override;
  void Transform(std::shared_ptr<AbsExpr_Expression> input,
                 std::vector<std::shared_ptr<AbsExpr_Expression>> &transformed,
                 OptimizeContextTemplate *context) const override;
};

}  // namespace optimizer
}  // namespace peloton
