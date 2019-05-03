//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// absexpr_expression.h
//
// Identification: src/include/optimizer/absexpr_expression.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include "expression/abstract_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/comparison_expression.h"
#include "expression/constant_value_expression.h"

#include <memory>
#include <vector>

namespace peloton {
namespace optimizer {

// AbsExpr_Container and AbsExpr_Expression provides and serves an analogous purpose
// to Operator and OperatorExpression. Each AbsExpr_Container wraps a single
// AbstractExpression node with the children placed inside the AbsExpr_Expression.
//
// This is done to export the correct interface from the wrapped AbstractExpression
// to the rest of the core rule/optimizer code/logic.
class AbsExpr_Container {
 public:
  // Default constructors
  AbsExpr_Container() = default;
  AbsExpr_Container(const AbsExpr_Container &other) {
    node = other.node;
  }

  AbsExpr_Container(std::shared_ptr<expression::AbstractExpression> expr) {
    node = expr;
  }

  // Return operator type
  ExpressionType GetType() const {
    if (IsDefined()) {
      return node->GetExpressionType();
    }
    return ExpressionType::INVALID;
  }

  std::shared_ptr<expression::AbstractExpression> GetExpr() const {
    return node;
  }

  // Operator contains Logical node
  bool IsLogical() const {
    return true;
  }

  // Operator contains Physical node
  bool IsPhysical() const {
    return false;
  }

  std::string GetName() const {
    if (IsDefined()) {
      return node->GetExpressionName();
    }

    return "Undefined";
  }

  hash_t Hash() const {
    if (IsDefined()) {
      return node->Hash();
    }
    return 0;
  }

  bool operator==(const AbsExpr_Container &r) {
    if (IsDefined() && r.IsDefined()) {
      //(TODO): proper equality check when migrate to terrier
      // Equality check relies on performing the following:
      // - Check each node's ExpressionType
      // - Check other parameters for a given node
      // We believe that in terrier so long as the AbstractExpression
      // are children-less, operator== provides sufficient checking.
      // The reason behind why the children-less guarantee is required,
      // is that the "real" children are actually tracked by the
      // AbsExpr_Expression class.
      return false;
    } else if (!IsDefined() && !r.IsDefined()) {
      return true;
    }
    return false;
  }

  // Operator contains physical or logical operator node
  bool IsDefined() const {
    return node != nullptr;
  }

  //(TODO): Function should use std::shared_ptr when migrate to terrier
  expression::AbstractExpression *CopyWithChildren(std::vector<expression::AbstractExpression*> children);

 private:
  std::shared_ptr<expression::AbstractExpression> node;
};


class AbsExpr_Expression {
 public:
  AbsExpr_Expression(AbsExpr_Container op): op(op) {};

  // Disallow copy and move constructor
  DISALLOW_COPY_AND_MOVE(AbsExpr_Expression);

  void PushChild(std::shared_ptr<AbsExpr_Expression> op) {
    children.push_back(op);
  }

  void PopChild() {
    children.pop_back();
  }

  const std::vector<std::shared_ptr<AbsExpr_Expression>> &Children() const {
    return children;
  }

  const AbsExpr_Container &Op() const {
    return op;
  }

 private:
  AbsExpr_Container op;
  std::vector<std::shared_ptr<AbsExpr_Expression>> children;
};

}  // namespace optimizer
}  // namespace peloton
