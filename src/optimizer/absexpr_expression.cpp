//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// absexpr_expression.cpp
//
// Identification: src/optimizer/absexpr_expression.cpp
//
//===----------------------------------------------------------------------===//

#include "optimizer/absexpr_expression.h"
#include "expression/operator_expression.h"
#include "expression/aggregate_expression.h"
#include "expression/star_expression.h"

#include <memory>
#include <vector>

namespace peloton {
namespace optimizer {

expression::AbstractExpression *AbsExpr_Container::CopyWithChildren(std::vector<expression::AbstractExpression*> children) {
  // Pre-compute left and right
  expression::AbstractExpression *left = nullptr;
  expression::AbstractExpression *right = nullptr;
  if (children.size() >= 2) {
    left = children[0];
    right = children[1];
  } else if (children.size() == 1) {
    left = children[0];
  }

  auto type = GetType();
  switch (type) {
    case ExpressionType::COMPARE_EQUAL:
    case ExpressionType::COMPARE_NOTEQUAL:
    case ExpressionType::COMPARE_LESSTHAN:
    case ExpressionType::COMPARE_GREATERTHAN:
    case ExpressionType::COMPARE_LESSTHANOREQUALTO:
    case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
    case ExpressionType::COMPARE_LIKE:
    case ExpressionType::COMPARE_NOTLIKE:
    case ExpressionType::COMPARE_IN:
    case ExpressionType::COMPARE_DISTINCT_FROM: {
      // Create new expression with 2 new children of the same type
      return new expression::ComparisonExpression(type, left, right);
    }

    case ExpressionType::CONJUNCTION_AND:
    case ExpressionType::CONJUNCTION_OR: {
      // Create new expression with the new children
      return new expression::ConjunctionExpression(type, left, right);
    }

    case ExpressionType::OPERATOR_PLUS:
    case ExpressionType::OPERATOR_MINUS:
    case ExpressionType::OPERATOR_MULTIPLY:
    case ExpressionType::OPERATOR_DIVIDE:
    case ExpressionType::OPERATOR_CONCAT:
    case ExpressionType::OPERATOR_MOD:
    case ExpressionType::OPERATOR_NOT:
    case ExpressionType::OPERATOR_IS_NULL:
    case ExpressionType::OPERATOR_IS_NOT_NULL:
    case ExpressionType::OPERATOR_EXISTS: {
      // Create new expression, preserving return_value_type_
      type::TypeId ret = node->GetValueType();
      return new expression::OperatorExpression(type, ret, left, right);
    }

    case ExpressionType::OPERATOR_UNARY_MINUS: {
      PELOTON_ASSERT(children.size() == 1);
      return new expression::OperatorUnaryMinusExpression(left);
    }

    case ExpressionType::STAR:
    case ExpressionType::VALUE_CONSTANT:
    case ExpressionType::VALUE_PARAMETER:
    case ExpressionType::VALUE_TUPLE: {
      PELOTON_ASSERT(children.size() == 0);
      return node->Copy();
    }

    case ExpressionType::AGGREGATE_COUNT:
    case ExpressionType::AGGREGATE_COUNT_STAR:
    case ExpressionType::AGGREGATE_SUM:
    case ExpressionType::AGGREGATE_MIN:
    case ExpressionType::AGGREGATE_MAX:
    case ExpressionType::AGGREGATE_AVG: {
      // We should not be changing # of children of AggregateExpression
      PELOTON_ASSERT(node->GetChildrenSize() == children.size());

      // Unfortunately, the aggregate_expression (also applies to function)
      // may already have extra state information created due to the binder.
      // Under Peloton's design, we decide to just copy() the node and then
      // install the child.
      auto expr = node->Copy();
      if (children.size() == 1) {
        // If we updated the child, install the child
        expr->SetChild(0, children[0]);
      }

      return expr;
    }

    case ExpressionType::FUNCTION: {
      // We really should not be modifying # children of Function
      PELOTON_ASSERT(children.size() == node->GetChildrenSize());
      auto copy = node->Copy();

      size_t num_child = children.size();
      for (size_t i = 0; i < num_child; i++) {
        copy->SetChild(i, children[i]);
      }
      return copy;
    }

    // Rewriting for these 2 uses special matching patterns.
    // As such, when building as an output, we just copy directly.
    case ExpressionType::ROW_SUBQUERY:
    case ExpressionType::OPERATOR_CASE_EXPR: {
      PELOTON_ASSERT(children.size() == 0);
      return node->Copy();
    }

    // These ExpressionTypes are never instantiated as a type
    case ExpressionType::PLACEHOLDER:
    case ExpressionType::COLUMN_REF:
    case ExpressionType::FUNCTION_REF:
    case ExpressionType::TABLE_REF:
    case ExpressionType::SELECT_SUBQUERY:
    case ExpressionType::VALUE_TUPLE_ADDRESS:
    case ExpressionType::VALUE_NULL:
    case ExpressionType::VALUE_VECTOR:
    case ExpressionType::VALUE_SCALAR:
    case ExpressionType::HASH_RANGE:
    case ExpressionType::OPERATOR_CAST:
    default: {
      int type = static_cast<int>(GetType());
      LOG_ERROR("Unimplemented Rebuild() for %d found", type);
      return node->Copy();
    }
  }
}

}  // namespace optimizer
}  // namespace peloton
