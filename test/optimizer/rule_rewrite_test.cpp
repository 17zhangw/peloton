//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule_rewrite_test.cpp
//
// Identification: test/optimizer/rule_rewrite_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include "common/harness.h"

#include "optimizer/operators.h"
#include "optimizer/rewriter.h"
#include "expression/constant_value_expression.h"
#include "expression/comparison_expression.h"
#include "expression/tuple_value_expression.h"
#include "type/value_factory.h"
#include "type/value_peeker.h"
#include "optimizer/rule_rewrite.h"

namespace peloton {

namespace test {

using namespace optimizer;

class RuleRewriteTests : public PelotonTest {
 public:
  // Creates expresson: (A = X) AND (B = Y)
  expression::AbstractExpression *CreateTransitiveExpression(expression::AbstractExpression *a,
                                                             expression::AbstractExpression *x,
                                                             expression::AbstractExpression *b,
                                                             expression::AbstractExpression *y) {
    auto left_eq = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a->Copy(), x->Copy()
    );

    auto right_eq = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, b->Copy(), y->Copy()
    );

    return new expression::ConjunctionExpression(ExpressionType::CONJUNCTION_AND, left_eq, right_eq);
  }

  expression::ConstantValueExpression *GetConstantExpression(int val) {
    auto value = type::ValueFactory::GetIntegerValue(val);
    return new expression::ConstantValueExpression(value);
  }
};

TEST_F(RuleRewriteTests, ComparatorEliminationEqual) {
  // (1 == 1) => (TRUE)
  auto left = GetConstantExpression(1);
  auto right = GetConstantExpression(1);
  auto equal = new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL, left, right);

  // (1 == 2) => (FALSE)
  auto left_f = GetConstantExpression(1);
  auto right_f = GetConstantExpression(2);
  auto equal_f = new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL, left_f, right_f);

  Rewriter *rewriter = new Rewriter();
  auto rewrote = rewriter->RewriteExpression(equal);
  auto rewrote_f = rewriter->RewriteExpression(equal_f);
  delete rewriter;
  delete equal;
  delete equal_f;
  
  auto casted = dynamic_cast<expression::ConstantValueExpression*>(rewrote);
  EXPECT_TRUE(casted != nullptr);
  EXPECT_TRUE(type::ValuePeeker::PeekBoolean(casted->GetValue()) == true);
  
  auto casted_f = dynamic_cast<expression::ConstantValueExpression*>(rewrote_f);
  EXPECT_TRUE(casted_f != nullptr);
  EXPECT_TRUE(type::ValuePeeker::PeekBoolean(casted_f->GetValue()) == false);

  delete rewrote;
  delete rewrote_f;
}

TEST_F(RuleRewriteTests, ComparatorEliminationNotEqual) {
  // (1 != 1) => (FALSE)
  auto left = GetConstantExpression(1);
  auto right = GetConstantExpression(1);
  auto equal = new expression::ComparisonExpression(ExpressionType::COMPARE_NOTEQUAL, left, right);

  // (1 != 2) => (TRUE)
  auto left_f = GetConstantExpression(1);
  auto right_f = GetConstantExpression(2);
  auto equal_f = new expression::ComparisonExpression(ExpressionType::COMPARE_NOTEQUAL, left_f, right_f);
  
  Rewriter *rewriter = new Rewriter();
  auto rewrote = rewriter->RewriteExpression(equal);
  auto rewrote_f = rewriter->RewriteExpression(equal_f);
  delete rewriter;
  delete equal;
  delete equal_f;
  
  auto casted = dynamic_cast<expression::ConstantValueExpression*>(rewrote);
  EXPECT_TRUE(casted != nullptr);
  EXPECT_TRUE(type::ValuePeeker::PeekBoolean(casted->GetValue()) == false);
  
  auto casted_f = dynamic_cast<expression::ConstantValueExpression*>(rewrote_f);
  EXPECT_TRUE(casted_f != nullptr);
  EXPECT_TRUE(type::ValuePeeker::PeekBoolean(casted_f->GetValue()) == true);

  delete rewrote;
  delete rewrote_f;
}

TEST_F(RuleRewriteTests, ComparatorEliminationLessThan) {
  // (0 < 1) => (TRUE)
  auto left = GetConstantExpression(0);
  auto right = GetConstantExpression(1);
  auto equal = new expression::ComparisonExpression(ExpressionType::COMPARE_LESSTHAN, left, right);

  // (1 < 1) => (FALSE)
  auto left_ef = GetConstantExpression(1);
  auto right_ef = GetConstantExpression(1);
  auto equal_ef = new expression::ComparisonExpression(ExpressionType::COMPARE_LESSTHAN, left_ef, right_ef);

  // (2 < 1) => (FALSE)
  auto left_f = GetConstantExpression(2);
  auto right_f = GetConstantExpression(1);
  auto equal_f = new expression::ComparisonExpression(ExpressionType::COMPARE_LESSTHAN, left_f, right_f);
  
  Rewriter *rewriter = new Rewriter();
  auto rewrote = rewriter->RewriteExpression(equal);
  auto rewrote_ef = rewriter->RewriteExpression(equal_ef);
  auto rewrote_f = rewriter->RewriteExpression(equal_f);
  delete rewriter;
  delete equal;
  delete equal_ef;
  delete equal_f;
  
  auto casted = dynamic_cast<expression::ConstantValueExpression*>(rewrote);
  EXPECT_TRUE(casted != nullptr);
  EXPECT_TRUE(type::ValuePeeker::PeekBoolean(casted->GetValue()) == true);
  
  auto casted_ef = dynamic_cast<expression::ConstantValueExpression*>(rewrote_ef);
  EXPECT_TRUE(casted_ef != nullptr);
  EXPECT_TRUE(type::ValuePeeker::PeekBoolean(casted_ef->GetValue()) == false);
  
  auto casted_f = dynamic_cast<expression::ConstantValueExpression*>(rewrote_f);
  EXPECT_TRUE(casted_f != nullptr);
  EXPECT_TRUE(type::ValuePeeker::PeekBoolean(casted_f->GetValue()) == false);
  delete rewrote;
  delete rewrote_ef;
  delete rewrote_f;
}

TEST_F(RuleRewriteTests, ComparatorEliminationGreaterThan) {
  // (0 > 1) => (FALSE)
  auto left = GetConstantExpression(0);
  auto right = GetConstantExpression(1);
  auto equal = new expression::ComparisonExpression(ExpressionType::COMPARE_GREATERTHAN, left, right);

  // (1 < 1) => (FALSE)
  auto left_ef = GetConstantExpression(1);
  auto right_ef = GetConstantExpression(1);
  auto equal_ef = new expression::ComparisonExpression(ExpressionType::COMPARE_GREATERTHAN, left_ef, right_ef);

  // (2 > 1) => (TRUE)
  auto left_f = GetConstantExpression(2);
  auto right_f = GetConstantExpression(1);
  auto equal_f = new expression::ComparisonExpression(ExpressionType::COMPARE_GREATERTHAN, left_f, right_f);
  
  Rewriter *rewriter = new Rewriter();
  auto rewrote = rewriter->RewriteExpression(equal);
  auto rewrote_ef = rewriter->RewriteExpression(equal_ef);
  auto rewrote_f = rewriter->RewriteExpression(equal_f);
  delete rewriter;
  delete equal;
  delete equal_ef;
  delete equal_f;
  
  auto casted = dynamic_cast<expression::ConstantValueExpression*>(rewrote);
  EXPECT_TRUE(casted != nullptr);
  EXPECT_TRUE(type::ValuePeeker::PeekBoolean(casted->GetValue()) == false);
  
  auto casted_ef = dynamic_cast<expression::ConstantValueExpression*>(rewrote_ef);
  EXPECT_TRUE(casted_ef != nullptr);
  EXPECT_TRUE(type::ValuePeeker::PeekBoolean(casted_ef->GetValue()) == false);
  
  auto casted_f = dynamic_cast<expression::ConstantValueExpression*>(rewrote_f);
  EXPECT_TRUE(casted_f != nullptr);
  EXPECT_TRUE(type::ValuePeeker::PeekBoolean(casted_f->GetValue()) == true);

  delete rewrote;
  delete rewrote_ef;
  delete rewrote_f;
}

TEST_F(RuleRewriteTests, ComparatorEliminationLessThanOrEqualTo) {
  // (0 <= 1) => (TRUE)
  auto left = GetConstantExpression(0);
  auto right = GetConstantExpression(1);
  auto equal = new expression::ComparisonExpression(ExpressionType::COMPARE_LESSTHANOREQUALTO, left, right);

  // (1 <= 1) => (TRUE)
  auto left_ef = GetConstantExpression(1);
  auto right_ef = GetConstantExpression(1);
  auto equal_ef = new expression::ComparisonExpression(ExpressionType::COMPARE_LESSTHANOREQUALTO, left_ef, right_ef);

  // (2 <= 1) => (FALSE)
  auto left_f = GetConstantExpression(2);
  auto right_f = GetConstantExpression(1);
  auto equal_f = new expression::ComparisonExpression(ExpressionType::COMPARE_LESSTHANOREQUALTO, left_f, right_f);
  
  Rewriter *rewriter = new Rewriter();
  auto rewrote = rewriter->RewriteExpression(equal);
  auto rewrote_ef = rewriter->RewriteExpression(equal_ef);
  auto rewrote_f = rewriter->RewriteExpression(equal_f);
  delete rewriter;
  delete equal;
  delete equal_ef;
  delete equal_f;
  
  auto casted = dynamic_cast<expression::ConstantValueExpression*>(rewrote);
  EXPECT_TRUE(casted != nullptr);
  EXPECT_TRUE(type::ValuePeeker::PeekBoolean(casted->GetValue()) == true);
  
  auto casted_ef = dynamic_cast<expression::ConstantValueExpression*>(rewrote_ef);
  EXPECT_TRUE(casted_ef != nullptr);
  EXPECT_TRUE(type::ValuePeeker::PeekBoolean(casted_ef->GetValue()) == true);
  
  auto casted_f = dynamic_cast<expression::ConstantValueExpression*>(rewrote_f);
  EXPECT_TRUE(casted_f != nullptr);
  EXPECT_TRUE(type::ValuePeeker::PeekBoolean(casted_f->GetValue()) == false);

  delete rewrote;
  delete rewrote_ef;
  delete rewrote_f;
}

TEST_F(RuleRewriteTests, ComparatorEliminationGreaterThanOrEqualTo) {
  // (0 >= 1) => (FALSE)
  auto left = GetConstantExpression(0);
  auto right = GetConstantExpression(1);
  auto equal = new expression::ComparisonExpression(ExpressionType::COMPARE_GREATERTHANOREQUALTO, left, right);

  // (1 >= 1) => (TRUE)
  auto left_ef = GetConstantExpression(1);
  auto right_ef = GetConstantExpression(1);
  auto equal_ef = new expression::ComparisonExpression(ExpressionType::COMPARE_GREATERTHANOREQUALTO, left_ef, right_ef);

  // (2 >= 1) => (TRUE)
  auto left_f = GetConstantExpression(2);
  auto right_f = GetConstantExpression(1);
  auto equal_f = new expression::ComparisonExpression(ExpressionType::COMPARE_GREATERTHANOREQUALTO, left_f, right_f);
  
  Rewriter *rewriter = new Rewriter();
  auto rewrote = rewriter->RewriteExpression(equal);
  auto rewrote_ef = rewriter->RewriteExpression(equal_ef);
  auto rewrote_f = rewriter->RewriteExpression(equal_f);
  delete rewriter;
  delete equal;
  delete equal_ef;
  delete equal_f;
  
  auto casted = dynamic_cast<expression::ConstantValueExpression*>(rewrote);
  EXPECT_TRUE(casted != nullptr);
  EXPECT_TRUE(type::ValuePeeker::PeekBoolean(casted->GetValue()) == false);
  
  auto casted_ef = dynamic_cast<expression::ConstantValueExpression*>(rewrote_ef);
  EXPECT_TRUE(casted_ef != nullptr);
  EXPECT_TRUE(type::ValuePeeker::PeekBoolean(casted_ef->GetValue()) == true);
  
  auto casted_f = dynamic_cast<expression::ConstantValueExpression*>(rewrote_f);
  EXPECT_TRUE(casted_f != nullptr);
  EXPECT_TRUE(type::ValuePeeker::PeekBoolean(casted_f->GetValue()) == true);

  delete rewrote;
  delete rewrote_ef;
  delete rewrote_f;
}

TEST_F(RuleRewriteTests, ComparatorEliminationLessThanOrEqualToNull) {
  auto valNULL = type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER);

  // 0 <= NULL => NULL
  auto left = GetConstantExpression(2);
  auto right = new expression::ConstantValueExpression(valNULL);
  auto equal = new expression::ComparisonExpression(ExpressionType::COMPARE_LESSTHANOREQUALTO, left, right);

  Rewriter *rewriter = new Rewriter();
  auto rewrote = rewriter->RewriteExpression(equal);
  delete rewriter;
  delete equal;

  auto casted = dynamic_cast<expression::ConstantValueExpression*>(rewrote);
  EXPECT_TRUE(casted != nullptr);

  auto value = casted->GetValue();
  EXPECT_TRUE(value.GetTypeId() == type::TypeId::BOOLEAN);
  EXPECT_TRUE(value.IsNull());

  delete rewrote;
}

TEST_F(RuleRewriteTests, TransitiveSingleDepthFalseTransform) {
  auto cv1 = GetConstantExpression(1);
  auto cv2 = GetConstantExpression(2);
  auto tv_base = new expression::TupleValueExpression("B", "A");

  Rewriter *rewriter = new Rewriter();

  // Base: (A.B = 1) AND (A.B = 2)
  auto base = CreateTransitiveExpression(tv_base, cv1, tv_base, cv2);

  // Inverse: (1 = A.B) AND (2 = A.B)
  auto inverse = CreateTransitiveExpression(cv1, tv_base, cv2, tv_base);

  // Inner Flip Left: (1 = A.B) AND (A.B = 2)
  auto if_left = CreateTransitiveExpression(cv1, tv_base, tv_base, cv2);

  // Inner Flip Right: (A.B = 1) AND (2 = A.B)
  auto if_right = CreateTransitiveExpression(tv_base, cv1, cv2, tv_base);

  std::vector<expression::AbstractExpression*> rewrites;
  rewrites.push_back(rewriter->RewriteExpression(base));
  //rewrites.push_back(rewriter->RewriteExpression(inverse));
  //rewrites.push_back(rewriter->RewriteExpression(if_left));
  //rewrites.push_back(rewriter->RewriteExpression(if_right));
  delete rewriter;
  delete cv1;
  delete cv2;
  delete tv_base;
  delete base;
  delete inverse;
  delete if_left;
  delete if_right;

  for (auto expr : rewrites) {
    EXPECT_TRUE(expr->GetExpressionType() == ExpressionType::VALUE_CONSTANT);
    EXPECT_TRUE(expr->GetChildrenSize() == 0);

    auto expr_val = dynamic_cast<expression::ConstantValueExpression*>(expr);
    EXPECT_TRUE(expr_val != nullptr);
    EXPECT_TRUE(type::ValuePeeker::PeekBoolean(expr_val->GetValue()) == false);
  }

  while (!rewrites.empty()) {
    auto expr = rewrites.back();
    rewrites.pop_back();
    delete expr;
  }
}

}  // namespace test
}  // namespace peloton
