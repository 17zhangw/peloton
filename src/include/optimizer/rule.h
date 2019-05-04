//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule.h
//
// Identification: src/include/optimizer/rule.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include "optimizer/pattern.h"
#include "optimizer/optimize_context.h"
#include "optimizer/operator_expression.h"
#include "optimizer/memo.h"

namespace peloton {
namespace optimizer {

template <class Node, class OperatorType, class OperatorExpr>
class GroupExpression;

#define PHYS_PROMISE 3
#define LOG_PROMISE 1

template <class Node, class OperatorType, class OperatorExpr>
class Rule {
 public:
  virtual ~Rule(){};

  std::shared_ptr<Pattern<OperatorType>> GetMatchPattern() const { return match_pattern; }

  bool IsPhysical() const {
    return type_ > RuleType::LogicalPhysicalDelimiter &&
           type_ < RuleType::RewriteDelimiter;
  }

  bool IsLogical() const { return type_ < RuleType::LogicalPhysicalDelimiter; }

  bool IsRewrite() const { return type_ > RuleType::RewriteDelimiter; }

  /**
   * @brief Get the promise of the current rule for a expression in the current
   *  context. Currently we only differentiate physical and logical rules.
   *  Physical rules have higher promise, and will be applied before logical
   *  rules. If the rule is not applicable because the pattern does not match,
   *  the promise should be 0, which indicates that we should not apply this
   *  rule
   *
   * @param group_expr The current group expression to apply the rule
   * @param context The current context for the optimization
   *
   * @return The promise, the higher the promise, the rule should be applied
   *  sooner
   */
  virtual int Promise(GroupExpression<Node, OperatorType, OperatorExpr> *group_expr,
                      OptimizeContext<Node, OperatorType, OperatorExpr> *context) const;

  /**
   * @brief Check if the rule is applicable for the operator expression. The
   *  input operator expression should have the required "before" pattern, but
   *  other conditions may prevent us from applying the rule. For example, if
   * the
   *  logical join does not specify a join key, we could not transform it into a
   *  hash join because we need the join key to build the hash table
   *
   * @param expr The "before" operator expression
   * @param context The current context for the optimization
   *
   * @return If the rule is applicable, return true, otherwise return false
   */
  virtual bool Check(std::shared_ptr<OperatorExpr> expr,
                     OptimizeContext<Node, OperatorType, OperatorExpr> *context) const = 0;

  /**
   * @brief Convert a "before" operator tree to an "after" operator tree
   *
   * @param input The "before" operator tree
   * @param transformed The "after" operator tree
   * @param context The current optimization context
   */
  virtual void Transform(
      std::shared_ptr<OperatorExpr> input,
      std::vector<std::shared_ptr<OperatorExpr>> &transformed,
      OptimizeContext<Node, OperatorType, OperatorExpr> *context) const = 0;

  inline RuleType GetType() { return type_; }

  inline uint32_t GetRuleIdx() { return static_cast<uint32_t>(type_); }

 protected:
  std::shared_ptr<Pattern<OperatorType>> match_pattern;
  RuleType type_;
};

/**
 * @brief A struct to store a rule together with its promise
 */
template <class Node, class OperatorType, class OperatorExpr>
struct RuleWithPromise {
  RuleWithPromise(Rule<Node,OperatorType,OperatorExpr> *rule, int promise) : rule(rule), promise(promise) {}

  Rule<Node, OperatorType, OperatorExpr> *rule;
  int promise;

  bool operator<(const RuleWithPromise<Node, OperatorType, OperatorExpr> &r) const { return promise < r.promise; }
  bool operator>(const RuleWithPromise<Node, OperatorType, OperatorExpr> &r) const { return promise > r.promise; }
};

enum class RewriteRuleSetName : uint32_t {
  PREDICATE_PUSH_DOWN = 0,
  UNNEST_SUBQUERY,
  COMPARATOR_ELIMINATION,
  EQUIVALENT_TRANSFORM,
  TRANSITIVE_TRANSFORM,
  BOOLEAN_SHORT_CIRCUIT,
  NULL_LOOKUP
};

/**
 * @brief All the rule sets, including logical transformation rules, physical
 *  implementation rules and rewrite rules
 */
template <class Node, class OperatorType, class OperatorExpr>
class RuleSet {
 public:
  // RuleSet will take the ownership of the rule object
  RuleSet();

  inline void AddTransformationRule(Rule<Node,OperatorType,OperatorExpr>* rule) { transformation_rules_.emplace_back(rule); }

  inline void AddImplementationRule(Rule<Node,OperatorType,OperatorExpr>* rule) { transformation_rules_.emplace_back(rule); }

  inline void AddRewriteRule(RewriteRuleSetName set, Rule<Node,OperatorType,OperatorExpr>* rule) {
    rewrite_rules_map_[static_cast<uint32_t>(set)].emplace_back(rule);
  }

  std::vector<std::unique_ptr<Rule<Node,OperatorType,OperatorExpr>>> &GetTransformationRules() {
    return transformation_rules_;
  }

  std::vector<std::unique_ptr<Rule<Node,OperatorType,OperatorExpr>>> &GetImplementationRules() {
    return implementation_rules_;
  }

  std::vector<std::unique_ptr<Rule<Node,OperatorType,OperatorExpr>>> &GetRewriteRulesByName(
      RewriteRuleSetName set) {
    return rewrite_rules_map_[static_cast<uint32_t>(set)];
  }

  std::unordered_map<uint32_t, std::vector<std::unique_ptr<Rule<Node,OperatorType,OperatorExpr>>>> &GetRewriteRulesMap() {
    return rewrite_rules_map_;
  }

  std::vector<std::unique_ptr<Rule<Node,OperatorType,OperatorExpr>>> &GetPredicatePushDownRules() {
    return predicate_push_down_rules_;
  }

 private:
  std::vector<std::unique_ptr<Rule<Node,OperatorType,OperatorExpr>>> transformation_rules_;
  std::vector<std::unique_ptr<Rule<Node,OperatorType,OperatorExpr>>> implementation_rules_;
  std::unordered_map<uint32_t, std::vector<std::unique_ptr<Rule<Node,OperatorType,OperatorExpr>>>> rewrite_rules_map_;
  std::vector<std::unique_ptr<Rule<Node,OperatorType,OperatorExpr>>> predicate_push_down_rules_;
};

}  // namespace optimizer
}  // namespace peloton
