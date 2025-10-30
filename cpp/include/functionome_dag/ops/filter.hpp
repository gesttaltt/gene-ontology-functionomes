#pragma once

#include "../dag/node.hpp"
#include "../columnar/table.hpp"
#include <functional>

namespace functionome {
namespace ops {

using columnar::Table;

/**
 * Filter operation - selects rows based on predicate
 * Supports predicate fusion (AND composition)
 */
class FilterNode : public dag::Node {
public:
    using Predicate = std::function<bool(std::shared_ptr<Table>, size_t row_idx)>;

    explicit FilterNode(Predicate pred, std::string name = "filter");

    std::shared_ptr<Table> execute() override;

    size_t estimated_memory_bytes() const override;
    size_t estimated_compute_ops() const override;

    // Fusion support
    bool can_fuse_with(const dag::Node& other) const override;
    std::shared_ptr<dag::Node> fuse_with(std::shared_ptr<dag::Node> other) override;

private:
    Predicate predicate_;
    std::vector<Predicate> fused_predicates_;  // AND chain
};

/**
 * Helper functions for common filter predicates
 */
namespace filter_ops {

// Column comparison predicates
FilterNode::Predicate greater_than(const std::string& col, double threshold);
FilterNode::Predicate less_than(const std::string& col, double threshold);
FilterNode::Predicate equals(const std::string& col, double value);
FilterNode::Predicate not_null(const std::string& col);

// String predicates
FilterNode::Predicate string_contains(const std::string& col, const std::string& substr);
FilterNode::Predicate string_starts_with(const std::string& col, const std::string& prefix);

// Logical combinators
FilterNode::Predicate and_predicates(FilterNode::Predicate p1, FilterNode::Predicate p2);
FilterNode::Predicate or_predicates(FilterNode::Predicate p1, FilterNode::Predicate p2);
FilterNode::Predicate not_predicate(FilterNode::Predicate p);

} // namespace filter_ops

} // namespace ops
} // namespace functionome
