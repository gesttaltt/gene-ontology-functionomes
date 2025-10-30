#include "functionome_dag/ops/filter.hpp"
#include <stdexcept>

namespace functionome {
namespace ops {

FilterNode::FilterNode(Predicate pred, std::string name)
    : Node(dag::OperationType::FILTER, std::move(name))
    , predicate_(std::move(pred)) {}

std::shared_ptr<Table> FilterNode::execute() {
    if (cache_valid_ && cached_result_) {
        return cached_result_;
    }

    if (inputs_.empty()) {
        throw std::runtime_error("FilterNode requires at least one input");
    }

    auto input_table = inputs_[0]->execute();
    size_t num_rows = input_table->num_rows();

    // Collect row indices that pass all predicates
    std::vector<size_t> passing_rows;
    passing_rows.reserve(num_rows);

    for (size_t row = 0; row < num_rows; ++row) {
        bool passes = predicate_(input_table, row);

        // Apply fused predicates (AND logic)
        for (const auto& fused_pred : fused_predicates_) {
            if (!fused_pred(input_table, row)) {
                passes = false;
                break;
            }
        }

        if (passes) {
            passing_rows.push_back(row);
        }
    }

    // Build filtered table
    auto result = std::make_shared<Table>();

    for (const auto& col_name : input_table->column_names()) {
        auto input_col = input_table->get_column(col_name);

        // Create new column with only passing rows (simplified)
        // Real implementation would batch this for efficiency
        std::shared_ptr<columnar::Column> filtered_col;

        switch (input_col->type()) {
            case columnar::DataType::INT64: {
                auto output = std::make_shared<columnar::Int64Column>();
                auto typed_input = std::static_pointer_cast<columnar::Int64Column>(input_col);
                for (size_t row_idx : passing_rows) {
                    output->append((*typed_input)[row_idx], typed_input->is_null(row_idx));
                }
                filtered_col = output;
                break;
            }
            // Add other types as needed
            default:
                throw std::runtime_error("Unsupported column type in filter");
        }

        result->add_column(col_name, filtered_col);
    }

    cached_result_ = result;
    cache_valid_ = true;
    return cached_result_;
}

size_t FilterNode::estimated_memory_bytes() const {
    if (inputs_.empty()) return 0;
    // Filter reduces output size (estimate 50% selectivity)
    return inputs_[0]->estimated_memory_bytes() / 2;
}

size_t FilterNode::estimated_compute_ops() const {
    if (inputs_.empty()) return 0;
    // Each row needs predicate evaluation
    size_t base_ops = inputs_[0]->estimated_compute_ops();
    return base_ops * (1 + fused_predicates_.size());
}

bool FilterNode::can_fuse_with(const dag::Node& other) const {
    return other.type() == dag::OperationType::FILTER;
}

std::shared_ptr<dag::Node> FilterNode::fuse_with(std::shared_ptr<dag::Node> other) {
    auto other_filter = std::dynamic_pointer_cast<FilterNode>(other);
    if (!other_filter) {
        return nullptr;
    }

    // Create fused filter node with AND composition
    auto fused = std::make_shared<FilterNode>(
        predicate_,
        name_ + "_fused_" + other->name()
    );

    // Copy our fused predicates
    fused->fused_predicates_ = fused_predicates_;

    // Add other's predicate
    fused->fused_predicates_.push_back(other_filter->predicate_);

    // Add other's fused predicates
    fused->fused_predicates_.insert(
        fused->fused_predicates_.end(),
        other_filter->fused_predicates_.begin(),
        other_filter->fused_predicates_.end()
    );

    // Copy inputs from the earlier node
    for (const auto& input : other->inputs()) {
        fused->add_input(input);
    }

    return fused;
}

// Helper functions

namespace filter_ops {

FilterNode::Predicate greater_than(const std::string& col, double threshold) {
    return [=](std::shared_ptr<Table> table, size_t row_idx) -> bool {
        auto column = table->get_column(col);
        if (column->is_null(row_idx)) return false;

        // Simplified - needs type checking
        if (auto int_col = std::dynamic_pointer_cast<columnar::Int64Column>(column)) {
            return (*int_col)[row_idx] > threshold;
        }
        return false;
    };
}

FilterNode::Predicate less_than(const std::string& col, double threshold) {
    return [=](std::shared_ptr<Table> table, size_t row_idx) -> bool {
        auto column = table->get_column(col);
        if (column->is_null(row_idx)) return false;

        if (auto int_col = std::dynamic_pointer_cast<columnar::Int64Column>(column)) {
            return (*int_col)[row_idx] < threshold;
        }
        return false;
    };
}

FilterNode::Predicate equals(const std::string& col, double value) {
    return [=](std::shared_ptr<Table> table, size_t row_idx) -> bool {
        auto column = table->get_column(col);
        if (column->is_null(row_idx)) return false;

        if (auto int_col = std::dynamic_pointer_cast<columnar::Int64Column>(column)) {
            return (*int_col)[row_idx] == value;
        }
        return false;
    };
}

FilterNode::Predicate not_null(const std::string& col) {
    return [=](std::shared_ptr<Table> table, size_t row_idx) -> bool {
        auto column = table->get_column(col);
        return !column->is_null(row_idx);
    };
}

FilterNode::Predicate string_contains(const std::string& col, const std::string& substr) {
    return [=](std::shared_ptr<Table> table, size_t row_idx) -> bool {
        auto column = table->get_column(col);
        if (column->is_null(row_idx)) return false;

        if (auto str_col = std::dynamic_pointer_cast<columnar::StringColumn>(column)) {
            return (*str_col)[row_idx].find(substr) != std::string::npos;
        }
        return false;
    };
}

FilterNode::Predicate string_starts_with(const std::string& col, const std::string& prefix) {
    return [=](std::shared_ptr<Table> table, size_t row_idx) -> bool {
        auto column = table->get_column(col);
        if (column->is_null(row_idx)) return false;

        if (auto str_col = std::dynamic_pointer_cast<columnar::StringColumn>(column)) {
            return (*str_col)[row_idx].rfind(prefix, 0) == 0;
        }
        return false;
    };
}

FilterNode::Predicate and_predicates(FilterNode::Predicate p1, FilterNode::Predicate p2) {
    return [=](std::shared_ptr<Table> table, size_t row_idx) -> bool {
        return p1(table, row_idx) && p2(table, row_idx);
    };
}

FilterNode::Predicate or_predicates(FilterNode::Predicate p1, FilterNode::Predicate p2) {
    return [=](std::shared_ptr<Table> table, size_t row_idx) -> bool {
        return p1(table, row_idx) || p2(table, row_idx);
    };
}

FilterNode::Predicate not_predicate(FilterNode::Predicate p) {
    return [=](std::shared_ptr<Table> table, size_t row_idx) -> bool {
        return !p(table, row_idx);
    };
}

} // namespace filter_ops

} // namespace ops
} // namespace functionome
