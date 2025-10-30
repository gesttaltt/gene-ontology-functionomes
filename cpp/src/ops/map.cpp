#include "functionome_dag/ops/map.hpp"
#include <stdexcept>

namespace functionome {
namespace ops {

MapNode::MapNode(MapFunc func, std::string name)
    : Node(dag::OperationType::MAP, std::move(name))
    , func_(std::move(func)) {}

std::shared_ptr<Table> MapNode::execute() {
    if (cache_valid_ && cached_result_) {
        return cached_result_;
    }

    if (inputs_.empty()) {
        throw std::runtime_error("MapNode requires at least one input");
    }

    auto input_table = inputs_[0]->execute();

    // Apply primary function
    auto result = func_(input_table);

    // Apply fused functions if any
    for (const auto& fused_func : fused_funcs_) {
        result = fused_func(result);
    }

    cached_result_ = result;
    cache_valid_ = true;
    return cached_result_;
}

size_t MapNode::estimated_memory_bytes() const {
    if (inputs_.empty()) return 0;
    // Map typically produces output same size as input
    return inputs_[0]->estimated_memory_bytes();
}

size_t MapNode::estimated_compute_ops() const {
    if (inputs_.empty()) return 0;
    // Estimate based on input size and number of fused operations
    size_t base_ops = inputs_[0]->estimated_compute_ops();
    return base_ops * (1 + fused_funcs_.size());
}

bool MapNode::can_fuse_with(const dag::Node& other) const {
    return other.type() == dag::OperationType::MAP;
}

std::shared_ptr<dag::Node> MapNode::fuse_with(std::shared_ptr<dag::Node> other) {
    auto other_map = std::dynamic_pointer_cast<MapNode>(other);
    if (!other_map) {
        return nullptr;
    }

    // Create fused map node
    auto fused = std::make_shared<MapNode>(
        func_,
        name_ + "_fused_" + other->name()
    );

    // Copy our fused functions
    fused->fused_funcs_ = fused_funcs_;

    // Add other's function
    fused->fused_funcs_.push_back(other_map->func_);

    // Add other's fused functions
    fused->fused_funcs_.insert(
        fused->fused_funcs_.end(),
        other_map->fused_funcs_.begin(),
        other_map->fused_funcs_.end()
    );

    // Copy inputs from the earlier node in the chain
    for (const auto& input : other->inputs()) {
        fused->add_input(input);
    }

    return fused;
}

// Helper functions

namespace map_ops {

MapNode::MapFunc add_column(
    const std::string& output_col,
    const std::vector<std::string>& input_cols,
    std::function<double(const std::vector<double>&)> compute
) {
    return [=](std::shared_ptr<Table> table) -> std::shared_ptr<Table> {
        // Get input columns
        std::vector<std::shared_ptr<columnar::Column>> columns;
        for (const auto& col_name : input_cols) {
            columns.push_back(table->get_column(col_name));
        }

        size_t num_rows = table->num_rows();

        // Create output column (simplified - assumes int64 for now)
        auto output = std::make_shared<columnar::Int64Column>();

        for (size_t row = 0; row < num_rows; ++row) {
            // Extract values (simplified - needs type checking)
            std::vector<double> values;
            for (const auto& col : columns) {
                // This is simplified - real implementation needs proper type handling
                values.push_back(0.0);  // Placeholder
            }

            double result = compute(values);
            output->append(static_cast<int64_t>(result));
        }

        // Create new table with all columns plus the new one
        auto result_table = std::make_shared<Table>();
        for (const auto& col_name : table->column_names()) {
            result_table->add_column(col_name, table->get_column(col_name));
        }
        result_table->add_column(output_col, output);

        return result_table;
    };
}

MapNode::MapFunc rename_column(const std::string& old_name, const std::string& new_name) {
    return [=](std::shared_ptr<Table> table) -> std::shared_ptr<Table> {
        auto result = std::make_shared<Table>();

        for (const auto& col_name : table->column_names()) {
            if (col_name == old_name) {
                result->add_column(new_name, table->get_column(col_name));
            } else {
                result->add_column(col_name, table->get_column(col_name));
            }
        }

        return result;
    };
}

MapNode::MapFunc cast_column(const std::string& col, columnar::DataType target_type) {
    return [=](std::shared_ptr<Table> table) -> std::shared_ptr<Table> {
        // TODO: Implement type casting
        return table;  // Placeholder
    };
}

} // namespace map_ops

} // namespace ops
} // namespace functionome
