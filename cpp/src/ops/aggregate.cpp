#include "functionome_dag/ops/aggregate.hpp"
#include <stdexcept>
#include <unordered_map>
#include <cmath>
#include <limits>

namespace functionome {
namespace ops {

AggregateNode::AggregateNode(
    std::vector<AggregateSpec> specs,
    std::vector<std::string> group_by_columns,
    std::string name
) : Node(dag::OperationType::AGGREGATE, std::move(name))
  , specs_(std::move(specs))
  , group_by_columns_(std::move(group_by_columns)) {}

std::shared_ptr<Table> AggregateNode::execute() {
    if (cache_valid_ && cached_result_) {
        return cached_result_;
    }

    if (inputs_.empty()) {
        throw std::runtime_error("AggregateNode requires at least one input");
    }

    auto input_table = inputs_[0]->execute();

    if (group_by_columns_.empty()) {
        cached_result_ = execute_simple_aggregation();
    } else {
        cached_result_ = execute_grouped_aggregation();
    }

    cache_valid_ = true;
    return cached_result_;
}

std::shared_ptr<Table> AggregateNode::execute_simple_aggregation() {
    auto input_table = inputs_[0]->execute();
    auto result = std::make_shared<Table>();

    for (const auto& spec : specs_) {
        auto input_col = input_table->get_column(spec.input_column);
        std::string output_name = spec.output_column.empty()
            ? spec.input_column + "_agg"
            : spec.output_column;

        // Compute aggregate based on function type
        int64_t result_value = 0;

        if (auto int_col = std::dynamic_pointer_cast<columnar::Int64Column>(input_col)) {
            const auto& data = int_col->data();

            switch (spec.function) {
                case AggregateFunction::SUM: {
                    int64_t sum = 0;
                    for (size_t i = 0; i < data.size(); ++i) {
                        if (!int_col->is_null(i)) {
                            sum += data[i];
                        }
                    }
                    result_value = sum;
                    break;
                }

                case AggregateFunction::AVG: {
                    int64_t sum = 0;
                    size_t count = 0;
                    for (size_t i = 0; i < data.size(); ++i) {
                        if (!int_col->is_null(i)) {
                            sum += data[i];
                            count++;
                        }
                    }
                    result_value = count > 0 ? sum / count : 0;
                    break;
                }

                case AggregateFunction::MIN: {
                    int64_t min_val = std::numeric_limits<int64_t>::max();
                    for (size_t i = 0; i < data.size(); ++i) {
                        if (!int_col->is_null(i) && data[i] < min_val) {
                            min_val = data[i];
                        }
                    }
                    result_value = min_val;
                    break;
                }

                case AggregateFunction::MAX: {
                    int64_t max_val = std::numeric_limits<int64_t>::min();
                    for (size_t i = 0; i < data.size(); ++i) {
                        if (!int_col->is_null(i) && data[i] > max_val) {
                            max_val = data[i];
                        }
                    }
                    result_value = max_val;
                    break;
                }

                case AggregateFunction::COUNT: {
                    size_t count = 0;
                    for (size_t i = 0; i < data.size(); ++i) {
                        if (!int_col->is_null(i)) {
                            count++;
                        }
                    }
                    result_value = count;
                    break;
                }

                case AggregateFunction::STDDEV: {
                    // Simplified stddev calculation
                    double sum = 0, sum_sq = 0;
                    size_t count = 0;
                    for (size_t i = 0; i < data.size(); ++i) {
                        if (!int_col->is_null(i)) {
                            double val = static_cast<double>(data[i]);
                            sum += val;
                            sum_sq += val * val;
                            count++;
                        }
                    }
                    if (count > 0) {
                        double mean = sum / count;
                        double variance = (sum_sq / count) - (mean * mean);
                        result_value = static_cast<int64_t>(std::sqrt(variance));
                    }
                    break;
                }
            }
        }

        auto output_col = std::make_shared<columnar::Int64Column>();
        output_col->append(result_value);
        result->add_column(output_name, output_col);
    }

    return result;
}

std::shared_ptr<Table> AggregateNode::execute_grouped_aggregation() {
    // TODO: Implement grouped aggregation with hash table
    throw std::runtime_error("Grouped aggregation not yet implemented");
}

size_t AggregateNode::estimated_memory_bytes() const {
    if (inputs_.empty()) return 0;

    if (group_by_columns_.empty()) {
        // Simple aggregation produces single row
        return 1024;  // Small constant
    } else {
        // Grouped aggregation size depends on cardinality
        return inputs_[0]->estimated_memory_bytes() / 10;  // Estimate 10% of input
    }
}

size_t AggregateNode::estimated_compute_ops() const {
    if (inputs_.empty()) return 0;
    // One pass through data for aggregation
    return inputs_[0]->estimated_compute_ops();
}

// Helper functions

namespace aggregate_ops {

AggregateSpec sum(const std::string& input_col, const std::string& output_col) {
    return {input_col, AggregateFunction::SUM, output_col.empty() ? input_col + "_sum" : output_col};
}

AggregateSpec avg(const std::string& input_col, const std::string& output_col) {
    return {input_col, AggregateFunction::AVG, output_col.empty() ? input_col + "_avg" : output_col};
}

AggregateSpec min(const std::string& input_col, const std::string& output_col) {
    return {input_col, AggregateFunction::MIN, output_col.empty() ? input_col + "_min" : output_col};
}

AggregateSpec max(const std::string& input_col, const std::string& output_col) {
    return {input_col, AggregateFunction::MAX, output_col.empty() ? input_col + "_max" : output_col};
}

AggregateSpec count(const std::string& input_col, const std::string& output_col) {
    return {input_col, AggregateFunction::COUNT, output_col.empty() ? input_col + "_count" : output_col};
}

} // namespace aggregate_ops

} // namespace ops
} // namespace functionome
