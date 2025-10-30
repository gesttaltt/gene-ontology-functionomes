#pragma once

#include "../dag/node.hpp"
#include "../columnar/table.hpp"
#include <functional>
#include <string>
#include <vector>

namespace functionome {
namespace ops {

using columnar::Table;

enum class AggregateFunction {
    SUM,
    AVG,
    MIN,
    MAX,
    COUNT,
    STDDEV
};

struct AggregateSpec {
    std::string input_column;
    AggregateFunction function;
    std::string output_column;
};

/**
 * Aggregate operation - computes summary statistics
 * Supports group-by aggregation
 */
class AggregateNode : public dag::Node {
public:
    explicit AggregateNode(
        std::vector<AggregateSpec> specs,
        std::vector<std::string> group_by_columns = {},
        std::string name = "aggregate"
    );

    std::shared_ptr<Table> execute() override;

    size_t estimated_memory_bytes() const override;
    size_t estimated_compute_ops() const override;

private:
    std::vector<AggregateSpec> specs_;
    std::vector<std::string> group_by_columns_;

    std::shared_ptr<Table> execute_simple_aggregation();
    std::shared_ptr<Table> execute_grouped_aggregation();
};

/**
 * Helper functions for common aggregations
 */
namespace aggregate_ops {

// Simple aggregations (no group by)
AggregateSpec sum(const std::string& input_col, const std::string& output_col = "");
AggregateSpec avg(const std::string& input_col, const std::string& output_col = "");
AggregateSpec min(const std::string& input_col, const std::string& output_col = "");
AggregateSpec max(const std::string& input_col, const std::string& output_col = "");
AggregateSpec count(const std::string& input_col, const std::string& output_col = "");

} // namespace aggregate_ops

} // namespace ops
} // namespace functionome
