#pragma once

#include "../dag/node.hpp"
#include "../columnar/table.hpp"
#include <functional>

namespace functionome {
namespace ops {

using columnar::Table;

/**
 * Map operation - transforms each row independently
 * Supports operation fusion for performance
 */
class MapNode : public dag::Node {
public:
    using MapFunc = std::function<std::shared_ptr<Table>(std::shared_ptr<Table>)>;

    explicit MapNode(MapFunc func, std::string name = "map");

    std::shared_ptr<Table> execute() override;

    size_t estimated_memory_bytes() const override;
    size_t estimated_compute_ops() const override;

    // Fusion support
    bool can_fuse_with(const dag::Node& other) const override;
    std::shared_ptr<dag::Node> fuse_with(std::shared_ptr<dag::Node> other) override;

private:
    MapFunc func_;
    std::vector<MapFunc> fused_funcs_;  // Chain of fused operations
};

/**
 * Helper functions for common map operations
 */
namespace map_ops {

// Add new column by computing from existing columns
MapNode::MapFunc add_column(
    const std::string& output_col,
    const std::vector<std::string>& input_cols,
    std::function<double(const std::vector<double>&)> compute
);

// Rename column
MapNode::MapFunc rename_column(const std::string& old_name, const std::string& new_name);

// Cast column type
MapNode::MapFunc cast_column(const std::string& col, columnar::DataType target_type);

} // namespace map_ops

} // namespace ops
} // namespace functionome
