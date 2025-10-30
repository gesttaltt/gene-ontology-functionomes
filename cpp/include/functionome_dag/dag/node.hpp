#pragma once

#include <memory>
#include <string>
#include <vector>
#include <functional>

namespace functionome {
namespace dag {

// Forward declarations
class Table;

enum class OperationType {
    SOURCE,      // Data source (from file, memory, etc.)
    MAP,         // Transform each row
    FILTER,      // Select rows based on predicate
    REDUCE,      // Aggregate all rows to single value
    AGGREGATE,   // Group by + aggregate
    SINK         // Output destination
};

/**
 * Abstract base class for DAG nodes
 * Each node represents a lazy operation on columnar data
 */
class Node {
public:
    explicit Node(OperationType type, std::string name = "");
    virtual ~Node() = default;

    // DAG structure
    void add_input(std::shared_ptr<Node> input);
    const std::vector<std::shared_ptr<Node>>& inputs() const { return inputs_; }

    // Metadata
    OperationType type() const { return type_; }
    const std::string& name() const { return name_; }
    void set_name(const std::string& name) { name_ = name; }

    // Execution
    virtual std::shared_ptr<Table> execute() = 0;

    // Cost estimation for optimizer
    virtual size_t estimated_memory_bytes() const = 0;
    virtual size_t estimated_compute_ops() const = 0;

    // Fusion capability
    virtual bool can_fuse_with(const Node& other) const { return false; }
    virtual std::shared_ptr<Node> fuse_with(std::shared_ptr<Node> other) { return nullptr; }

protected:
    OperationType type_;
    std::string name_;
    std::vector<std::shared_ptr<Node>> inputs_;

    // Cached result for memoization
    mutable std::shared_ptr<Table> cached_result_;
    mutable bool cache_valid_ = false;
};

/**
 * Source node - loads data from external source
 */
class SourceNode : public Node {
public:
    using LoadFunc = std::function<std::shared_ptr<Table>()>;

    explicit SourceNode(LoadFunc loader, std::string name = "source");

    std::shared_ptr<Table> execute() override;
    size_t estimated_memory_bytes() const override;
    size_t estimated_compute_ops() const override;

private:
    LoadFunc loader_;
};

/**
 * Sink node - writes data to external destination
 */
class SinkNode : public Node {
public:
    using WriteFunc = std::function<void(std::shared_ptr<Table>)>;

    explicit SinkNode(WriteFunc writer, std::string name = "sink");

    std::shared_ptr<Table> execute() override;
    size_t estimated_memory_bytes() const override;
    size_t estimated_compute_ops() const override;

private:
    WriteFunc writer_;
};

} // namespace dag
} // namespace functionome
