#include "functionome_dag/dag/node.hpp"
#include "functionome_dag/columnar/table.hpp"

namespace functionome {
namespace dag {

using columnar::Table;

Node::Node(OperationType type, std::string name)
    : type_(type), name_(std::move(name)) {
    if (name_.empty()) {
        name_ = "node_" + std::to_string(reinterpret_cast<uintptr_t>(this));
    }
}

void Node::add_input(std::shared_ptr<Node> input) {
    inputs_.push_back(std::move(input));
    cache_valid_ = false;
}

// SourceNode implementation

SourceNode::SourceNode(LoadFunc loader, std::string name)
    : Node(OperationType::SOURCE, std::move(name))
    , loader_(std::move(loader)) {}

std::shared_ptr<Table> SourceNode::execute() {
    if (cache_valid_ && cached_result_) {
        return cached_result_;
    }

    cached_result_ = loader_();
    cache_valid_ = true;
    return cached_result_;
}

size_t SourceNode::estimated_memory_bytes() const {
    // Estimate based on typical data size
    return 1024 * 1024;  // 1 MB default estimate
}

size_t SourceNode::estimated_compute_ops() const {
    return 100;  // Minimal I/O overhead
}

// SinkNode implementation

SinkNode::SinkNode(WriteFunc writer, std::string name)
    : Node(OperationType::SINK, std::move(name))
    , writer_(std::move(writer)) {}

std::shared_ptr<Table> SinkNode::execute() {
    if (inputs_.empty()) {
        throw std::runtime_error("SinkNode requires at least one input");
    }

    auto input_table = inputs_[0]->execute();
    writer_(input_table);

    cached_result_ = input_table;
    cache_valid_ = true;
    return cached_result_;
}

size_t SinkNode::estimated_memory_bytes() const {
    return inputs_.empty() ? 0 : inputs_[0]->estimated_memory_bytes();
}

size_t SinkNode::estimated_compute_ops() const {
    return 100;  // Minimal I/O overhead
}

} // namespace dag
} // namespace functionome
