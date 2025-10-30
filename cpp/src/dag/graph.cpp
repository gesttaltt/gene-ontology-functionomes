#include "functionome_dag/dag/graph.hpp"
#include <algorithm>
#include <sstream>
#include <stdexcept>
#include <queue>

namespace functionome {
namespace dag {

void Graph::add_node(std::shared_ptr<Node> node) {
    if (!node) {
        throw std::invalid_argument("Cannot add null node to graph");
    }

    const std::string& name = node->name();
    if (nodes_.find(name) != nodes_.end()) {
        throw std::invalid_argument("Node with name '" + name + "' already exists");
    }

    nodes_[name] = std::move(node);
}

std::shared_ptr<Node> Graph::get_node(const std::string& name) const {
    auto it = nodes_.find(name);
    if (it == nodes_.end()) {
        throw std::invalid_argument("Node '" + name + "' not found");
    }
    return it->second;
}

std::shared_ptr<columnar::Table> Graph::execute(const std::string& sink_name) {
    auto node = get_node(sink_name);
    return node->execute();
}

void Graph::execute_all() {
    auto sorted = topological_sort();
    for (auto& node : sorted) {
        node->execute();
    }
}

std::vector<std::shared_ptr<Node>> Graph::topological_sort() const {
    validate_acyclic();

    // Calculate in-degrees
    std::unordered_map<Node*, int> in_degree;
    for (const auto& [name, node] : nodes_) {
        in_degree[node.get()] = 0;
    }

    for (const auto& [name, node] : nodes_) {
        for (const auto& input : node->inputs()) {
            in_degree[input.get()]++;
        }
    }

    // Kahn's algorithm
    std::queue<std::shared_ptr<Node>> queue;
    for (const auto& [name, node] : nodes_) {
        if (in_degree[node.get()] == 0) {
            queue.push(node);
        }
    }

    std::vector<std::shared_ptr<Node>> sorted;
    while (!queue.empty()) {
        auto node = queue.front();
        queue.pop();
        sorted.push_back(node);

        for (const auto& input : node->inputs()) {
            in_degree[input.get()]--;
            if (in_degree[input.get()] == 0) {
                queue.push(input);
            }
        }
    }

    if (sorted.size() != nodes_.size()) {
        throw std::runtime_error("Graph contains cycles");
    }

    return sorted;
}

void Graph::validate_acyclic() const {
    std::unordered_set<Node*> visited;
    std::unordered_set<Node*> rec_stack;

    for (const auto& [name, node] : nodes_) {
        if (visited.find(node.get()) == visited.end()) {
            if (has_cycle_util(node, visited, rec_stack)) {
                throw std::runtime_error("Graph contains cycle involving node: " + name);
            }
        }
    }
}

bool Graph::has_cycle_util(
    std::shared_ptr<Node> node,
    std::unordered_set<Node*>& visited,
    std::unordered_set<Node*>& rec_stack
) const {
    Node* raw_node = node.get();

    visited.insert(raw_node);
    rec_stack.insert(raw_node);

    for (const auto& input : node->inputs()) {
        Node* raw_input = input.get();

        if (visited.find(raw_input) == visited.end()) {
            if (has_cycle_util(input, visited, rec_stack)) {
                return true;
            }
        } else if (rec_stack.find(raw_input) != rec_stack.end()) {
            return true;
        }
    }

    rec_stack.erase(raw_node);
    return false;
}

std::vector<std::shared_ptr<Node>> Graph::find_fusible_chains() const {
    std::vector<std::shared_ptr<Node>> chains;

    for (const auto& [name, node] : nodes_) {
        // Look for Map->Map or Filter->Filter chains
        if (node->inputs().size() == 1) {
            auto input = node->inputs()[0];
            if (node->can_fuse_with(*input)) {
                chains.push_back(node);
            }
        }
    }

    return chains;
}

void Graph::optimize() {
    // Find and fuse compatible operations
    bool changed = true;
    while (changed) {
        changed = false;
        auto fusible = find_fusible_chains();

        for (auto& node : fusible) {
            if (node->inputs().size() != 1) continue;

            auto input = node->inputs()[0];
            auto fused = node->fuse_with(input);

            if (fused) {
                // Replace node with fused version
                nodes_[node->name()] = fused;
                changed = true;
            }
        }
    }
}

std::string Graph::to_dot() const {
    std::ostringstream oss;
    oss << "digraph DAG {\n";
    oss << "  rankdir=BT;\n";
    oss << "  node [shape=box];\n";

    for (const auto& [name, node] : nodes_) {
        oss << "  \"" << name << "\" [label=\"" << name << "\\n";

        switch (node->type()) {
            case OperationType::SOURCE: oss << "SOURCE"; break;
            case OperationType::MAP: oss << "MAP"; break;
            case OperationType::FILTER: oss << "FILTER"; break;
            case OperationType::REDUCE: oss << "REDUCE"; break;
            case OperationType::AGGREGATE: oss << "AGGREGATE"; break;
            case OperationType::SINK: oss << "SINK"; break;
        }

        oss << "\"];\n";

        for (const auto& input : node->inputs()) {
            oss << "  \"" << input->name() << "\" -> \"" << name << "\";\n";
        }
    }

    oss << "}\n";
    return oss.str();
}

void Graph::print_execution_plan() const {
    auto sorted = topological_sort();

    std::cout << "Execution Plan (" << sorted.size() << " nodes):\n";
    std::cout << "=========================================\n";

    for (size_t i = 0; i < sorted.size(); ++i) {
        const auto& node = sorted[i];
        std::cout << i + 1 << ". " << node->name();
        std::cout << " (mem: " << node->estimated_memory_bytes() / 1024 << " KB, ";
        std::cout << "ops: " << node->estimated_compute_ops() << ")\n";

        if (!node->inputs().empty()) {
            std::cout << "   Inputs: ";
            for (const auto& input : node->inputs()) {
                std::cout << input->name() << " ";
            }
            std::cout << "\n";
        }
    }
}

} // namespace dag
} // namespace functionome
