#pragma once

#include "node.hpp"
#include <unordered_map>
#include <unordered_set>

namespace functionome {
namespace dag {

/**
 * DAG computational graph
 * Manages nodes and provides topological execution
 */
class Graph {
public:
    Graph() = default;

    // Graph construction
    void add_node(std::shared_ptr<Node> node);
    std::shared_ptr<Node> get_node(const std::string& name) const;

    // Execution
    std::shared_ptr<Table> execute(const std::string& sink_name);
    void execute_all();

    // Analysis
    std::vector<std::shared_ptr<Node>> topological_sort() const;
    std::vector<std::shared_ptr<Node>> find_fusible_chains() const;

    // Optimization
    void optimize();

    // Debugging
    std::string to_dot() const;
    void print_execution_plan() const;

private:
    std::unordered_map<std::string, std::shared_ptr<Node>> nodes_;

    void validate_acyclic() const;
    bool has_cycle_util(
        std::shared_ptr<Node> node,
        std::unordered_set<Node*>& visited,
        std::unordered_set<Node*>& rec_stack
    ) const;
};

} // namespace dag
} // namespace functionome
