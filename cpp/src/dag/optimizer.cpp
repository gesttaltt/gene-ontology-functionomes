#include "functionome_dag/dag/optimizer.hpp"
#include "functionome_dag/ops/map.hpp"
#include "functionome_dag/ops/filter.hpp"
#include <iostream>

namespace functionome {
namespace dag {

// MapFusionRule implementation

bool MapFusionRule::apply(Graph& graph) {
    bool changed = false;

    auto chains = graph.find_fusible_chains();
    for (auto& node : chains) {
        if (node->type() != OperationType::MAP) continue;
        if (node->inputs().size() != 1) continue;

        auto input = node->inputs()[0];
        if (input->type() != OperationType::MAP) continue;

        // Fuse the two map operations
        auto fused = node->fuse_with(input);
        if (fused) {
            std::cout << "  [MapFusion] Fused " << input->name()
                      << " -> " << node->name() << "\n";
            changed = true;
        }
    }

    return changed;
}

// FilterFusionRule implementation

bool FilterFusionRule::apply(Graph& graph) {
    bool changed = false;

    auto chains = graph.find_fusible_chains();
    for (auto& node : chains) {
        if (node->type() != OperationType::FILTER) continue;
        if (node->inputs().size() != 1) continue;

        auto input = node->inputs()[0];
        if (input->type() != OperationType::FILTER) continue;

        // Fuse the two filter operations
        auto fused = node->fuse_with(input);
        if (fused) {
            std::cout << "  [FilterFusion] Fused " << input->name()
                      << " -> " << node->name() << "\n";
            changed = true;
        }
    }

    return changed;
}

// FilterPushdownRule implementation

bool FilterPushdownRule::apply(Graph& graph) {
    // TODO: Implement filter pushdown optimization
    // This is more complex and requires analyzing data dependencies
    return false;
}

// IdentityEliminationRule implementation

bool IdentityEliminationRule::apply(Graph& graph) {
    // TODO: Detect and eliminate identity operations
    return false;
}

// Optimizer implementation

Optimizer::Optimizer() {
    // Add default optimization rules
    add_rule(std::make_unique<MapFusionRule>());
    add_rule(std::make_unique<FilterFusionRule>());
    // FilterPushdown and IdentityElimination are more complex, disabled for now
    // add_rule(std::make_unique<FilterPushdownRule>());
    // add_rule(std::make_unique<IdentityEliminationRule>());
}

void Optimizer::add_rule(std::unique_ptr<OptimizationRule> rule) {
    rules_.push_back(std::move(rule));
}

void Optimizer::optimize(Graph& graph, int max_iterations) {
    std::cout << "\n=== Optimizing DAG ===\n";

    applied_rules_.clear();

    for (int iteration = 0; iteration < max_iterations; ++iteration) {
        std::cout << "\nIteration " << (iteration + 1) << ":\n";

        bool changed = false;
        for (const auto& rule : rules_) {
            if (rule->apply(graph)) {
                applied_rules_.push_back(rule->name());
                changed = true;
            }
        }

        if (!changed) {
            std::cout << "  No more optimizations possible (fixed point reached)\n";
            break;
        }
    }

    std::cout << "\nOptimization complete. Applied rules: ";
    if (applied_rules_.empty()) {
        std::cout << "(none)\n";
    } else {
        for (const auto& rule : applied_rules_) {
            std::cout << rule << " ";
        }
        std::cout << "\n";
    }
}

void Optimizer::optimize_once(Graph& graph) {
    optimize(graph, 1);
}

} // namespace dag
} // namespace functionome
