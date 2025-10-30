#pragma once

#include "graph.hpp"
#include <vector>

namespace functionome {
namespace dag {

/**
 * Optimization rules for DAG transformation
 * Inspired by Spark Catalyst optimizer
 */
class OptimizationRule {
public:
    virtual ~OptimizationRule() = default;

    virtual std::string name() const = 0;
    virtual bool apply(Graph& graph) = 0;  // Returns true if graph was modified
};

/**
 * Fuses consecutive Map operations into single operation
 * map(f) -> map(g) becomes map(g ∘ f)
 */
class MapFusionRule : public OptimizationRule {
public:
    std::string name() const override { return "MapFusion"; }
    bool apply(Graph& graph) override;
};

/**
 * Fuses consecutive Filter operations
 * filter(p) -> filter(q) becomes filter(p && q)
 */
class FilterFusionRule : public OptimizationRule {
public:
    std::string name() const override { return "FilterFusion"; }
    bool apply(Graph& graph) override;
};

/**
 * Pushes Filter before Map when possible (reduces data early)
 * map(f) -> filter(p) becomes filter(p ∘ f⁻¹) -> map(f) if beneficial
 */
class FilterPushdownRule : public OptimizationRule {
public:
    std::string name() const override { return "FilterPushdown"; }
    bool apply(Graph& graph) override;
};

/**
 * Eliminates redundant operations
 * map(identity) -> node becomes node
 */
class IdentityEliminationRule : public OptimizationRule {
public:
    std::string name() const override { return "IdentityElimination"; }
    bool apply(Graph& graph) override;
};

/**
 * Main optimizer - applies multiple rules in sequence
 */
class Optimizer {
public:
    Optimizer();

    void add_rule(std::unique_ptr<OptimizationRule> rule);

    // Apply all rules until fixed point (no more changes)
    void optimize(Graph& graph, int max_iterations = 10);

    // Apply rules once
    void optimize_once(Graph& graph);

    const std::vector<std::string>& applied_rules() const { return applied_rules_; }

private:
    std::vector<std::unique_ptr<OptimizationRule>> rules_;
    std::vector<std::string> applied_rules_;
};

} // namespace dag
} // namespace functionome
