#pragma once

#include "../dag/node.hpp"
#include "../columnar/table.hpp"
#include <functional>

namespace functionome {
namespace ops {

using columnar::Table;

/**
 * Reduce operation - aggregates all rows to single value
 * (Simplified version of aggregate for single output)
 */

// For now, Reduce is covered by AggregateNode with no group-by

} // namespace ops
} // namespace functionome
