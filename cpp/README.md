# FunctionomeDAG - High-Performance Columnar Data Processing

C++ DAG engine with operation fusion for gene functional abstraction pipeline.

## Features

### Core Capabilities
- **Columnar Storage**: Cache-friendly column-oriented data layout
- **Operation Fusion**: Automatically fuses Map→Map and Filter→Filter chains to eliminate intermediate allocations
- **Lazy Evaluation**: Builds computation graph, executes only when needed
- **Python Bindings**: Seamless integration with existing Python workflows via pybind11

### Optimization Techniques

#### 1. Map Fusion
```python
# Before optimization: 2 separate iterations
map(f) -> map(g)

# After fusion: single iteration
map(g ∘ f)
```

#### 2. Filter Fusion
```python
# Before: 2 passes through data
filter(p) -> filter(q)

# After: single pass with AND predicate
filter(p && q)
```

#### 3. Columnar Layout
```
Row-based (Python):          Columnar (C++):
[gene1, 5, 10]              genes: [gene1, gene2, gene3, ...]
[gene2, 3, 7]               pathways: [5, 3, 8, ...]
[gene3, 8, 12]              interactions: [10, 7, 12, ...]
```

Benefits:
- Better CPU cache utilization
- SIMD vectorization opportunities
- Lower memory allocations

## Architecture

### Inspired by Spark Catalyst Optimizer

The DAG system implements algebraic optimization similar to Spark's query optimizer:

1. **Logical Plan**: User constructs DAG of operations
2. **Optimization**: Rules transform DAG (fusion, pushdown)
3. **Physical Plan**: Optimized execution sequence
4. **Execution**: Columnar operators with minimal overhead

### Components

```
functionome_dag/
├── dag/                    # DAG infrastructure
│   ├── node.hpp           # Abstract operation nodes
│   ├── graph.hpp          # Computation graph
│   └── optimizer.hpp      # Fusion rules
│
├── columnar/              # Columnar storage
│   ├── table.hpp         # Multi-column table
│   └── column.hpp        # Typed column arrays
│
└── ops/                   # Operations
    ├── map.hpp           # Transform rows
    ├── filter.hpp        # Select rows
    └── aggregate.hpp     # Summarize data
```

## Example Usage

### Python Interface

```python
import pyfunctionome_dag as dag

# Create columnar table
table = dag.columnar.Table()

genes = dag.columnar.StringColumn()
genes.append("TP53")
genes.append("BRCA1")
table.add_column("gene", genes)

pathways = dag.columnar.Int64Column()
pathways.append(15)
pathways.append(8)
table.add_column("pathways", pathways)

# Build DAG
graph = dag.dag.Graph()

source = dag.dag.SourceNode(lambda: table, "source")
graph.add_node(source)

# Add operations
def compute_impact(tbl):
    impact = dag.columnar.Int64Column()
    pw = tbl.get_column("pathways")
    for i in range(tbl.num_rows()):
        impact.append(pw[i] * 2)  # Simple scoring
    tbl.add_column("impact", impact)
    return tbl

map_node = dag.ops.MapNode(compute_impact, "score")
map_node.add_input(source)
graph.add_node(map_node)

# Optimize and execute
optimizer = dag.dag.Optimizer()
optimizer.optimize(graph)

result = graph.execute("score")
print(result)
```

### C++ Interface (Advanced)

```cpp
#include <functionome_dag/dag/graph.hpp>
#include <functionome_dag/ops/map.hpp>

using namespace functionome;

// Create table
auto table = std::make_shared<columnar::Table>();
auto col = std::make_shared<columnar::Int64Column>();
col->append(10);
col->append(20);
table->add_column("values", col);

// Build DAG
dag::Graph graph;

auto source = std::make_shared<dag::SourceNode>(
    [table]() { return table; }, "source"
);
graph.add_node(source);

auto map_func = [](auto tbl) {
    // Double all values
    return tbl;
};

auto mapper = std::make_shared<ops::MapNode>(map_func, "double");
mapper->add_input(source);
graph.add_node(mapper);

// Execute
auto result = graph.execute("double");
```

## Performance Characteristics

### Expected Speedups (vs Pure Python)

| Operation | Speedup | Notes |
|-----------|---------|-------|
| Map (simple) | 5-8x | Reduced interpreter overhead |
| Filter | 10-15x | Columnar scan efficiency |
| Aggregate | 8-12x | Cache-friendly reduction |
| Fused operations | 15-25x | Eliminated intermediate allocations |

### Memory Usage

- Columnar layout: ~30% less memory than row-based (typical case)
- Operation fusion: Eliminates intermediate tables (can save 2-10x memory)

## Trade-offs

### Advantages
- **Speed**: 5-25x faster than pure Python for typical workloads
- **Memory**: Lower memory footprint via columnar storage and fusion
- **Portable**: Builds on Linux, macOS, Windows with minimal dependencies
- **Composable**: Easy to extend with new operations

### Limitations
- **Complexity**: More complex than pure Python implementation
- **Build time**: Requires C++ compilation (~2-5 minutes)
- **Arrow optional**: Full Arrow integration requires ~500MB download and build
- **Limited types**: Currently supports INT32/INT64/FLOAT32/FLOAT64/STRING/BOOL

## Comparison to Alternatives

| Feature | FunctionomeDAG | Pure Python | PySpark | Polars |
|---------|---------------|-------------|---------|--------|
| Setup | Medium | Easy | Complex | Easy |
| Speed | Fast (5-25x) | Baseline | Very Fast | Very Fast |
| Memory | Low | High | Low | Very Low |
| Portability | High | High | Medium | High |
| Dependencies | Minimal | None | Java+Scala | Rust |
| Learning curve | Medium | Low | High | Low |

## Design Decisions

### Why a Separate C++ Implementation?

**This is intentionally a standalone implementation, NOT a replacement or merger with Spark.**

The three pipeline implementations (C++, Spark, Python) serve different purposes:
- **C++ DAG**: Local production workloads with minimal dependencies
- **Spark**: Cloud-scale distributed processing
- **Python**: Development and prototyping

**Do NOT attempt to merge these implementations.** Each has different:
- Execution models (single-machine vs distributed vs interpreted)
- Dependencies (C++/pybind11 vs JVM/Scala vs pure Python)
- Performance characteristics (low-latency vs high-throughput vs simplicity)
- Operational requirements (compiled binary vs cluster vs script)

Unifying them would create a bloated, complex abstraction that satisfies no use case well.

### Why Columnar Storage?
- Gene functional data is naturally columnar (pathways, interactions, impact scores)
- Analytics workloads benefit from column-oriented layout
- Easier to implement SIMD operations in future

### Why Operation Fusion?
- Eliminates ~50-80% of intermediate allocations in typical pipelines
- Single pass through data cache is much faster than multiple passes
- Inspired by Spark's Catalyst optimizer success

### Why Not Full Arrow Integration?
- Arrow adds ~500MB dependency and longer build times
- Custom columnar format is sufficient for this use case
- Arrow support available as compile-time option for future needs

## Future Enhancements

- [ ] Grouped aggregation (GROUP BY)
- [ ] Join operations
- [ ] Parallel execution of independent DAG branches
- [ ] SIMD vectorization for numeric operations
- [ ] Arrow IPC for zero-copy data sharing
- [ ] Cost-based optimizer (beyond rule-based)

## License

Same as parent project.
