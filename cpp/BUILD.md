# Building FunctionomeDAG

## Prerequisites

### Required
- CMake 3.15+
- C++17 compatible compiler (GCC 7+, Clang 5+, MSVC 2017+)
- Python 3.7+ (for bindings)

### Optional
- Apache Arrow 14.0+ (will be fetched automatically if not found)

## Quick Start

### Linux/macOS

```bash
# From project root
cd cpp
mkdir build && cd build

# Configure (without Arrow - fastest build)
cmake .. -DUSE_ARROW=OFF

# Or with Arrow support (recommended for production)
cmake .. -DUSE_ARROW=ON

# Build
make -j$(nproc)

# Install Python bindings
make install

# Test Python bindings
python3 -c "import pyfunctionome_dag; print(pyfunctionome_dag.version())"
```

### Windows (MSVC)

```powershell
cd cpp
mkdir build
cd build

# Configure
cmake .. -DUSE_ARROW=OFF

# Build
cmake --build . --config Release

# Install
cmake --build . --target install --config Release
```

### Windows (MinGW)

```bash
cd cpp
mkdir build && cd build

# Configure for MinGW
cmake .. -G "MinGW Makefiles" -DUSE_ARROW=OFF

# Build
mingw32-make -j4

# Install
mingw32-make install
```

## Build Options

### CMake Options

| Option | Default | Description |
|--------|---------|-------------|
| `USE_ARROW` | ON | Use Apache Arrow for columnar storage |
| `BUILD_PYTHON_BINDINGS` | ON | Build Python bindings with pybind11 |
| `BUILD_BENCHMARKS` | ON | Build benchmark suite |

Example:
```bash
cmake .. \
  -DUSE_ARROW=ON \
  -DBUILD_PYTHON_BINDINGS=ON \
  -DBUILD_BENCHMARKS=OFF
```

## Development Build

For faster iteration during development:

```bash
# Debug build with symbols
cmake .. -DCMAKE_BUILD_TYPE=Debug -DUSE_ARROW=OFF
make -j$(nproc)

# Run with verbose output
VERBOSE=1 make
```

## Troubleshooting

### Issue: Arrow not found

**Solution**: Disable Arrow (builds without it) or install manually:
```bash
# Disable Arrow
cmake .. -DUSE_ARROW=OFF

# Or install Arrow (Ubuntu/Debian)
sudo apt-get install libarrow-dev

# Or let CMake fetch it (slower)
cmake .. -DUSE_ARROW=ON
```

### Issue: Python bindings not found

**Solution**: Ensure Python development headers are installed:
```bash
# Ubuntu/Debian
sudo apt-get install python3-dev

# macOS
brew install python@3

# Then rebuild
cmake .. && make install
```

### Issue: MinGW build fails on Windows

**Solution**: Make sure MinGW is in PATH and specify generator:
```bash
cmake .. -G "MinGW Makefiles" -DCMAKE_CXX_COMPILER=g++
```

## Performance Tips

### 1. Release Build
```bash
cmake .. -DCMAKE_BUILD_TYPE=Release
```

### 2. Link-Time Optimization (LTO)
```bash
cmake .. -DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON
```

### 3. Native CPU Optimization
```bash
cmake .. -DCMAKE_CXX_FLAGS="-march=native"
```

## Verifying Installation

```python
import pyfunctionome_dag as dag

# Check version
print(f"FunctionomeDAG version: {dag.version()}")

# Create simple table
table = dag.columnar.Table()
col = dag.columnar.Int64Column()
col.append(1)
col.append(2)
col.append(3)
table.add_column("test", col)

print(f"Table has {table.num_rows()} rows")
# Output: Table has 3 rows
```

## Running the Pipeline

After building and installing:

```bash
# From project root
cd src

# Run with C++ DAG engine
python pipeline_dag.py

# Run without DAG (pure Python fallback)
python pipeline_dag.py --no-dag

# Export results
python pipeline_dag.py --export results.csv
```

## Benchmarking

If built with benchmarks enabled:

```bash
cd build
./benchmark_dag
```

Expected speedup: 5-10x over pure Python for typical workloads.
