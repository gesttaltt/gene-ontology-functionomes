#pragma once

#include "column.hpp"
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#ifdef USE_ARROW
#include <arrow/table.h>
#include <arrow/compute/api.h>
#endif

namespace functionome {
namespace columnar {

/**
 * Columnar table representation
 * Wraps Apache Arrow Table or custom column storage
 */
class Table {
public:
    Table() = default;

    // Construction
    void add_column(const std::string& name, std::shared_ptr<Column> column);

    // Access
    std::shared_ptr<Column> get_column(const std::string& name) const;
    std::vector<std::string> column_names() const;

    size_t num_rows() const;
    size_t num_columns() const;

    // Operations
    std::shared_ptr<Table> select(const std::vector<std::string>& columns) const;
    std::shared_ptr<Table> slice(size_t offset, size_t length) const;

#ifdef USE_ARROW
    // Arrow interop
    static std::shared_ptr<Table> from_arrow(std::shared_ptr<arrow::Table> arrow_table);
    std::shared_ptr<arrow::Table> to_arrow() const;
#endif

    // Debugging
    std::string to_string(size_t max_rows = 10) const;

private:
    std::unordered_map<std::string, std::shared_ptr<Column>> columns_;
    std::vector<std::string> column_order_;
    size_t num_rows_ = 0;
};

} // namespace columnar
} // namespace functionome
