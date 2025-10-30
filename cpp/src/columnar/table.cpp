#include "functionome_dag/columnar/table.hpp"
#include <iostream>
#include <iomanip>
#include <sstream>
#include <stdexcept>

namespace functionome {
namespace columnar {

void Table::add_column(const std::string& name, std::shared_ptr<Column> column) {
    if (!column) {
        throw std::invalid_argument("Cannot add null column");
    }

    if (num_rows_ == 0) {
        num_rows_ = column->size();
    } else if (column->size() != num_rows_) {
        throw std::invalid_argument(
            "Column size (" + std::to_string(column->size()) +
            ") does not match table rows (" + std::to_string(num_rows_) + ")"
        );
    }

    if (columns_.find(name) != columns_.end()) {
        throw std::invalid_argument("Column '" + name + "' already exists");
    }

    columns_[name] = column;
    column_order_.push_back(name);
}

std::shared_ptr<Column> Table::get_column(const std::string& name) const {
    auto it = columns_.find(name);
    if (it == columns_.end()) {
        throw std::invalid_argument("Column '" + name + "' not found");
    }
    return it->second;
}

std::vector<std::string> Table::column_names() const {
    return column_order_;
}

size_t Table::num_rows() const {
    return num_rows_;
}

size_t Table::num_columns() const {
    return columns_.size();
}

std::shared_ptr<Table> Table::select(const std::vector<std::string>& columns) const {
    auto result = std::make_shared<Table>();

    for (const auto& col_name : columns) {
        auto col = get_column(col_name);
        result->add_column(col_name, col);
    }

    return result;
}

std::shared_ptr<Table> Table::slice(size_t offset, size_t length) const {
    if (offset + length > num_rows_) {
        throw std::out_of_range("Slice range out of bounds");
    }

    auto result = std::make_shared<Table>();

    for (const auto& col_name : column_order_) {
        auto col = columns_.at(col_name);
        auto sliced = col->slice(offset, length);
        result->add_column(col_name, sliced);
    }

    return result;
}

std::string Table::to_string(size_t max_rows) const {
    std::ostringstream oss;

    oss << "Table(" << num_rows_ << " rows x " << num_columns_ << " columns)\n";

    if (column_order_.empty()) {
        oss << "(empty table)\n";
        return oss.str();
    }

    // Print header
    oss << "┌";
    for (size_t i = 0; i < column_order_.size(); ++i) {
        oss << "────────────────";
        if (i < column_order_.size() - 1) oss << "┬";
    }
    oss << "┐\n│";

    for (const auto& col_name : column_order_) {
        oss << std::setw(15) << col_name << " │";
    }
    oss << "\n├";

    for (size_t i = 0; i < column_order_.size(); ++i) {
        oss << "────────────────";
        if (i < column_order_.size() - 1) oss << "┼";
    }
    oss << "┤\n";

    // Print rows (up to max_rows)
    size_t rows_to_print = std::min(num_rows_, max_rows);
    for (size_t row = 0; row < rows_to_print; ++row) {
        oss << "│";
        for (const auto& col_name : column_order_) {
            auto col = columns_.at(col_name);
            oss << std::setw(15);

            if (col->is_null(row)) {
                oss << "NULL";
            } else {
                // Type-specific printing (simplified)
                switch (col->type()) {
                    case DataType::INT32:
                    case DataType::INT64:
                    case DataType::FLOAT32:
                    case DataType::FLOAT64:
                        oss << "<num>";
                        break;
                    case DataType::STRING:
                        oss << "<str>";
                        break;
                    case DataType::BOOL:
                        oss << "<bool>";
                        break;
                }
            }
            oss << " │";
        }
        oss << "\n";
    }

    if (num_rows_ > max_rows) {
        oss << "│" << std::setw(15 * column_order_.size()) << "... ("
            << (num_rows_ - max_rows) << " more rows)" << " │\n";
    }

    oss << "└";
    for (size_t i = 0; i < column_order_.size(); ++i) {
        oss << "────────────────";
        if (i < column_order_.size() - 1) oss << "┴";
    }
    oss << "┘\n";

    return oss.str();
}

#ifdef USE_ARROW
std::shared_ptr<Table> Table::from_arrow(std::shared_ptr<arrow::Table> arrow_table) {
    // TODO: Implement Arrow conversion
    throw std::runtime_error("Arrow conversion not yet implemented");
}

std::shared_ptr<arrow::Table> Table::to_arrow() const {
    // TODO: Implement Arrow conversion
    throw std::runtime_error("Arrow conversion not yet implemented");
}
#endif

} // namespace columnar
} // namespace functionome
