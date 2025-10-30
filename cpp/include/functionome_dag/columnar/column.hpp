#pragma once

#include <memory>
#include <string>
#include <variant>
#include <vector>

#ifdef USE_ARROW
#include <arrow/array.h>
#include <arrow/type.h>
#endif

namespace functionome {
namespace columnar {

enum class DataType {
    INT32,
    INT64,
    FLOAT32,
    FLOAT64,
    STRING,
    BOOL
};

/**
 * Type-erased columnar storage
 * Stores data in contiguous memory for cache-friendly access
 */
class Column {
public:
    virtual ~Column() = default;

    virtual DataType type() const = 0;
    virtual size_t size() const = 0;
    virtual bool is_null(size_t index) const = 0;

    // Slice operation
    virtual std::shared_ptr<Column> slice(size_t offset, size_t length) const = 0;

#ifdef USE_ARROW
    virtual std::shared_ptr<arrow::Array> to_arrow() const = 0;
#endif
};

/**
 * Typed column implementation
 */
template<typename T>
class TypedColumn : public Column {
public:
    TypedColumn() = default;
    explicit TypedColumn(std::vector<T> data, std::vector<bool> null_bitmap = {});

    DataType type() const override;
    size_t size() const override { return data_.size(); }
    bool is_null(size_t index) const override;

    // Typed access
    const T& operator[](size_t index) const { return data_[index]; }
    T& operator[](size_t index) { return data_[index]; }

    const std::vector<T>& data() const { return data_; }
    std::vector<T>& data() { return data_; }

    void append(const T& value, bool is_null = false);

    std::shared_ptr<Column> slice(size_t offset, size_t length) const override;

#ifdef USE_ARROW
    std::shared_ptr<arrow::Array> to_arrow() const override;
    static std::shared_ptr<TypedColumn<T>> from_arrow(std::shared_ptr<arrow::Array> array);
#endif

private:
    std::vector<T> data_;
    std::vector<bool> null_bitmap_;  // true = null
};

// Common type aliases
using Int32Column = TypedColumn<int32_t>;
using Int64Column = TypedColumn<int64_t>;
using Float32Column = TypedColumn<float>;
using Float64Column = TypedColumn<double>;
using StringColumn = TypedColumn<std::string>;
using BoolColumn = TypedColumn<bool>;

} // namespace columnar
} // namespace functionome
