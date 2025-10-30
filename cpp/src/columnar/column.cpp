#include "functionome_dag/columnar/column.hpp"
#include <stdexcept>

namespace functionome {
namespace columnar {

// TypedColumn implementation

template<typename T>
TypedColumn<T>::TypedColumn(std::vector<T> data, std::vector<bool> null_bitmap)
    : data_(std::move(data))
    , null_bitmap_(std::move(null_bitmap)) {
    if (!null_bitmap_.empty() && null_bitmap_.size() != data_.size()) {
        throw std::invalid_argument("Null bitmap size must match data size");
    }
}

template<typename T>
DataType TypedColumn<T>::type() const {
    if constexpr (std::is_same_v<T, int32_t>) return DataType::INT32;
    else if constexpr (std::is_same_v<T, int64_t>) return DataType::INT64;
    else if constexpr (std::is_same_v<T, float>) return DataType::FLOAT32;
    else if constexpr (std::is_same_v<T, double>) return DataType::FLOAT64;
    else if constexpr (std::is_same_v<T, std::string>) return DataType::STRING;
    else if constexpr (std::is_same_v<T, bool>) return DataType::BOOL;
    else throw std::runtime_error("Unsupported column type");
}

template<typename T>
bool TypedColumn<T>::is_null(size_t index) const {
    if (index >= data_.size()) {
        throw std::out_of_range("Column index out of range");
    }
    return !null_bitmap_.empty() && null_bitmap_[index];
}

template<typename T>
void TypedColumn<T>::append(const T& value, bool is_null) {
    data_.push_back(value);
    if (!null_bitmap_.empty() || is_null) {
        if (null_bitmap_.empty()) {
            null_bitmap_.resize(data_.size() - 1, false);
        }
        null_bitmap_.push_back(is_null);
    }
}

template<typename T>
std::shared_ptr<Column> TypedColumn<T>::slice(size_t offset, size_t length) const {
    if (offset + length > data_.size()) {
        throw std::out_of_range("Slice range out of bounds");
    }

    std::vector<T> sliced_data(data_.begin() + offset, data_.begin() + offset + length);
    std::vector<bool> sliced_nulls;

    if (!null_bitmap_.empty()) {
        sliced_nulls = std::vector<bool>(
            null_bitmap_.begin() + offset,
            null_bitmap_.begin() + offset + length
        );
    }

    return std::make_shared<TypedColumn<T>>(std::move(sliced_data), std::move(sliced_nulls));
}

#ifdef USE_ARROW
template<typename T>
std::shared_ptr<arrow::Array> TypedColumn<T>::to_arrow() const {
    // TODO: Implement Arrow conversion
    throw std::runtime_error("Arrow conversion not yet implemented");
}

template<typename T>
std::shared_ptr<TypedColumn<T>> TypedColumn<T>::from_arrow(std::shared_ptr<arrow::Array> array) {
    // TODO: Implement Arrow conversion
    throw std::runtime_error("Arrow conversion not yet implemented");
}
#endif

// Explicit template instantiations
template class TypedColumn<int32_t>;
template class TypedColumn<int64_t>;
template class TypedColumn<float>;
template class TypedColumn<double>;
template class TypedColumn<std::string>;
template class TypedColumn<bool>;

} // namespace columnar
} // namespace functionome
