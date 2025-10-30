#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>

#include "functionome_dag/dag/node.hpp"
#include "functionome_dag/dag/graph.hpp"
#include "functionome_dag/dag/optimizer.hpp"
#include "functionome_dag/ops/map.hpp"
#include "functionome_dag/ops/filter.hpp"
#include "functionome_dag/ops/aggregate.hpp"
#include "functionome_dag/columnar/table.hpp"
#include "functionome_dag/columnar/column.hpp"

namespace py = pybind11;
using namespace functionome;

PYBIND11_MODULE(pyfunctionome_dag, m) {
    m.doc() = "FunctionomeDAG - High-performance columnar data processing with operation fusion";

    // Columnar module
    auto m_columnar = m.def_submodule("columnar", "Columnar storage");

    py::enum_<columnar::DataType>(m_columnar, "DataType")
        .value("INT32", columnar::DataType::INT32)
        .value("INT64", columnar::DataType::INT64)
        .value("FLOAT32", columnar::DataType::FLOAT32)
        .value("FLOAT64", columnar::DataType::FLOAT64)
        .value("STRING", columnar::DataType::STRING)
        .value("BOOL", columnar::DataType::BOOL);

    py::class_<columnar::Column, std::shared_ptr<columnar::Column>>(m_columnar, "Column")
        .def("type", &columnar::Column::type)
        .def("size", &columnar::Column::size)
        .def("is_null", &columnar::Column::is_null);

    py::class_<columnar::Int64Column, columnar::Column, std::shared_ptr<columnar::Int64Column>>(
        m_columnar, "Int64Column")
        .def(py::init<>())
        .def("append", &columnar::Int64Column::append,
             py::arg("value"), py::arg("is_null") = false)
        .def("__getitem__",
             [](const columnar::Int64Column& col, size_t idx) { return col[idx]; })
        .def("data", [](const columnar::Int64Column& col) { return col.data(); });

    py::class_<columnar::StringColumn, columnar::Column, std::shared_ptr<columnar::StringColumn>>(
        m_columnar, "StringColumn")
        .def(py::init<>())
        .def("append", &columnar::StringColumn::append,
             py::arg("value"), py::arg("is_null") = false)
        .def("__getitem__",
             [](const columnar::StringColumn& col, size_t idx) { return col[idx]; });

    py::class_<columnar::Table, std::shared_ptr<columnar::Table>>(m_columnar, "Table")
        .def(py::init<>())
        .def("add_column", &columnar::Table::add_column)
        .def("get_column", &columnar::Table::get_column)
        .def("column_names", &columnar::Table::column_names)
        .def("num_rows", &columnar::Table::num_rows)
        .def("num_columns", &columnar::Table::num_columns)
        .def("select", &columnar::Table::select)
        .def("slice", &columnar::Table::slice)
        .def("__repr__", [](const columnar::Table& t) { return t.to_string(10); });

    // DAG module
    auto m_dag = m.def_submodule("dag", "DAG computation graph");

    py::enum_<dag::OperationType>(m_dag, "OperationType")
        .value("SOURCE", dag::OperationType::SOURCE)
        .value("MAP", dag::OperationType::MAP)
        .value("FILTER", dag::OperationType::FILTER)
        .value("REDUCE", dag::OperationType::REDUCE)
        .value("AGGREGATE", dag::OperationType::AGGREGATE)
        .value("SINK", dag::OperationType::SINK);

    py::class_<dag::Node, std::shared_ptr<dag::Node>>(m_dag, "Node")
        .def("type", &dag::Node::type)
        .def("name", &dag::Node::name)
        .def("set_name", &dag::Node::set_name)
        .def("add_input", &dag::Node::add_input)
        .def("execute", &dag::Node::execute);

    py::class_<dag::SourceNode, dag::Node, std::shared_ptr<dag::SourceNode>>(m_dag, "SourceNode")
        .def(py::init<dag::SourceNode::LoadFunc, std::string>(),
             py::arg("loader"), py::arg("name") = "source");

    py::class_<dag::SinkNode, dag::Node, std::shared_ptr<dag::SinkNode>>(m_dag, "SinkNode")
        .def(py::init<dag::SinkNode::WriteFunc, std::string>(),
             py::arg("writer"), py::arg("name") = "sink");

    py::class_<dag::Graph>(m_dag, "Graph")
        .def(py::init<>())
        .def("add_node", &dag::Graph::add_node)
        .def("get_node", &dag::Graph::get_node)
        .def("execute", &dag::Graph::execute)
        .def("execute_all", &dag::Graph::execute_all)
        .def("topological_sort", &dag::Graph::topological_sort)
        .def("optimize", &dag::Graph::optimize)
        .def("to_dot", &dag::Graph::to_dot)
        .def("print_execution_plan", &dag::Graph::print_execution_plan);

    py::class_<dag::Optimizer>(m_dag, "Optimizer")
        .def(py::init<>())
        .def("optimize", &dag::Optimizer::optimize,
             py::arg("graph"), py::arg("max_iterations") = 10)
        .def("optimize_once", &dag::Optimizer::optimize_once)
        .def("applied_rules", &dag::Optimizer::applied_rules);

    // Operations module
    auto m_ops = m.def_submodule("ops", "DAG operations");

    py::class_<ops::MapNode, dag::Node, std::shared_ptr<ops::MapNode>>(m_ops, "MapNode")
        .def(py::init<ops::MapNode::MapFunc, std::string>(),
             py::arg("func"), py::arg("name") = "map");

    py::class_<ops::FilterNode, dag::Node, std::shared_ptr<ops::FilterNode>>(m_ops, "FilterNode")
        .def(py::init<ops::FilterNode::Predicate, std::string>(),
             py::arg("predicate"), py::arg("name") = "filter");

    py::enum_<ops::AggregateFunction>(m_ops, "AggregateFunction")
        .value("SUM", ops::AggregateFunction::SUM)
        .value("AVG", ops::AggregateFunction::AVG)
        .value("MIN", ops::AggregateFunction::MIN)
        .value("MAX", ops::AggregateFunction::MAX)
        .value("COUNT", ops::AggregateFunction::COUNT)
        .value("STDDEV", ops::AggregateFunction::STDDEV);

    py::class_<ops::AggregateSpec>(m_ops, "AggregateSpec")
        .def(py::init<std::string, ops::AggregateFunction, std::string>())
        .def_readwrite("input_column", &ops::AggregateSpec::input_column)
        .def_readwrite("function", &ops::AggregateSpec::function)
        .def_readwrite("output_column", &ops::AggregateSpec::output_column);

    py::class_<ops::AggregateNode, dag::Node, std::shared_ptr<ops::AggregateNode>>(
        m_ops, "AggregateNode")
        .def(py::init<std::vector<ops::AggregateSpec>, std::vector<std::string>, std::string>(),
             py::arg("specs"), py::arg("group_by_columns") = std::vector<std::string>(),
             py::arg("name") = "aggregate");

    // Utility functions
    m.def("version", []() { return "0.1.0"; });
}
