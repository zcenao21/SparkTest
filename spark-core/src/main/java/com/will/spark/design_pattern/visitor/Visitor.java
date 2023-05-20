package com.will.spark.design_pattern.visitor;

public interface Visitor {
    void visit(Shanghai city);
    void visit(Chongqing city);
    void visit(Tianjing city);
    void visit(Beijing city);
}
