package com.will.spark.sql.relate;

public interface Visitor {
    void visit(Shanghai city);
    void visit(Chongqing city);
    void visit(Tianjing city);
    void visit(Beijing city);
}
