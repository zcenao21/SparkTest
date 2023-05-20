package com.will.spark.design_pattern.visitor;

public class Tianjing implements City{
    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }
}
