package com.will.spark.sql.relate;

public class Tianjing implements City{
    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }
}
