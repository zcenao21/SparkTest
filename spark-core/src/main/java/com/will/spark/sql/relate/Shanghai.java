package com.will.spark.sql.relate;

public class Shanghai implements City{
    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }
}
