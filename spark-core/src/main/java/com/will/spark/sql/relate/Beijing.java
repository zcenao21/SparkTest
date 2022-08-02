package com.will.spark.sql.relate;

public class Beijing implements City{
    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }
}
