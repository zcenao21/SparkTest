package com.will.spark.sql.relate;

public class Demo {
    public static void main(String[] args) {
        TravelCitys citys = new TravelCitys();
        citys.travel(new SingleVisitor());
    }
}
