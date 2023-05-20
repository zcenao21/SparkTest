package com.will.spark.design_pattern.visitor;

public class Demo {
    public static void main(String[] args) {
        TravelCitys citys = new TravelCitys();
        citys.travel(new SingleVisitor());
    }
}
