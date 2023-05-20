package com.will.spark.design_pattern.visitor;

public class TravelCitys {
    City[] cities;
    public TravelCitys(){
        cities = new City[]{new Beijing(),new Shanghai(), new Chongqing(), new Tianjing()};
    }

    public void travel(Visitor visitor){
        for(City city:cities){
            city.accept(visitor);
        }
    }
}
