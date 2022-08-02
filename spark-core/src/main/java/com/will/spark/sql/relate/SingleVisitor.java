package com.will.spark.sql.relate;

public class SingleVisitor implements Visitor{
    @Override
    public void visit(Shanghai city) {
        System.out.println("Visit Shang hai");
    }

    @Override
    public void visit(Chongqing city) {
        System.out.println("Visit Chong qing");
    }

    @Override
    public void visit(Tianjing city) {
        System.out.println("Visit Tian jin");
    }

    @Override
    public void visit(Beijing city) {
        System.out.println("Visit Bei Jing");
    }
}
