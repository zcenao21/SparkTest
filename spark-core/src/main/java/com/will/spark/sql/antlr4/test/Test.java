package com.will.spark.sql.antlr4.test;

import com.will.spark.sql.antlr4.astImp.CalculatorVisitorImpl;
import com.will.spark.sql.antlr4.gen.CalculatorBaseVisitor;
import com.will.spark.sql.antlr4.gen.CalculatorLexer;
import com.will.spark.sql.antlr4.gen.CalculatorParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

public class Test {
    public static void main(String[] args) {
        String expression = "a = 12\n" +
                "b = a * 3\n" +
                "a + b\n" +
                "a - b\n";
        CalculatorLexer lexer = new CalculatorLexer(CharStreams.fromString(expression));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        CalculatorParser parser = new CalculatorParser(tokens);
        parser.setBuildParseTree(true);
        ParseTree root = parser.prog();
        CalculatorBaseVisitor<Integer> visitor = new CalculatorVisitorImpl();
        root.accept(visitor);
    }
}
