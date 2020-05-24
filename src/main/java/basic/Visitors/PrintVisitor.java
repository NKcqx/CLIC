package basic.Visitors;

import basic.Operators.ExecutableOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import basic.Operators.Operator;

public class PrintVisitor implements Visitor {
    // private static final Logger logger = LoggerFactory.getLogger(PrintVisitor.class);


    @Override
    public void visit(Operator opt) {
        this.logging(opt.toString());
    }

    @Override
    public void visit(ExecutableOperator opt) {
        this.logging(opt.toString());
    }


    private void logging(String s){
        System.out.println(s);
        //logger.info(s);
    }
}
