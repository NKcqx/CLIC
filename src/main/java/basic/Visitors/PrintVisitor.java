package basic.Visitors;

import basic.Operators.Operator;

public class PrintVisitor implements Visitor {
    // private static final Logger logger = LoggerFactory.getLogger(PrintVisitor.class);

    @Override
    public void visit(Operator opt) {
        this.logging(opt.toString());
    }

    private void logging(String s){
        System.out.println(s);
    }
}
