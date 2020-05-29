package basic.Visitors;

import basic.Operators.Operator;
import basic.PlanTraversal;

public class PrintVisitor extends Visitor {
    // private static final Logger logger = LoggerFactory.getLogger(PrintVisitor.class);

    public PrintVisitor(PlanTraversal planTraversal){
        super(planTraversal);
    }

    @Override
    public void visit(Operator opt) {
        this.logging(opt.toString());
        if (planTraversal.hasNextOpt()){
            planTraversal.nextOpt().acceptVisitor(this);
        }
    }

    private void logging(String s){
        System.out.println(s);
    }
}
