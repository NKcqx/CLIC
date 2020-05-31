package basic.Visitors;

import basic.Operators.Operator;
import basic.PlanTraversal;

import java.util.ArrayList;
import java.util.List;

public class PrintVisitor extends Visitor {
    // private static final Logger logger = LoggerFactory.getLogger(PrintVisitor.class);

    public PrintVisitor(PlanTraversal planTraversal){
        super(planTraversal);
    }
    private List<Operator> visited = new ArrayList<>();

    @Override
    public void visit(Operator opt) {
        if (!isVisited(opt)){
            this.logging(opt.toString());
            this.visited.add(opt);
        }
        if (planTraversal.hasNextOpt()){
            planTraversal.nextOpt().acceptVisitor(this);
        }
    }

    private boolean isVisited(Operator opt){
        return this.visited.contains(opt);
    }

    private void logging(String s){
        System.out.println(s);
    }
}
