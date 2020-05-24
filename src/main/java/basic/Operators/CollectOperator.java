package basic.Operators;

import basic.Visitors.Visitor;

public class CollectOperator extends Operator{
    public final String ID = "CollectOperator";

    public CollectOperator(String optName) {
        super(optName);
    }

    @Override
    public String getID(){return this.ID;}

    @Override
    Integer estimateCost(){
        return 10;
    }

    @Override
    public void acceptVisitor(Visitor visitor) {
        visitor.visit(this);
    }
}
