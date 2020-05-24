package basic.Operators;


import basic.Visitors.Visitor;

import java.util.function.Function;
import java.util.function.Supplier;

public  class MapOperator extends Operator{
    public final String ID = "MapOperator";
    protected Supplier func;

    public MapOperator(Supplier func, String optName){
        super(optName);
        this.func = func;
    }

    @Override
    public String getID(){return this.ID;}

    public Supplier getUDF(){return this.func;}


    @Override
    Integer estimateCost(){
        return 5;
    }

    @Override
    public void acceptVisitor(Visitor visitor) {
        visitor.visit(this);
    }
}
