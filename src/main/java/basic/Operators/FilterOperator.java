package basic.Operators;

import java.util.function.Predicate;

public  class FilterOperator extends Operator {
    public final String ID = "FilterOperator";

    private Predicate predicate;

    public FilterOperator(Predicate predicate, String optName) {
        super(optName);
        this.predicate = predicate;
    }

    @Override
    public String getID(){return this.ID;}

    public Predicate getUDF(){return this.predicate;}

    @Override
    Integer estimateCost(){
        return 10;
    }
}
