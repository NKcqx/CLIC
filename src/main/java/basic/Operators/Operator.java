package basic.Operators;

import basic.Visitors.Visitor;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public abstract class Operator implements Visitable {
    protected final String ID = "Operator";
    protected String optName;
    // protected Collection<Operator> successors;
    public Operator(String n){
        this.optName = n;
        // this.successors = new LinkedList<>();
    }

    public String getOptName(){return this.optName;}

    public String getID(){return this.ID;}


    @Override
    public String toString() {
        return "Operator{" +
                "ID='" + ID + '\'' +
                ", optName='" + optName + '\'' +
                '}';
    }

    Integer estimateCost(){
        return 0;
    }

}
