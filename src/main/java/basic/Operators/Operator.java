package basic.Operators;

public class Operator {
    protected final String ID = "Operator";
    protected String optName;

    public Operator(String n){this.optName = n;}

    public String getOptName(){return this.optName;}

    public String getID(){return this.ID;}


    Integer estimateCost(){
        return 0;
    }

}
