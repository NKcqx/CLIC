import api.PlanBuilder;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class demo {
    public static void main(String[] args){
        Supplier<String> mapUDF = () -> {
            System.out.println("*****   Hello World   *****");
            return "";
        };


        PlanBuilder planBuilder = null;
        try {
            planBuilder = new PlanBuilder();
            planBuilder.sort();

            planBuilder.execute();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
