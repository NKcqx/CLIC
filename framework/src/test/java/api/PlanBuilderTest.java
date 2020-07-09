package api;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;

import static org.mockito.Mockito.*;

/**
 * @Author nathan
 * @Date 2020/7/8 7:07 下午
 * @Version 1.0
 */
public class PlanBuilderTest {
    @Test
    public void testReadFromData() throws Exception {
        int end=System.getProperty("user.dir").indexOf("framework");
        System.setProperty("user.dir",System.getProperty("user.dir").substring(0,end));
        System.out.println(System.getProperty("user.dir"));

        PlanBuilder planBuilder=new PlanBuilder();
        assert planBuilder.readDataFrom(new HashMap<String,String>(){{put("input", "data/test.csv");}})==planBuilder.getHeadDataQuanta();

        DataQuanta dataQuanta = DataQuanta.createInstance("source", new HashMap<String,String>(){{put("input", "data/test.csv");}});
        planBuilder.setHeadDataQuanta(dataQuanta);
        assert  planBuilder.getHeadDataQuanta()==dataQuanta;


    }
}
