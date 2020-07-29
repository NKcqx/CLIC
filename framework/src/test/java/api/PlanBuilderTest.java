package api;

import org.junit.Test;

import java.util.HashMap;

/**
 * @Author nathan
 * @Date 2020/7/8 7:07 下午
 * @Version 1.0
 */
public class PlanBuilderTest {
    @Test
    public void testReadFromData() throws Exception {
        PlanBuilder planBuilder = new PlanBuilder();
        assert planBuilder.readDataFrom(new HashMap<String, String>() {{
            put("input_path", "data/test.csv");
        }}) == planBuilder.getHeadDataQuanta();

        DataQuanta dataQuanta = DataQuanta.createInstance("source", new HashMap<String, String>() {{
            put("input_path", "data/test.csv");
        }});
        planBuilder.setHeadDataQuanta(dataQuanta);
        assert planBuilder.getHeadDataQuanta() == dataQuanta;


    }
}
