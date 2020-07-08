package api;

import org.junit.Test;

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
        int end=System.getProperty("user.dir").indexOf("framework")-1;
        System.setProperty("user.dir",System.getProperty("user.dir").substring(0,end));
        System.out.println(System.getProperty("user.dir"));

        PlanBuilder planBuilder=new PlanBuilder();
//        PlanBuilder test=spy(planBuilder);

    }
}
