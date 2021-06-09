package basic.visitor;

import basic.Configuration;
import basic.operators.Operator;
import basic.operators.OperatorFactory;
import basic.visitors.ExecuteVisitor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.List;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ExecuteVisitorTest {
    private ExecuteVisitor spyVisitor;
    private Field field;

    @Before
    public void before() throws NoSuchFieldException {
        ExecuteVisitor visitor = new ExecuteVisitor();
        spyVisitor = spy(visitor);

        // 使私有属性visited可访问
        field = ExecuteVisitor.class.getDeclaredField("visited");
        field.setAccessible(true);

        // 初始化OperatorFactory
        try {
            OperatorFactory.initMapping((new Configuration()).getProperty("operator-mapping-file"));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    @Test
    public void visitTest() {
        try {
            Operator op1 = Mockito.mock(Operator.class);
            Operator op2 = Mockito.mock(Operator.class);
            when(op1.evaluate()).thenReturn(true);
            when(op2.evaluate()).thenReturn(true);

            spyVisitor.visit(op1);
            Assert.assertEquals(((List) field.get(spyVisitor)).size(), 1);

            spyVisitor.visit(op2);
            Assert.assertEquals(((List) field.get(spyVisitor)).size(), 2);

            // 重复访问operator
            spyVisitor.visit(op2);
            Assert.assertEquals(((List) field.get(spyVisitor)).size(), 2);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
