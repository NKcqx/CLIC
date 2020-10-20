package basic.visitor;

import basic.Configuration;
import basic.operators.Operator;
import basic.operators.OperatorFactory;
import basic.visitors.ExecutionGenerationVisitor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;

import static org.mockito.Mockito.*;

public class ExecutionGenerationVisitorTest {
    private ExecutionGenerationVisitor spyVisitor;
    private Field field;

    @Before
    public void before() throws NoSuchFieldException {
        ExecutionGenerationVisitor visitor = new ExecutionGenerationVisitor();
        spyVisitor = spy(visitor);

        // 使私有属性visited可访问
        field = ExecutionGenerationVisitor.class.getDeclaredField("visited");
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
            Operator op1 = OperatorFactory.createOperator("map");
            spyVisitor.visit(op1);
            Assert.assertEquals(((List) field.get(spyVisitor)).size(),1);

            Operator op2 = OperatorFactory.createOperator("sort");
            spyVisitor.visit(op2);
            Assert.assertEquals(((List) field.get(spyVisitor)).size(),2);

            // 重复访问operator
            spyVisitor.visit(op2);
            Assert.assertEquals(((List) field.get(spyVisitor)).size(),2);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
