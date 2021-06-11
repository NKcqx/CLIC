package basic.operators;

import basic.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class OperatorEntityTest {

    private Configuration configuration;
    private List<Operator> operatorList = new ArrayList<>();

    // 解析函数打包
    private Element getElement(String xmlPath) throws Exception {
        InputStream configStream = OperatorFactory.class.getClassLoader().getResourceAsStream(xmlPath);
        assert configStream != null;
        Document configFile = DocumentBuilderFactory.newInstance()
                .newDocumentBuilder()
                .parse(configStream);
        return configFile.getDocumentElement();
    }

    // 解析所有操作符的Entity
    @Before
    public void before() throws Exception {
        configuration = new Configuration();
        // 初始化OperatorFactor
        OperatorFactory.initMapping(configuration.getProperty("operator-mapping-file"));
        // 获得配置文件中的所有ability
        Element rootOfOptMap = getElement(configuration.getProperty("operator-mapping-file"));
        NodeList pairs = rootOfOptMap.getElementsByTagName("pair");
        for (int i = 0; i < pairs.getLength(); i++) {
            Node pairNode = pairs.item(i);
            if (pairNode.getNodeType() == Node.ELEMENT_NODE) {
                Element pair = (Element) pairNode;
                String ability = pair.getElementsByTagName("ability").item(0).getTextContent();
                String template = pair.getElementsByTagName("template").item(0).getTextContent();
                System.out.println(template);
                Operator opt = OperatorFactory.createOperatorFromFile(template);
                operatorList.add(opt);
            }
        }
    }

    @Test
    public void testGetterAndSetter() throws Exception {
        for (Operator opt : operatorList) {
            for (OperatorEntity optEntity : opt.getEntities().values()) {
                optEntity.setEntityID("test" + optEntity.entityID);
                assertEquals(optEntity.getEntityID(), optEntity.entityID);
                optEntity.setLanguage("test" + optEntity.language);
                assertEquals(optEntity.getLanguage(), optEntity.language);
                assertEquals(optEntity.getCost(), optEntity.cost);
            }
        }
    }

    @Test
    public void testToString() throws Exception {
        for (Operator opt : operatorList) {
            for (OperatorEntity optEntity : opt.getEntities().values()) {
                assertEquals("OperatorEntity{"
                        + "language='" + optEntity.language + '\''
                        + ", cost=" + optEntity.cost
                        + '}', optEntity.toString());
            }
        }
    }
}
