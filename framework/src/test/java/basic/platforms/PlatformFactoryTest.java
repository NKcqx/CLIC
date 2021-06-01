package basic.platforms;

import basic.Configuration;
import basic.operators.OperatorFactory;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import scala.collection.mutable.HashMap;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class PlatformFactoryTest {
    Configuration configuration;
    Set<String> shouldGetSet = new HashSet<>();

    @Before
    public void before() throws Exception {
        configuration = new Configuration();
        PlatformFactory.initMapping(configuration.getProperty("platform-mapping-file"));
    }

    @Test
    public void testGetAllPlatform() throws Exception {
        Element rootOfPlt = getElement(configuration.getProperty("platform-mapping-file"));
        NodeList pairs = rootOfPlt.getElementsByTagName("pair");
        for (int i = 0; i < pairs.getLength(); i++) {
            Element pairNode = (Element) pairs.item(i);
            shouldGetSet.add(pairNode.getElementsByTagName("name").item(0).getTextContent());
        }
        assertEquals(shouldGetSet, PlatformFactory.getAllPlatform());
    }

    @Test
    public void testSetPlatformArgValue() throws Exception {
        Element rootOfPlt = getElement(configuration.getProperty("platform-mapping-file"));
        NodeList pairs = rootOfPlt.getElementsByTagName("pair");
        for (int i = 0; i < pairs.getLength(); i++) {
            Element pairNode = (Element) pairs.item(i);
            shouldGetSet.add(pairNode.getElementsByTagName("name").item(0).getTextContent());
        }
        for (String platform : shouldGetSet) {
            // 得到每个Platform的args属性
            Map<String, String> args = (Map) PlatformFactory.getConfigByPlatformName(platform).get("args");
            for (String arg : args.keySet()) {
                String argValue = args.get(arg);
                String argValueShouldBe = "test" + argValue;
                PlatformFactory.setPlatformArgValue(platform, arg, "test" + argValue);
                // 修改完更新当前的args
                Map<String, String> argsTemp = (Map) PlatformFactory.getConfigByPlatformName(platform).get("args");
                if (argValue.startsWith(arg)) argValueShouldBe = arg + "=" + argValueShouldBe;
                assertEquals(argsTemp.get(arg), argValueShouldBe);
            }
        }
    }

    @Test
    public void simpleTest() {
        System.out.println(PlatformFactory.getAllPlatform());
        System.out.println(PlatformFactory.getConfigByPlatformName("spark"));
    }

    private Element getElement(String xmlPath) throws Exception {
        InputStream configStream = OperatorFactory.class.getClassLoader().getResourceAsStream(xmlPath);
        assert configStream != null;
        Document configFile = DocumentBuilderFactory.newInstance()
                .newDocumentBuilder()
                .parse(configStream);
        return configFile.getDocumentElement();
    }
}
