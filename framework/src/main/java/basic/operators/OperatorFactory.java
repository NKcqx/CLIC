/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:40 下午
 */
package basic.operators;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 单例类，实现API -> OperatorTemplate的映射，以供Operator找到对应的配置文件
 */
public final class OperatorFactory {
    private static Map<String, String> mapping = new HashMap<>();
    private OperatorFactory instance = new OperatorFactory();

    private OperatorFactory() {
    }

    public static void initMapping(String configPath) throws ParserConfigurationException, IOException, SAXException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document configFile = builder.parse(new File(configPath));
        configFile.getDocumentElement().normalize();

        Element root = configFile.getDocumentElement();
        NodeList pairs = root.getElementsByTagName("pair");

        for (int i = 0; i < pairs.getLength(); i++) {
            Node pairNode = pairs.item(i);
            if (pairNode.getNodeType() == Node.ELEMENT_NODE) {
                Element pair = (Element) pairNode;
                String ability = pair.getElementsByTagName("ability").item(0).getTextContent();
                String template = pair.getElementsByTagName("template").item(0).getTextContent();
                OperatorFactory.mapping.put(ability, template);
            }
        }
    }

    private static String getTemplate(String ability) throws Exception {
        if (OperatorFactory.mapping.containsKey(ability)) {
            return OperatorFactory.mapping.getOrDefault(ability, null);
        } else {
            throw new Exception(String.format("找不到与`%s`对应的Template，请重新检查配置文件", ability));
        }

    }

    /**
     * 根据用户需求，生成包含指定XML文件的Opt.（主要由API中调用）
     *
     * @param ability 希望Opt.具有的功能
     * @return 包含有具体配置文件的Opt.
     * @throws Exception
     */
    public static Operator createOperator(String ability) throws Exception {
        String templatePath = OperatorFactory.getTemplate(ability);

        Operator operator = new Operator(templatePath);
        // operator.loadConfiguration(template_path); // 这步交给Visitor来做
        return operator;
    }

    public OperatorFactory getInstance() {
        if (mapping.isEmpty()) { // 防止没初始化（或配置文件为空）就拿到对象
            return null;
        }
        return instance;
    }

}
