package basic.operators;

import basic.Param;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * 单例类，实现API -> OperatorTemplate的映射，以供Operator找到对应的配置文件
 *
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:40 下午
 */
public final class OperatorFactory {
    private static Map<String, String> mapping = new HashMap<>();
    private OperatorFactory instance = new OperatorFactory();

    private OperatorFactory() {
    }

    public static void initMapping(String configName) throws ParserConfigurationException, IOException, SAXException {
        InputStream configStream = OperatorFactory.class.getClassLoader().getResourceAsStream(configName);
        assert configStream != null;
        Document configFile = DocumentBuilderFactory.newInstance()
                .newDocumentBuilder()
                .parse(configStream);

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
        if (mapping.isEmpty()) {
            throw new Exception("未初始化OperatorFactory");
        }
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
        if (mapping.isEmpty()) {
            throw new Exception("未初始化OperatorFactory");
        }
        String templatePath = OperatorFactory.getTemplate(ability);
        return createOperatorFromFile(templatePath);
    }

    /**
     * 从给定配置文件中读取并创建一个Operator
     *
     * @param templatePath 配置文件的资源路径（相对resource目录的路径）
     * @return 创建完成的Operator
     * @throws Exception 配置文件不存在、XML解析错误
     */
    public static Operator createOperatorFromFile(String templatePath) throws Exception {
        InputStream fullConfigFileStream = OperatorFactory.class.getClassLoader().getResourceAsStream(templatePath);
        assert fullConfigFileStream != null;
        Document operatorConfig = DocumentBuilderFactory.newInstance()
                .newDocumentBuilder()
                .parse(fullConfigFileStream);
        operatorConfig.getDocumentElement().normalize();

        Element root = operatorConfig.getDocumentElement();

        // 1. 先载入Opt的基本信息，如ID、name、kind
        String code = String.valueOf(new Date().hashCode());
        Operator operator = new Operator(root.getAttribute("ID") + code,
                root.getAttribute("name"),
                root.getAttribute("kind"));

        // 2. 加载输入参数列表
        List<Param> parameters = getParams(root, "parameters", "parameter");
        for (Param param : parameters) {
            operator.addParameter(param);
        }


        // 3. 加载输入数据列表
        List<Param> inputs = getParams(root, "inputs", "input");
        for (Param input : inputs) {
            operator.addInputData(input);
        }


        // 4. 加载输出数据列表
        List<Param> outputs = getParams(root, "outputs", "output");
        for (Param output : outputs) {
            operator.addOutputData(output);
        }


        // 5. 加载每个平台配置文件的信息
        List<OperatorEntity> platformEntity = getOptPlatforms(root);
        for (OperatorEntity entity : platformEntity) {
            operator.addOperatorEntity(entity);
        }

        return operator;
    }

    private static List<Param> getParams(Element root, String tag, String childTag) {
        ArrayList<Element> eleList = getElementListByTag(Optional.of(root), tag, childTag);
        List<Param> result = new ArrayList<>();
        for (Element ele : eleList) {
            String name = ele.getAttribute("name");
            String dataType = ele.getAttribute("data_type");
            String defaultValue = ele.getAttribute("default");
            boolean isRequired = ele.getAttribute("is_required").equals("true");
            Param param = new Param(name, dataType, isRequired, defaultValue.isEmpty() ? null : defaultValue);
            result.add(param);
        }
        return result;
    }

    private static List<OperatorEntity> getOptPlatforms(Element root) throws Exception {
        List<OperatorEntity> operatorEntities = new ArrayList<>();
        ArrayList<Element> eleList = getElementListByTag(Optional.ofNullable(root), "platforms", "platform");
        for (Element ele : eleList) {
            //String platform = ele.getAttribute("ID");
            String path = getElementContentByTag(Optional.of(ele), "path");
            InputStream pltConfigStream = OperatorFactory.class.getClassLoader().getResourceAsStream(path);
            assert pltConfigStream != null;
            Document config = DocumentBuilderFactory.newInstance()
                    .newDocumentBuilder()
                    .parse(pltConfigStream);
            config.getDocumentElement().normalize();
            Element pltRoot = config.getDocumentElement();
            Optional<Element> pltEle = getElementByTag(pltRoot, "platform");
            String platform = getElementByTag(pltRoot, "platform").get().getAttribute("ID");
            String language = getElementContentByTag(pltEle, "language");
            Double cost = Double.valueOf(getElementContentByTag(pltEle, "cost"));
            List<Param> pltParams = getParams(pltRoot, "parameters", "parameter");

            // String id = pltEle.map(element -> element.getAttribute("ID")).orElse(""); // 用根XML里得到的ID
            operatorEntities.add(new OperatorEntity(platform, language, cost, pltParams));
        }
        return operatorEntities;
    }

    /**
     * 获取node的tag节点，为了防止空指针
     *
     * @param node node
     * @param tag  标签
     * @return Optional对象
     */
    private static Optional<Element> getElementByTag(Element node, String tag) {
        if (node == null) {
            return Optional.empty();
        }
        return Optional.ofNullable((Element) node.getElementsByTagName(tag).item(0));
    }

    /**
     * 获取node对应的内容，默认设为空串
     *
     * @param node 节点
     * @param tag  标签
     * @return Optional对象
     */
    @SuppressWarnings("all")
    private static String getElementContentByTag(Optional<Element> node, String tag) {
        return node.map(element -> element.getElementsByTagName(tag))
                .map(element -> (Element) element.item(0))
                .map(Element::getTextContent)
                .orElse("");
    }

    /**
     * 获取node的tag下所有childTag，并返回Element的list，默认返回空列表
     *
     * @param node     节点
     * @param tag      标签
     * @param childTag 子标签
     * @return 子标签Element列表
     */
    @SuppressWarnings("all")
    private static ArrayList<Element> getElementListByTag(Optional<Element> node, String tag, String childTag) {
        return node.map(element -> element.getElementsByTagName(tag))
                .map(element -> (Element) element.item(0))
                .map(element -> element.getElementsByTagName(childTag))
                .map(nodeList -> {
                    ArrayList<Element> elements = new ArrayList<>();
                    for (int i = 0; i < nodeList.getLength(); i++) {
                        elements.add((Element) nodeList.item(i));
                    }
                    return elements;
                })
                .orElse(new ArrayList<>());
    }


    public OperatorFactory getInstance() {
        if (mapping.isEmpty()) { // 防止没初始化（或配置文件为空）就拿到对象
            return null;
        }
        return instance;
    }

}
