package basic.platforms;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.inject.Singleton;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * 加载平台的xml配置到内存中
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/7/8 4:40 PM
 */
@Singleton
public class PlatformFactory {

    // 存储平台及对应的配置Map
    private static Map<String, Map<String, Object>> platformConfigMap = new HashMap<>();

    /**
     * 初始化配置到内存中
     *
     * @param configPath 配置路径
     * @throws ParserConfigurationException ParserConfigurationException
     * @throws IOException                  IOException
     * @throws SAXException                 SAXException
     */
    public static void initMapping(String configPath) throws ParserConfigurationException,
            IOException, SAXException {
        InputStream configStream = PlatformFactory.class.getClassLoader().getResourceAsStream(configPath);
        assert configStream != null;
        Document configFile = DocumentBuilderFactory.newInstance()
                .newDocumentBuilder()
                .parse(configStream);

        configFile.getDocumentElement().normalize();

        Element root = configFile.getDocumentElement();
        NodeList pairs = root.getElementsByTagName("pair");

        for (int i = 0; i < pairs.getLength(); i++) {
            Node pairNode = pairs.item(i);
            if (pairNode.getNodeType() == Node.ELEMENT_NODE) {
                Map<String, Object> pairConfigs = getPlatformConfigByElement((Element) pairNode);
                platformConfigMap.put((String) pairConfigs.get("platformName"), pairConfigs);
            }
        }
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

    /**
     * 获取pair中的各个信息
     *
     * @param node xml的node
     * @return flat后的map
     */
    private static Map<String, Object> getPlatformConfigByElement(Element node) {
        String platformName = getElementByTag(node, "name").map(Element::getTextContent).orElse(null);
        Optional<Element> platformInfo = getElementByTag(node, "platform");
        String dockerImage = getElementContentByTag(platformInfo, "docker-image");
        Optional<Element> execution = getElementByTag(node, "execution");
        String environment = getElementContentByTag(execution, "environment");
        String executor = getElementContentByTag(execution, "executor");
        // args在传入参数时是有序的
        Map<String, String> argsVal = new LinkedHashMap<String, String>() {{
            for (Element arg : getElementListByTag(execution, "args", "arg")) {
                if (arg.getAttribute("with-name").equals("false")) {
                    put(arg.getAttribute("name"), arg.getTextContent());
                } else {
                    put(arg.getAttribute("name"),
                            arg.getAttribute("name") + "=" + arg.getTextContent());
                }
            }
        }};
        Map<String, String> properties = new HashMap<String, String>() {{
            for (Element property : getElementListByTag(platformInfo, "properties", "property")) {
                put(property.getAttribute("name"), property.getTextContent());
            }
        }};

        return new HashMap<String, Object>() {{
            put("platformName", platformName);
            put("dockerImage", dockerImage);
            put("environment", environment);
            put("executor", executor);
            put("args", argsVal);
            put("properties", properties);
        }};
    }

    /**
     * 获取该平台对应的配置信息
     *
     * @param platform 平台名称
     * @return 将配置信息flat成一个map返回
     */
    public static Map<String, Object> getConfigByPlatformName(String platform) {
        return platformConfigMap.get(platform);
    }

    /**
     * 获取所有的配置的平台名称
     *
     * @return 平台名称的set
     */
    public static Set<String> getAllPlatform() {
        return platformConfigMap.keySet();
    }

    /**
     * 设置平台的参数的值
     *
     * @param platform 平台
     * @param arg      参数key
     * @param newVal   新的值
     */
    public static void setPlatformArgValue(String platform, String arg, String newVal) {
        if (platformConfigMap.containsKey(platform)) {
            // 使用新的值替换，为了不改变顺序，重新插入一遍
            Map<String, String> newArgMap = new LinkedHashMap<>();

            Map<String, Object> platformInfo = platformConfigMap.get(platform);
            @SuppressWarnings("unchecked")
            Map<String, String> argMap = (Map<String, String>) platformInfo.getOrDefault("args",
                    new LinkedHashMap<String, String>());
            argMap.forEach((key, value) -> {
                if (key.equals(arg)) {
                    // 说明包含key
                    if (value.startsWith(key)) {
                        newArgMap.put(key, key + "=" + newVal);
                    } else {
                        newArgMap.put(key, newVal);
                    }
                } else {
                    newArgMap.put(key, value);
                }
            });
            platformInfo.put("args", newArgMap);
            platformConfigMap.put(platform, platformInfo);
        }
    }

}
