/**
 * @author 陈齐翔，杜清华
 * @since  2020/7/6 11:39
 * @version 1.0
 */
package basic.operators;


import basic.Param;
import basic.visitors.Visitor;
import channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class Operator implements Visitable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Operator.class);

    private Document config = null; // Operator的XML文件
    private String configFilePath = null; // Operator的XML文件路径
    private String id; // Operator ID
    private String name; // Operator Name
    private OperatorKind kind; // Operator Kind
    private Map<String, OperatorEntity> entities = new HashMap<>(); // Operator的所有实现
    private OperatorEntity selectedEntity = null; // 当前Operator选择的最优的平台实现

    private Map<String, String> pltMapping = new HashMap<>(); // platform mapping，用于找到该Operator所支持的所有平台
    private String theData; // 临时的，代表当前Opt的计算结果，想办法赋予个unique的值
    private List<Channel> inputChannels;
    private Map<String, Param> inputDataList;
    // 记录下一跳Opt.
    private List<Channel> outputChannels; // 这里Channel的index应该没什么用
    private Map<String, Param> outputDataList; // 有一个result就得有一个output channel，两个变量的index要（隐性）同步


    public Operator(String configFilePath) throws IOException, SAXException, ParserConfigurationException {
        //暂时使用相对路径
        this.configFilePath = configFilePath;
        String full_config_file_path = System.getProperty("user.dir") + configFilePath;
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        this.config = builder.parse(new File(full_config_file_path));
        this.config.getDocumentElement().normalize();

        // 现在拿到index的方式还比较草率（.size()），所以不敢随便初始化大小
//        this.result_list = Arrays.asList(new String[10]);
//        this.output_channel = Arrays.asList(new Channel[10]);
//        this.input_channel = Arrays.asList(new Channel[10]);
        this.inputDataList = new HashMap<String, Param>();
        this.outputDataList = new HashMap<String, Param>();
        this.outputChannels = new ArrayList<>();
        this.inputChannels = new ArrayList<>();

        // 1. 先载入Opt的基本信息，如ID、name、kind
        this.loadBasicInfo();
        // 2. 加载参数列表
        this.loadParams();
        // 3. 再找到opt所有平台实现的配置文件的路径
        this.loadOperatorConfs();
        // 4. 加载每个平台配置文件的信息
        this.getPlatformOptConf();
    }

    /**
     * 装载基本状态，如ID、Name、params, 各类Entity，装载后的Opt.仍是 `抽象`的
     */
    private void loadBasicInfo() {
        Element root = config.getDocumentElement();
        this.id = root.getAttribute("ID");
        this.name = root.getAttribute("name");
        // Temp data, 下一版就删除
        this.theData = "Compute Result: `" + this.id + this.hashCode() + "`";
        switch (root.getAttribute("kind")) {
            case "calculator":
                this.kind = OperatorKind.CALCULATOR;
                break;
            case "supplier":
                this.kind = OperatorKind.SUPPLIER;
                break;
            case "consumer":
                this.kind = OperatorKind.CONSUMER;
                break;
            case "transformer":
                this.kind = OperatorKind.TRANSFORMER;
                break;
            case "shuffler":
                this.kind = OperatorKind.SHUFFLER;
                break;
            default:
                this.kind = OperatorKind.CALCULATOR;
        }

    }

    /**
     * 加载输入输出参数列表
     */
    private void loadParams() {
        // 加载参数列表
        Element root = this.config.getDocumentElement();
        Node params_root_node = root.getElementsByTagName("parameters").item(0);
        if (params_root_node.getNodeType() == Node.ELEMENT_NODE) {
            Element params_root_ele = (Element) params_root_node;
            NodeList params = params_root_ele.getElementsByTagName("parameter");
            for (int i = 0; i < params.getLength(); i++) {
                Element param_ele = (Element) params.item(i);
                String kind = param_ele.getAttribute("kind");
                String name = param_ele.getAttribute("name");
                String data_type = param_ele.getAttribute("data_type");
                String default_value = param_ele.getAttribute("default");
                Boolean is_required = param_ele.getAttribute("is_required").equals("true"); // 没定义该属性时返回空字符串,于是默认为false
                Param param = new Param(name, data_type, is_required, default_value.isEmpty() ? null : default_value);
                if (kind.equals("input")) {
                    // 输入参数
                    this.inputDataList.put(name, param);
                } else {
                    this.outputDataList.put(name, param);
                }
            }
        }
    }

    /**
     * 加载在该算子类型下所包含的所有平台的路径
     */
    public void loadOperatorConfs() {
        // 再依次载入所有的平台实现（XML）
        Element root = this.config.getDocumentElement();
        Node platforms_root_node = root.getElementsByTagName("platforms").item(0);
        if (platforms_root_node.getNodeType() == Node.ELEMENT_NODE) {
            Element platform_root_ele = (Element) platforms_root_node;
            NodeList platforms = platform_root_ele.getElementsByTagName("platform");
            // 逐个遍历所有Platform
            for (int i = 0; i < platforms.getLength(); i++) {
                Node platform_node = platforms.item(i);
                if (platform_node.getNodeType() == Node.ELEMENT_NODE) {
                    Element platform_ele = (Element) platform_node;
                    String platform = platform_ele.getAttribute("ID");
                    String path = platform_ele.getElementsByTagName("path").item(0).getTextContent();
                    //格式<platform，path>
                    this.pltMapping.put(platform, path);
                }
            }
        }
    }

    /**
     * 根据不同平台的operator的xml路径，读取相应配置
     *
     * @throws ParserConfigurationException
     * @throws IOException
     * @throws SAXException
     */
    public void getPlatformOptConf() throws ParserConfigurationException, IOException, SAXException {
        if (this.pltMapping == null || this.pltMapping.isEmpty()) {
            throw new FileNotFoundException("该算子没有具体平台的实现");
        }
        for (String key : pltMapping.keySet()) {
            //相对路径
            String path = System.getProperty("user.dir") + pltMapping.get(key);

            File configFile = new File(path);
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document config = builder.parse(configFile);
            config.getDocumentElement().normalize();
            this.loadImplements(config);
        }
    }

    /**
     * 遍历具体平台的Operator的实现，封装到 OperatorEntity 中
     * // @param root XML文件的根DOM对象（毕竟是private 无所谓参数类型，后期改也OK）
     */
    private void loadImplements(Document config) {
        Element root = config.getDocumentElement();
        Node platform_node = root.getElementsByTagName("platform").item(0);
        if (platform_node.getNodeType() == Node.ELEMENT_NODE) {
            Element platform_ele = (Element) platform_node;
            OperatorEntity platform = new OperatorEntity(
                    platform_ele.getAttribute("ID"),
                    platform_ele.getElementsByTagName("language").item(0).getTextContent(),
                    Double.valueOf(platform_ele.getElementsByTagName("cost").item(0).getTextContent())
            );
            this.entities.put(platform_ele.getAttribute("ID"), platform);
        }
    }

    /**
     * 根据传入的entity_id为当前opt设置其对应的要运行的平台
     *
     * @param entity_id 特定平台的ID
     * @throws FileNotFoundException
     */
    public void selectEntity(String entity_id) throws FileNotFoundException {
        if (this.entities.containsKey(entity_id)) {
            this.selectedEntity = this.entities.get(entity_id);
        } else
            throw new FileNotFoundException("未找到与 %s 匹配的实体，请使用配置文件中platform的ID属性");

    }

    /**
     * 由用户直接为Opt指定具体计算平台，而不用系统择优选择
     *
     * @param entity_id
     * @throws FileNotFoundException
     */
    public void withTargetPlatform(String entity_id) throws FileNotFoundException {
        if (this.selectedEntity != null) {
            // TODO: 已经选好了 还能变吗？
            ;
        }
        this.selectEntity(entity_id);
    }

    public boolean evaluate() {
        // 1. 准备数据
        tempPrepareData();

        // 2. 检查是否获得了全部输入数据，是：继续执行； 否：return 特殊值
        for (Map.Entry entry : this.inputDataList.entrySet()) {
            Param param = (Param) entry.getValue();
            if (!param.hasValue()) {
                return false;
            }
        }
        // 已拿到所有输入数据, 开始"计算"
        this.tempDoEvaluate();
        return true;
    }

    /**
     * 准备输入数据，实际上这个很复杂，设计各类协议的各类参数，是需要在Opt的配置文件里指定的
     */
    public void tempPrepareData() {
        for (Map.Entry entry : this.inputDataList.entrySet()) {
            String key = (String) entry.getKey();
            this.setInputData(key, key + "'s temp value");
        }
    }

    public void tempDoEvaluate() {
        this.logging(this.getId() + " evaluate: {\n   inputs: ");
        for (String key : this.inputDataList.keySet()) {
            this.logging("      " + key);
        }
        this.logging("   outputs:");
        for (String key : this.outputDataList.keySet()) {
            this.logging("      " + key);
        }
        this.logging("}");
    }

    /**
     * 根据key获得该Opt的输出数据，输出列表里的值是文件路径在加载opt的时候就有了
     *
     * @param key 要获取的输出数据的Key
     * @return
     */
    public String getOutputData(String key) {
        Param output_data = this.outputDataList.get(key);
        return output_data.getData();
    }

    public List<String> getOutputData(List<String> keys) {
        List<String> output_sublist = new ArrayList<>();
        for (String key : keys) {
            output_sublist.add(this.outputDataList.get(key).getData());
        }
        return output_sublist;
    }

    public void setData(String key, String value) {
        if (this.inputDataList.containsKey(key)) {
            this.setInputData(key, value);
        } else if (this.outputDataList.containsKey(key)) {
            this.setOutputData(key, value);
        } else {
            throw new NoSuchElementException(String.format("未在配置文件%s中找到指定的参数名：%s", this.configFilePath, key));
        }
    }

//    public List<String> getAllOutputData() {
//        List<String> output_list = new ArrayList<>();
//        for (Map.Entry entry : this.output_data_list.entrySet()) {
//            Param param = (Param) entry.getValue();
//            output_list.add(param.getData());
//        }
//        return output_list;
//    }
//
//    public Map<String, String> getAllKVOutputData() {
//        Map<String, String> output_list = new HashMap<>();
//        for (Map.Entry entry : this.output_data_list.entrySet()) {
//            Param param = (Param) entry.getValue();
//            output_list.put((String) entry.getKey(), param.getData());
//        }
//        return output_list;
//    }
//
//    public Set<String> getInputKeys() {
//        return this.input_data_list.keySet();
//    }
//
//    public Set<String> getOutputKeys() {
//        return this.output_data_list.keySet();
//    }

    private void setOutputData(String key, String value) {
        this.outputDataList.get(key).setValue(value);

    }

    private void setInputData(String key, String value) {
        // TODO：value的type要和Param里声明的type做类型检查
        this.inputDataList.get(key).setValue(value);

    }

    public List<Channel> getOutputChannel() {
        return outputChannels;
    }

    public List<Channel> getInputChannel() {
        return inputChannels;
    }

    public Map<String, Param> getInputDataList() {
        return this.inputDataList;
    }

    public Map<String, Param> getOutputDataList() {
        return this.outputDataList;
    }

    /**
     * 即 outgoing_opt的setter
     *
     * @param outgoing_channel 和下一跳相连的边
     * @return 本次Channel的index
     */
    public int connectTo(Channel outgoing_channel) {
        // 拿到下一个放数据的槽的index
        this.outputChannels.add(outgoing_channel);
        return this.outputChannels.size();
    }

    /**
     * 同connectTO
     *
     * @param incoming_channel 和上一跳相连的边(channel)
     * @return 本次Channel的index
     */
    public int connectFrom(Channel incoming_channel) {
        // 拿到下一个放数据的槽的index
        this.inputChannels.add(incoming_channel);
        return this.inputChannels.size();
    }

    public boolean isLoaded() {
        return !(this.config == null); // 用这个判断可能不太好，也许可以试试判断有没有configFile
    }

    public int getNextOutputIndex() {
        return this.outputChannels.size();
    }

    public int getNextInputIndex() {
        return this.inputChannels.size();
    }

    public String getName() {
        return this.name;
    }

    public String getId() {
        return this.id;
    }

    public Map<String, OperatorEntity> getEntities() {
        return entities;
    }

    private void logging(String s) {
        LOGGER.info(s);
    }

    public OperatorEntity getSelectedEntities() {
        return selectedEntity;
    }

    @Override
    public void acceptVisitor(Visitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String toString() {
        return "Operator{" +
                "config=" + config +
                ", ID='" + id + '\'' +
                ", name='" + name + '\'' +
                ", kind=" + kind +
                ", entities=" + entities +
                '}';
    }

    public enum OperatorKind {
        CALCULATOR, SUPPLIER, CONSUMER, TRANSFORMER, SHUFFLER
    }

    public class OperatorEntity {
        String ID;
        String language;
        Double cost;

        public OperatorEntity() {
            this("", "", 0.);
        }

        public OperatorEntity(String ID, String language, Double cost) {
            this.ID = ID;
            this.language = language;
            this.cost = cost;
        }

        public String getID() {
            return ID;
        }

        public void setID(String ID) {
            this.ID = ID;
        }

        public String getLanguage() {
            return language;
        }

        public void setLanguage(String language) {
            this.language = language;
        }

        public Double getCost() {
            return cost;
        }

        public void setCost(Double cost) {
            this.cost = cost;
        }

        @Override
        public String toString() {
            return "OperatorEntity{" +
                    "language='" + language + '\'' +
                    ", cost=" + cost +
                    '}';
        }
    }
}
