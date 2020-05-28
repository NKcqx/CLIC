package basic.Operators;


import basic.Visitors.Visitor;

import java.util.*;

import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.*;
import java.io.*;

public class Operator implements Visitable {
    private Document config = null;
    private String config_file_path = null;
    private String ID;
    private String name;
    private OperatorKind kind;
    private Map<String, OperatorEntity> entities = new HashMap<>();
    private OperatorEntity selected_entity = null;
    private String execute_command = null;
    // protected Collection<Operator> successors;
    public enum OperatorKind{
        CALCULATOR, SUPPLIER, CONSUMER, TRANSFORMER, SHUFFLER
    }

    public Operator(){}

    public Operator(String config_file_path){
        this.config_file_path = config_file_path;
    }

    public void loadConfiguration() throws ParserConfigurationException, IOException, SAXException {
        if(this.config_file_path != null){
            this.loadConfiguration(this.config_file_path);
        }else
            throw new FileNotFoundException("未指定配置文件路径");
    }

    /**
     * 读入XML文件并用 DOM Parser 解析，将结构保存为该Opt.的状态
     * @param config_file_path XML文件的路径
     * @throws ParserConfigurationException
     * @throws IOException
     * @throws SAXException
     */
    public void loadConfiguration(String config_file_path) throws ParserConfigurationException, IOException, SAXException {
        if(this.config == null){
            File configFile = new File(config_file_path);
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            this.config = builder.parse(configFile);
            this.config.getDocumentElement().normalize();
            this.loadBasicInfo();
        }else {
            logging("Already assigned a configuration.");
        }
    }

    /**
     * 装载基本状态，如ID、Name、各类Entity，装载后的Opt.仍是 `抽象`的
     */
    private void loadBasicInfo(){
        Element root = config.getDocumentElement();
        this.ID = root.getAttribute("ID");
        this.name = root.getAttribute("name");
        switch (root.getAttribute("kind")){
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

        this.loadImplements();
    }

    public String getConfig_file_path() {
        return config_file_path;
    }

    public void setConfig_file_path(String config_file_path) {
        this.config_file_path = config_file_path;
    }

    /**
     * 遍历所有Operator的实现，封装到 OperatorEntity 中
     * // @param root XML文件的根DOM对象（毕竟是private 无所谓参数类型，后期改也OK）
     */
    private void loadImplements(){
        Element root = config.getDocumentElement();
        Node platforms_root_node = root.getElementsByTagName("platforms").item(0);
        if (platforms_root_node.getNodeType() == Node.ELEMENT_NODE){
            Element platform_root_ele = (Element) platforms_root_node;
            NodeList platforms = platform_root_ele.getElementsByTagName("platform");
            // 逐个遍历所有Platform
            for (int i=0; i<platforms.getLength();i++){
                Node platform_node = platforms.item(i);
                // 我也不知道为啥 必须得转成Document才能访问这些
                if (platform_node.getNodeType() == Node.ELEMENT_NODE){
                    Element platform_ele = (Element) platform_node;
                    OperatorEntity platform = new OperatorEntity(
                            platform_ele.getAttribute("ID"),
                            platform_ele.getElementsByTagName("language").item(0).getTextContent(),
                            platform_ele.getElementsByTagName("implementation").item(0).getTextContent(),
                            platform_ele.getElementsByTagName("command").item(0).getTextContent(),
                            Double.valueOf(platform_ele.getElementsByTagName("cost").item(0).getTextContent())
                    );
                    this.entities.put(platform_ele.getAttribute("ID"), platform);
                }
            }
        }
    }

    public void select_entity(String entity_id) throws FileNotFoundException {
        if(this.entities.containsKey(entity_id)){
            this.selected_entity = this.entities.get(entity_id);
        }else
            throw new FileNotFoundException("未找到与 %s 匹配的实体，请使用配置文件中platform的ID属性");

    }

    public void withTargetPlatform(String entity_id) throws FileNotFoundException {
        if (this.selected_entity != null){
            // TODO: 已经选好了 还能变吗？
            ;
        }
        this.select_entity(entity_id);
    }

    /**
     * 执行Opt的img，为了实现泛化，这里不能假设已知运行Img的command需要的各项参数（否则即使只改参数名称都要重写代码）
     * 有一个XMLCompleter的Visitor，把选择的最佳img填入XML中，然后再来一个execute img，
     * @param inputChannel
     * @param outputChannel
     */
    public void execute(String inputChannel, String outputChannel) throws IOException {
        String command = this.selected_entity.getCommand(); // 要执行的命令（此时还未传入参数值）
        Element root = config.getDocumentElement(); // XML文件的根节点
        // 拿到所有params
        Element execute_root_ele = (Element) root.getElementsByTagName("execute").item(0);
        Element params_root_ele = (Element) execute_root_ele.getElementsByTagName("params").item(0);
        NodeList params = params_root_ele.getElementsByTagName("param");
        for (int i=0; i<params.getLength();i++){
            Element param = (Element) params.item(i);
            String param_id = param.getAttribute("ID");
            String param_value = param.getTextContent();

        }
        // TODO: 以上的部分很可能拿到Visitor中去做，填写完command以后在这直接运行

        if (this.execute_command != null && !this.execute_command.equals("")){
            Runtime.getRuntime().exec(this.execute_command);
        }else {
            throw new NoSuchElementException("没有给Operator指定具体执行命令");
        }

    }

    public boolean isLoaded(){
        return !(this.config == null); // 用这个判断可能不太好，也许可以试试判断有没有configFIle
    }

    public String getName(){return this.name;}

    public String getID(){return this.ID;}

    public String getExecute_command() {
        return execute_command;
    }

    public void setExecute_command(String execute_command) {
        this.execute_command = execute_command;
    }

    public Map<String, OperatorEntity> getEntities() {
        return entities;
    }

    private void logging(String s){
        System.out.println(s);
    }

    Integer estimateCost(){
        return 0;
    }

    @Override
    public void acceptVisitor(Visitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String toString() {
        return "Operator{" +
                "config=" + config +
                ", ID='" + ID + '\'' +
                ", name='" + name + '\'' +
                ", kind=" + kind +
                ", entities=" + entities +
                '}';
    }

    public class OperatorEntity{
        String ID;
        String language;
        String img_path;
        String command;
        Double cost;
        public OperatorEntity(){
            this("", "", "", "", 0.);
        }

        public OperatorEntity(String ID, String language, String img_path, String command, Double cost) {
            this.ID = ID;
            this.language = language;
            this.img_path = img_path;
            this.command = command;
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

        public String getImg_path() {
            return img_path;
        }

        public void setImg_path(String img_path) {
            this.img_path = img_path;
        }

        public String getCommand() {
            return command;
        }

        public void setCommand(String command) {
            this.command = command;
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
                    ", img_path='" + img_path + '\'' +
                    ", command='" + command + '\'' +
                    ", cost=" + cost +
                    '}';
        }
    }
}
