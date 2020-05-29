package basic.Operators;


import basic.Visitors.Visitor;

import java.util.*;

import channel.InputChannel;
import channel.OutputChannel;
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

    private  Map<String, String> plt_mapping = new HashMap<>();


    // 定义输入数据和输出数据
    private InputChannel inputChannel = null;
    private OutputChannel outputChannel = null;


    // 记录下一跳Opt.
    private ArrayList<Operator> outgoing_opt = new ArrayList<>();
    private ArrayList<Operator> incoming_opt = new ArrayList<>();
    // protected Collection<Operator> successors;
    public enum OperatorKind{
        CALCULATOR, SUPPLIER, CONSUMER, TRANSFORMER, SHUFFLER
    }

    public Operator(){}

    public Operator(String config_file_path) throws IOException, SAXException, ParserConfigurationException {

        //暂时使用相对路径
        this.config_file_path = System.getProperty("user.dir")+config_file_path;
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        this.config = builder.parse(new File(this.config_file_path));
        this.config.getDocumentElement().normalize();
        // 1. 先载入Opt的基本信息，如ID、name、kind
        this.loadBasicInfo();
        // 2. 再找到opt所有平台实现的配置文件的路径
        this.loadOperatorConfs();
        // 3. 加载每个平台配置文件的信息
        this.getPlatformOptConf();
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
    }

    /**
     * 加载在该算子类型下所包含的所有平台的路径
     */
    public void loadOperatorConfs() {
        // 再依次载入所有的平台实现（XML）
        Element root = this.config.getDocumentElement();
        Node platforms_root_node = root.getElementsByTagName("platforms").item(0);
        if (platforms_root_node.getNodeType() == Node.ELEMENT_NODE){
            Element platform_root_ele = (Element) platforms_root_node;
            NodeList platforms = platform_root_ele.getElementsByTagName("platform");
            // 逐个遍历所有Platform
            for (int i=0; i<platforms.getLength();i++){
                Node platform_node = platforms.item(i);
                if (platform_node.getNodeType() == Node.ELEMENT_NODE){
                    Element platform_ele = (Element) platform_node;
                    String platform=platform_ele.getAttribute("ID");
                    String path = platform_ele.getElementsByTagName("path").item(0).getTextContent();
                    //格式<platform，path>
                    this.plt_mapping.put(platform, path);
                }
            }
        }
    }
    /**
     * 根据不同平台的operator的xml路径，读取相应配置
     * @throws ParserConfigurationException
     * @throws IOException
     * @throws SAXException
     */
    public void getPlatformOptConf() throws ParserConfigurationException, IOException, SAXException {
        if(this.plt_mapping==null || this.plt_mapping.isEmpty()){
            throw new FileNotFoundException("该算子没有具体平台的实现");
        }
        for(String key : plt_mapping.keySet()){
            //相对路径
            String path =System.getProperty("user.dir")+ plt_mapping.get(key);

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
    private void loadImplements(Document config){
        Element root = config.getDocumentElement();
        Node platform_node = root.getElementsByTagName("platform").item(0);
        if(platform_node.getNodeType()==Node.ELEMENT_NODE){
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

    public String getConfig_file_path() {
        return config_file_path;
    }

    public void setConfig_file_path(String config_file_path) {
        this.config_file_path = config_file_path;
    }


    public void selectEntity(String entity_id) throws FileNotFoundException {
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
        this.selectEntity(entity_id);
    }

    /**
     * 设置输入数据路径
     * @param fileName
     */
    public void setInputChannel(String fileName) {
        this.inputChannel = new InputChannel(fileName);
    }

    /**
     * 设置输出数据路径
     * @param fileName
     */
    public void setOutputChannel(String fileName) {
        this.outputChannel = new OutputChannel(fileName);
    }

    // TODO: 这里传入 inputChannel,  outputChannel
    public void evaluate(){
        String inputPath = this.inputChannel.getFilePath();
        String outputPath = this.outputChannel.getFilePath();
        System.out.println("input_data_path: " + inputPath);
        System.out.println("output_data_path: " + outputPath);

        // inputChannel与outputChannel已传进此函数
        // 对xml里command格式的修改我不是很熟悉，因此麻烦翔哥了
        // PlanBuilder中的executePlan函数里的planTraversal暂没有建成一棵树
        // 因此打印出来的data_path只有一对

        this.logging(String.format("evaluate: \n img= %s\n command= %s",
                this.selected_entity.getImg_path(),
                this.selected_entity.getCommand()));
    }

//    TODO: 下面的execute比较重要，后期再完善,现在先拿evaluate代替
//    /**
//     * 执行Opt的img，为了实现泛化，这里不能假设已知运行Img的command需要的各项参数（否则即使只改参数名称都要重写代码）
//     * 有一个XMLCompleter的Visitor，把选择的最佳img填入XML中，然后再来一个execute img，
//     * @param inputChannel
//     * @param outputChannel
//     */
//    public void execute(String inputChannel, String outputChannel) throws IOException {
//        String command = this.selected_entity.getCommand(); // 要执行的命令（此时还未传入参数值）
//        Element root = config.getDocumentElement(); // XML文件的根节点
//        // 拿到所有params
//        Element execute_root_ele = (Element) root.getElementsByTagName("execute").item(0);
//        Element params_root_ele = (Element) execute_root_ele.getElementsByTagName("params").item(0);
//        NodeList params = params_root_ele.getElementsByTagName("param");
//        for (int i=0; i<params.getLength();i++){
//            Element param = (Element) params.item(i);
//            String param_id = param.getAttribute("ID");
//            String param_value = param.getTextContent();
//
//        }
//        // TODO: 以上的部分很可能拿到Visitor中去做，填写完command以后在这直接运行
//
//        if (this.execute_command != null && !this.execute_command.equals("")){
//            Runtime.getRuntime().exec(this.execute_command);
//        }else {
//            throw new NoSuchElementException("没有给Operator指定具体执行命令");
//        }
//
//    }

    public ArrayList<Operator> getOutgoing_opt() {
        return outgoing_opt;
    }

    /**
     * 即 outgoing_opt的setter，但多做了一步双向注册（下一跳opt也要把this注册为它的上一跳）
     * @param outgoing 下一跳Opt
     * @return
     */
    public int connectTo(Operator outgoing){
        // 双向绑定
        this.outgoing_opt.add(outgoing);
        outgoing.incoming_opt.add(this);
        return this.outgoing_opt.size();
    }

    public ArrayList<Operator> getIncoming_opt() {
        return incoming_opt;
    }

    /**
     * 同connectTO
     * @param incoming 上一跳
     * @return
     */
    public int connectFrom(Operator incoming){
        this.incoming_opt.add(incoming);
        // incoming.connectTo(this);
        incoming.outgoing_opt.add(this);
        return this.incoming_opt.size();
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
                    ", cost=" + cost +
                    '}';
        }
    }
}
