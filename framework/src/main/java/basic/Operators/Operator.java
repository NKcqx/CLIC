package basic.Operators;


import basic.Param;
import basic.Visitors.Visitor;
import channel.Channel;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.util.*;

public class Operator implements Visitable {
    private Document config = null;
    private String config_file_path = null;
    private String ID;
    private String name;
    private OperatorKind kind;
    private Map<String, OperatorEntity> entities = new HashMap<>();
    private OperatorEntity selected_entity = null;

    private  Map<String, String> plt_mapping = new HashMap<>();

    private String the_data; // 临时的，代表当前Opt的计算结果，想办法赋予个unique的值
    private List<Channel> input_channels;
    private Map<String, Param> input_data_list;
    // 记录下一跳Opt.
    private List<Channel> output_channels; // 这里Channel的index应该没什么用
    private Map<String, Param> output_data_list; // 有一个result就得有一个output channel，两个变量的index要（隐性）同步


    public enum OperatorKind{
        CALCULATOR, SUPPLIER, CONSUMER, TRANSFORMER, SHUFFLER
    }

    public Operator(String config_file_path) throws IOException, SAXException, ParserConfigurationException {
        //暂时使用相对路径
        this.config_file_path = config_file_path;
        String full_config_file_path = System.getProperty("user.dir")+config_file_path;
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        this.config = builder.parse(new File(full_config_file_path));
        this.config.getDocumentElement().normalize();

        // 现在拿到index的方式还比较草率（.size()），所以不敢随便初始化大小
//        this.result_list = Arrays.asList(new String[10]);
//        this.output_channel = Arrays.asList(new Channel[10]);
//        this.input_channel = Arrays.asList(new Channel[10]);
        this.input_data_list = new HashMap<String, Param>();
        this.output_data_list = new HashMap<String, Param>();
        this.output_channels = new ArrayList<>();
        this.input_channels = new ArrayList<>();

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
    private void loadBasicInfo(){
        Element root = config.getDocumentElement();
        this.ID = root.getAttribute("ID");
        this.name = root.getAttribute("name");
        // Temp data, 下一版就删除
        this.the_data = "Compute Result: `" + this.ID + this.hashCode() + "`";
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
     * 加载输入输出参数列表
     */
    private void loadParams(){
        // 加载参数列表
        Element root = this.config.getDocumentElement();
        Node params_root_node = root.getElementsByTagName("parameters").item(0);
        if (params_root_node.getNodeType() == Node.ELEMENT_NODE){
            Element params_root_ele = (Element) params_root_node;
            NodeList params = params_root_ele.getElementsByTagName("parameter");
            for (int i=0;i<params.getLength();i++){
                Element param_ele =(Element) params.item(i);
                String kind = param_ele.getAttribute("kind");
                String name = param_ele.getAttribute("name");
                String data_type = param_ele.getAttribute("data_type");
                Param param = new Param(name, data_type);
                if (kind.equals("input")){
                    // 输入参数
                    this.input_data_list.put(name, param);
                }else{
                    this.output_data_list.put(name, param);
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

    public boolean evaluate(){
        // 1. 准备数据
        tempPrepareData();

        // 2. 检查是否获得了全部输入数据，是：继续执行； 否：return 特殊值
        for (Map.Entry entry : this.input_data_list.entrySet()){
            Param param = (Param) entry.getValue();
            if (!param.hasValue()){
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
    public void tempPrepareData(){
        for (Map.Entry entry : this.input_data_list.entrySet()){
            String key = (String) entry.getKey();
            this.setInputData(key, key + "'s temp value");
        }
    }

    public void tempDoEvaluate(){
        this.logging(this.getID() + " evaluate: {\n   inputs: ");
        for (String key:this.input_data_list.keySet()){
            this.logging("      "+key);
        }
        this.logging("   outputs:");
        for (String key:this.output_data_list.keySet()){
            this.logging("      "+key);
        }
        this.logging("}");
    }

    /**
     * 根据key获得该Opt的输出数据，输出列表里的值是文件路径在加载opt的时候就有了
     * @param key
     * @return
     */
    public String getOutputData(String key){
        Param output_data = this.output_data_list.get(key);
        return output_data.getData();
    }

    public List<String> getOutputData(List<String> keys){
        List<String> output_sublist = new ArrayList<>();
        for (String key:keys){
            output_sublist.add(this.output_data_list.get(key).getData());
        }
        return output_sublist;
    }

    public List<String> getAllOutputData(){
        List<String> output_list = new ArrayList<>();
        for (Map.Entry entry : this.output_data_list.entrySet()){
            Param param = (Param) entry.getValue();
            output_list.add(param.getData());
        }
        return output_list;
    }

    public Map<String, String> getAllKVOutputData(){
        Map<String, String> output_list = new HashMap<>();
        for (Map.Entry entry : this.output_data_list.entrySet()){
            Param param = (Param) entry.getValue();
            output_list.put((String)entry.getKey(), param.getData());
        }
        return output_list;
    }

    public Set<String> getInputKeys(){
        return this.input_data_list.keySet();
    }

    public Set<String> getOutputKeys(){
        return this.output_data_list.keySet();
    }

    public void setData(String key, String value){
        if(this.input_data_list.containsKey(key)){
            this.setInputData(key, value);
        }else if(this.output_data_list.containsKey(key)){
            this.setOutputData(key, value);
        }
        else{
            throw new NoSuchElementException(String.format("未在配置文件%s中找到指定的参数名：%s", this.config_file_path, key));
        }
    }

    private void setOutputData(String key, String value){
        this.output_data_list.get(key).setValue(value);

    }

    private void setInputData(String key, String value){
        // TODO：value的type要和Param里声明的type做类型检查
        this.input_data_list.get(key).setValue(value);

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


    public List<Channel> getOutputChannel() {
        return output_channels;
    }

    public List<Channel> getInputChannel() {
        return input_channels;
    }

    public Map<String,Param> getInput_data_list(){return this.input_data_list;}
    public Map<String,Param> getOutput_data_list(){return this.output_data_list;}

    /**
     * 即 outgoing_opt的setter
     * @param outgoing_channel 和下一跳相连的边
     * @return 本次Channel的index
     */
    public int connectTo(Channel outgoing_channel){
        // 拿到下一个放数据的槽的index
        this.output_channels.add(outgoing_channel);
        return this.output_channels.size();
    }


    /**
     * 同connectTO
     * @param incoming_channel 和上一跳相连的边(channel)
     * @return 本次Channel的index
     */
    public int connectFrom(Channel incoming_channel){
        // 拿到下一个放数据的槽的index
        this.input_channels.add(incoming_channel);
        return this.input_channels.size();
    }

    public boolean isLoaded(){
        return !(this.config == null); // 用这个判断可能不太好，也许可以试试判断有没有configFile
    }

    public int getNextOutputIndex(){
        return this.output_channels.size();
    }

    public int getNextInputIndex(){
        return this.input_channels.size();
    }

    public String getName(){return this.name;}

    public String getID(){return this.ID;}

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
