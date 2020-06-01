package basic.Operators;


import basic.Visitors.Visitor;

import java.util.*;

import channel.Channel;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.*;
import java.io.*;

public class Operator implements Visitable {
    private Document config = null;
    private String ID;
    private String name;
    private OperatorKind kind;
    private Map<String, OperatorEntity> entities = new HashMap<>();
    private OperatorEntity selected_entity = null;
    private String execute_command = null;

    private  Map<String, String> plt_mapping = new HashMap<>();

    private String the_data; // 临时的，代表当前Opt的计算结果，想办法赋予个unique的值
    // 记录下一跳Opt.
    private List<Channel> output_channels; // 这里Channel的index应该没什么用
    private List<String> result_list; // 有一个result就得有一个output channel，两个变量的index要（隐性）同步
    private List<Channel> input_channels;

    public enum OperatorKind{
        CALCULATOR, SUPPLIER, CONSUMER, TRANSFORMER, SHUFFLER
    }

    public Operator(String config_file_path) throws IOException, SAXException, ParserConfigurationException {
        //暂时使用相对路径
        String full_config_file_path = System.getProperty("user.dir")+config_file_path;
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        this.config = builder.parse(new File(full_config_file_path));
        this.config.getDocumentElement().normalize();

        // 现在拿到index的方式还比较草率（.size()），所以不敢随便初始化大小
//        this.result_list = Arrays.asList(new String[10]);
//        this.output_channel = Arrays.asList(new Channel[10]);
//        this.input_channel = Arrays.asList(new Channel[10]);
        // TODO: 也得有对应的input_list
        this.result_list = new ArrayList<>();
        this.output_channels = new ArrayList<>();
        this.input_channels = new ArrayList<>();

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

    // TODO: 这里使用 inputChannel,  outputChannel
    // TODO: 同时！！ 把输出结果（可以暂时是一个能表示每个instance的string）放到outputChannel里
    public void evaluate(int input_channel_index){

        // String one_of_data = this.input_channels.get(input_channel_index).getData();
        // TODO: 这不对，有多个输入时，每次走到这个节点都会拿一次所有数据！！应该是没拿到所有数据的时候就等待！
//        List<String> datas = new ArrayList<>();
//        for (Channel input_channel : this.input_channels){
//            datas.add(input_channel.getData());
//        }
        if (this.kind == OperatorKind.SUPPLIER){
            // Supplier 无需输入数据
            this.logging("input data: {SUPPLIER doesn't need input data.}");
        }else{
            // 拿到输入数据
            Channel inputChannel = this.input_channels.get(input_channel_index);
            String data = inputChannel.getData();

            this.logging("input data: {" + data+"}");
        }
        // 做计算
        this.logging("evaluate: {" + this.getID()+"}");
        // 把结果放到所有的槽里
        for (int i=0;i<output_channels.size();i++){
            this.result_list.add(this.the_data);
        }

        // 然后把所有输出 自己按顺序放到对应的result_list里
//        this.logging(String.format("evaluate: \n inputs: %s, outputs:%s",
//                this.inputChannel.getAllDatas().toString(),
//                this.outputChannel.getAllDatas().toString()));
    }

    public String getData(int index){
        // 最前面注释里说过，channel的index和result_list的index是隐性同步的, TODO: 做成pair？
        return this.result_list.get(index);
    }

    public List<String> getAllData(){
        return this.result_list;
    }

    private void resizeResultList(int new_size){
        List<String> new_array = Arrays.asList(new String[new_size]);
        new_array.addAll(this.result_list);
        this.result_list = new_array;
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


    public List<Channel> getOutput_channel() {
        return output_channels;
    }

    public List<Channel> getInput_channel() {
        return input_channels;
    }

    /**
     * 即 outgoing_opt的setter，但多做了一步双向注册（下一跳opt也要把this注册为它的上一跳）
     * @param outgoing 下一跳opt
     * @param input_index 下一跳的input_index
     * @return 本次Channel的index
     */
    public int connectTo(Operator outgoing, int input_index){
        // 拿到下一个放数据的槽的index
        int output_index = this.output_channels.size();
        // 创建边 TODO: 这一步不知道在哪做合适
        Channel channel = new Channel(this, output_index, outgoing, input_index);
        // 双向绑定
        this.output_channels.add(channel);
        outgoing.input_channels.add(channel);
        return this.output_channels.size();
    }

    /**
     * 同connectTO
     * @param incoming 上一跳opt
     * @param output_index 上一跳的output_index
     * @return 本次Channel的index
     */
    public int connectFrom(Operator incoming, int output_index){
        // 拿到下一个放数据的槽的index
        int input_index = this.input_channels.size();
        // 创建边 TODO: 这一步不知道在哪做合适
        Channel channel = new Channel(incoming, output_index, this, input_index);
        // 双向绑定
        this.input_channels.add(channel);
        incoming.output_channels.add(channel);
        return this.input_channels.size();
    }

    public boolean isLoaded(){
        return !(this.config == null); // 用这个判断可能不太好，也许可以试试判断有没有configFIle
    }

    public int getNextOutputIndex(){
        return this.output_channels.size();
    }

    public int getNextInputIndex(){
        return this.input_channels.size();
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
