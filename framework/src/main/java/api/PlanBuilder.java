package api;

import adapters.ArgoAdapter;
import basic.Configuration;
import basic.operators.Operator;
import basic.operators.OperatorFactory;
import basic.traversal.AbstractTraversal;
import basic.traversal.BfsTraversal;
import basic.traversal.TopTraversal;
import basic.visitors.ExecutionGenerationVisitor;
import basic.visitors.PipelineVisitor;
import basic.visitors.PrintVisitor;
import fdu.daslab.backend.executor.model.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class PlanBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(PlanBuilder.class);
    private LinkedList<Operator> pipeline;
    private DataQuanta headDataQuanta = null; // 其实可以有多个head
    // 现在最简单粗暴的方法是将图存储在PlanBuilder中
    private List<DataQuanta> dataQuantaList = new ArrayList<>();
    private DataQuanta presentDataQuanta = null; // head永远是present的上一个节点

    private Configuration configuration;

    /**
     * @param configuration 配置文件，从中加载系统运行时必要的参数，即系统运行时的上下文
     * @throws IOException
     * @throws SAXException
     * @throws ParserConfigurationException
     */
    public PlanBuilder(Configuration configuration) throws IOException, SAXException, ParserConfigurationException {
        pipeline = new LinkedList<>();
        // executionPlan = new LinkedList<>();

        this.configuration = configuration;
        OperatorFactory.initMapping(configuration.getProperty("operator-mapping-file"));
    }

    public PlanBuilder() throws ParserConfigurationException, SAXException, IOException {
        this(new Configuration());
    }

    public DataQuanta readDataFrom(Map<String, String> params) throws Exception {
        DataQuanta dataQuanta = DataQuanta.createInstance("source", params);
        this.headDataQuanta = dataQuanta;
        return this.headDataQuanta; // 不需要connectTo
    }

    public DataQuanta getHeadDataQuanta() {
        return headDataQuanta;
    }

    public void setHeadDataQuanta(DataQuanta headDataQuanta) {
        this.headDataQuanta = headDataQuanta;
    }

    /**
     * 把 headOperator 交给各类Visitor
     * 1: Optimize the pipeline structure
     * 2: Mapping operator to Executable, Platform depended Operator
     * 3. Run
     */
    public void execute() throws InterruptedException {
        this.logging("===========【Stage 1】Get User Defined Plan ===========");
        this.printPlan();
        this.logging("   ");

        this.logging("===========【Stage 2】Choose best Operator implementation ===========");
        this.optimizePlan();
        this.printPlan();
        this.logging("   ");

        this.logging("===========【Stage 3】execute plan ==========");
        this.executePlan();

    }

    public void printPlan() {
        this.logging("Current Plan:");
        AbstractTraversal planTraversal = new TopTraversal(this.getHeadDataQuanta().getOperator());
        PrintVisitor printVisitor = new PrintVisitor(planTraversal);
        printVisitor.startVisit();
//        for (Visitable v : plan){
//            v.acceptVisitor(printVisitor);
//        }
    }

    public void optimizePlan() {
        AbstractTraversal planTraversal = new BfsTraversal(this.getHeadDataQuanta().getOperator());
        ExecutionGenerationVisitor executionGenerationVisitor = new ExecutionGenerationVisitor(planTraversal);
        executionGenerationVisitor.startVisit();
        // this.getHeadDataQuanta().getOperator().acceptVisitor(executionGenerationVisitor);
//        for (Operator operator : this.pipeline){
//            operator.acceptVisitor(executionGenerationVisitor);
//        }
    }

    private void executePlan() {
//        AbstractTraversal planTraversal = new BfsTraversal(this.getHeadDataQuanta().getOperator());
//        ExecuteVisitor executeVisitor = new ExecuteVisitor(planTraversal);
//        executeVisitor.startVisit();
        AbstractTraversal planTraversal = new BfsTraversal(this.getHeadDataQuanta().getOperator());
        PipelineVisitor executeVisitor = new PipelineVisitor(planTraversal);
        executeVisitor.startVisit();
        // 获取所有的operator
        List<Operator> allOperators = executeVisitor.getAllOperators();
        // 调用argo平台运行
        Pipeline argoPipeline = new Pipeline(new ArgoAdapter(), allOperators);
        argoPipeline.execute();

//        for (Operator opt : this.pipeline){
//            opt.acceptVisitor(executeVisitor);
//        }
    }

    private LinkedList<Operator> optimizePipeline() {
        this.switchOperator(1, 2);
        return this.pipeline;
    }

    /**
     * 交换pipeline中任意两opt的位置，用于算子重组
     *
     * @param idx1 第一个opt的idx
     * @param idx2 第二个opt的idx
     */
    private void switchOperator(int idx1, int idx2) {
        this.logging(String.format("->    Switching Opt %s @%d with %s @%d",
                this.pipeline.get(idx1).getId(), idx1,
                this.pipeline.get(idx2).getId(), idx2
        ));
        assert idx1 < this.pipeline.size() : "idx1是无效的索引";
        assert idx2 < this.pipeline.size() : "idx2是无效的索引";
        Operator opt1 = this.pipeline.get(idx1);
        Operator opt2 = this.pipeline.get(idx2);

        this.pipeline.add(idx1, opt2);
        this.pipeline.remove(idx1 + 1); // 删除原来的opt1

        this.pipeline.add(idx2, opt1);
        this.pipeline.remove(idx2 + 1);
    }

    private void logging(String s) {
        LOGGER.info(s);
    }
}
