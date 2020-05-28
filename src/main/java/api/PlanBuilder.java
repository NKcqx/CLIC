package api;

import basic.Operators.*;
import basic.Visitors.ExecutionGenerationVisitor;
import basic.Visitors.PrintVisitor;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class PlanBuilder {
    private LinkedList<Operator> pipeline;

    // private LinkedList<ExecutableOperator> executionPlan;

    private String context;

    /**
     *
     * @param context 临时充当 OperatorMapping文件的路径
     * @throws IOException
     * @throws SAXException
     * @throws ParserConfigurationException
     */
    public PlanBuilder(String context ) throws IOException, SAXException, ParserConfigurationException {
        pipeline = new LinkedList<>();
        // executionPlan = new LinkedList<>();

        this.context = context;
        OperatorMapping.initMapping(context);

    }

    public PlanBuilder() throws ParserConfigurationException, SAXException, IOException {
        // TODO: 改一下默认文件路径的传输方式（用命令行传入）
        this("/Users/jason/Documents/Study_Study/DASLab/Cross_Platform_Compute/practice/IRdemo/resources/OperatorTemplates/OperatorMapping.xml");
    }


    /**
     * 将API的map接口转为构建Plan时需要的Operator
     * @param udf 实际应该是Function 而不是String，代表map中要执行的操作
     * @param name 本次map操作的名字
     * @return PlanBuilder，这样就可以用pipeline的形式使用API了
     */
    public PlanBuilder map(Supplier udf, String name){
        try {
            Operator opt = OperatorMapping.createOperator("map");
            this.pipeline.add(opt);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this;
    }

    public PlanBuilder sort(){
        try {
            Operator opt = OperatorMapping.createOperator("sort");
            this.pipeline.add(opt);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this;
    }

    public PlanBuilder filter(Predicate predicate, String name){
        try {
            Operator opt = OperatorMapping.createOperator("filter");
            this.pipeline.add(opt);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this;
    }

    public PlanBuilder collect(){
        try {
            Operator opt = OperatorMapping.createOperator("collect");
            this.pipeline.add(opt);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this;
    }

    /**
     * 1: Optimize the pipeline structure
     * 2: Mapping operator to Executable, Platform depended Operator
     * 3. Run
     */
    public void execute() throws InterruptedException {
        this.logging("===========【Stage 1】Get User Defined Plan ===========");
        this.printPlan(this.pipeline);
        this.logging("   ");

        this.logging("===========【Stage 2】Choose best Operator implementation ===========");
        this.optimizePlan();
        this.printPlan(this.pipeline);
//        // Optimize
//        long startTime = System.currentTimeMillis();
//        this.logging("===========【Stage 2】Optimizing Plan ===========");
//        // 算子融合调度
//        this.logging("Start operator fusion and re-organize...");
//        this.optimizePipeline();
//        Thread.sleep(1000);
//        this.logging(String.format("Optimize Plan took: %d ms", System.currentTimeMillis() - startTime));
//        this.printPlan(this.pipeline);
//        this.logging("   ");
//
//        // Mapping
//        startTime = System.currentTimeMillis();
//        this.logging("=========== 【Stage 3】 Mapping Plan to Execution Plan ===========");
//        this.traversePlan();
//        this.logging(String.format("Mapping Plan took: %d ms", System.currentTimeMillis() - startTime));
//        this.logging("   ");
//
//        this.logging("=========== 【Stage 4】Executing Current execution plan ===========");
//        this.executePlan();
//
//        this.logging("\ndone.");
    }

    public void printPlan(LinkedList<? extends Visitable> plan){
        this.logging("Current Plan:");
        PrintVisitor printVisitor = new PrintVisitor();
        for (Visitable v : plan){
            v.acceptVisitor(printVisitor);
        }
    }

    public void optimizePlan(){
        ExecutionGenerationVisitor executionGenerationVisitor = new ExecutionGenerationVisitor();
        for (Operator operator : this.pipeline){
            operator.acceptVisitor(executionGenerationVisitor);
        }
    }

//    private void traversePlan(){
//        ExecutionGenerationVisitor executionGenerationVisitor = new ExecutionGenerationVisitor("java,spark");
//        for (Visitable opt : this.pipeline){
//            opt.acceptVisitor(executionGenerationVisitor);
//        }
//        this.executionPlan = executionGenerationVisitor.getExecutionPlan();
//        this.printPlan(this.executionPlan);
//    }
//
//    private void executePlan(){
//        ExecuteVisitor executeVisitor = new ExecuteVisitor();
//        for (ExecutableOperator eopt : this.executionPlan){
//            eopt.acceptVisitor(executeVisitor);
//        }
//    }

    private LinkedList<Operator> optimizePipeline(){
        this.switchOperator(1, 2);
        return this.pipeline;
    }

    /**
     * 交换pipeline中任意两opt的位置，用于算子重组
     * @param idx1 第一个opt的idx
     * @param idx2 第二个opt的idx
     */
    private void switchOperator(int idx1, int idx2){
        this.logging(String.format("->    Switching Opt %s @%d with %s @%d",
                this.pipeline.get(idx1).getID(), idx1,
                this.pipeline.get(idx2).getID(), idx2
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

    private void logging(String s){
        System.out.println(s);
    }
}
