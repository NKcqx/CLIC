package api;

import basic.Operators.*;
import basic.Platform;
import basic.Visitors.ExecuteVisitor;
import basic.Visitors.ExecutionGenerationVisitor;
import basic.Visitors.PrintVisitor;
import platforms.Java.JavaPlatform;
import platforms.Spark.SparkPlatform;


import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class PlanBuilder {
    private LinkedList<Operator> pipeline; DAG not linear; Dataflow;

    private LinkedList<ExecutableOperator> executionPlan;

    private List<Platform> providePlatform;

    public PlanBuilder(){
        pipeline = new LinkedList<>();
        executionPlan = new LinkedList<>();
        providePlatform = new ArrayList<>();
        // 本应由用户指定需要用的platform，这里做了简化
        providePlatform.add(new JavaPlatform());
        providePlatform.add(new SparkPlatform());
    }


    /**
     * 将API的map接口转为构建Plan时需要的Operator
     * @param udf 实际应该是Function 而不是String，代表map中要执行的操作
     * @param name 本次map操作的名字
     * @return PlanBuilder，这样就可以用pipeline的形式使用API了
     */
    public PlanBuilder map(Supplier udf, String name){
        this.pipeline.add(new MapOperator(udf, name));
        // 找所有的 mapXML Operator -> XML ->mapOperator
        return this;
    }

    public PlanBuilder sort(String name){
        this.pipeline.add(new SortOperator(name));

        return this;
    }

    public PlanBuilder filter(Predicate predicate, String name){
        this.pipeline.add(new FilterOperator(predicate, name));

        return this;
    }

    public PlanBuilder collect(){
        this.pipeline.add(new CollectOperator("CollectOperator"));

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

        // Optimize
        long startTime = System.currentTimeMillis();
        this.logging("===========【Stage 2】Optimizing Plan ===========");
        // 算子融合调度
        this.logging("Start operator fusion and re-organize...");
        this.optimizePipeline();
        Thread.sleep(1000);
        this.logging(String.format("Optimize Plan took: %d ms", System.currentTimeMillis() - startTime));
        this.printPlan(this.pipeline);
        this.logging("   ");

        // Mapping
        startTime = System.currentTimeMillis();
        this.logging("=========== 【Stage 3】 Mapping Plan to Execution Plan ===========");
        this.traversePlan();
        this.logging(String.format("Mapping Plan took: %d ms", System.currentTimeMillis() - startTime));
        this.logging("   ");

        this.logging("=========== 【Stage 4】Executing Current execution plan ===========");
        this.executePlan();

        this.logging("\ndone.");
    }

    public void printPlan(LinkedList<? extends Visitable> plan){
        this.logging("Current Plan:");
        PrintVisitor printVisitor = new PrintVisitor();
        for (Visitable v : plan){
            v.acceptVisitor(printVisitor);
        }
    }

    private void traversePlan(){
        ExecutionGenerationVisitor executionGenerationVisitor = new ExecutionGenerationVisitor("java,spark");
        for (Visitable opt : this.pipeline){
            opt.acceptVisitor(executionGenerationVisitor);
        }
        this.executionPlan = executionGenerationVisitor.getExecutionPlan();
        this.printPlan(this.executionPlan);
    }

    private void executePlan(){
        ExecuteVisitor executeVisitor = new ExecuteVisitor();
        for (ExecutableOperator eopt : this.executionPlan){
            eopt.acceptVisitor(executeVisitor);
        }
    }

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
