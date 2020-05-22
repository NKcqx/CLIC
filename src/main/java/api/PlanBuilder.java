package api;

import basic.Operators.*;
import basic.Platform;
import platforms.Java.JavaPlatform;
import platforms.Spark.SparkPlatform;


import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class PlanBuilder {
    LinkedList<Operator> pipeline;

    LinkedList<ExecutableOperator> executionPlan;

    List<Platform> providePlatform;

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
        return this;
    }

    public PlanBuilder sort(String name){
        this.pipeline.add(new SortOperator(name));

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
        // Optimize
        long startTime = System.currentTimeMillis();
        this.logging("Start Optimize Plan...");
        // 算子融合调度
        this.logging("Start operator fusion and re-organize...");
        this.optimizePipeline();
        Thread.sleep(1000);
        this.logging(String.format("Optimize Plan took: %d ms", System.currentTimeMillis() - startTime));
        this.printPlan();
        // Mapping
        startTime = System.currentTimeMillis();
        this.logging("   ");

        this.logging("Start Mapping Plan to Execution Plan...");
        try {
            traversePlan(this.pipeline);
            this.logging(String.format("Mapping Plan took: %d ms", System.currentTimeMillis() - startTime));
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        this.logging("   ");

        this.logging("Execute Current execution plan : ");
        // Execute
        for(ExecutableOperator eopt : this.executionPlan){
            eopt.evaluate("input", "output");
        }

        this.logging("\ndone.");

    }

    /**
     * Walk through Plan, Mapping each Opt to the best(least cost) Executable Opt
     * @param plan Original plan that waiting to be mapped
     */
    private void traversePlan(LinkedList<Operator> plan) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, ClassNotFoundException, InterruptedException {
        while (!plan.isEmpty()){
            Operator opt = plan.poll(); // 当前的抽象Opt.

            // 遍历所有提供的平台（这里做了简化）并拿到各自平台对当前opt的Mapping
            Platform JavaPlatform = providePlatform.get(0);
            // 得到当前Operator的Java平台实现
            String className = JavaPlatform.mappingOperator(opt.getID());
            if (className == null)
                ; // 平台并不支持该Opt.  继续从下一个平台中找
            Class javaOptCls = Class.forName(className); // 反射
            // 确保平台mapping到了正确的Opt
            assert javaOptCls.equals(ExecutableOperator.class) : String.format("Java的Mapping未得到正确类型的返回值，需要 Executable, 得到 %s", javaOptCls.toString());
            // 使用newInstance创建Java的Operator
            ExecutableOperator javaOpt = (ExecutableOperator) javaOptCls.getConstructor(opt.getClass()).newInstance(opt);

            // 同理，得到Opt的Spark实现
            Platform SparkPlatform =  providePlatform.get(1);
            className = SparkPlatform.mappingOperator(opt.getID());
            if (className != null)
                ; // 平台并不支持该Opt.  继续从下一个平台中找
            Class sparkOptCls = Class.forName(className);

            assert sparkOptCls.equals(ExecutableOperator.class) : String.format("Spark的Mapping未得到正确类型的返回值，需要 Executable, 得到 %s", javaOptCls.toString());
            // 根据得到的Class反向生成对应的Opt（好处是能由客户指定构造参数）
            ExecutableOperator sparkOpt = (ExecutableOperator) sparkOptCls
                    .getConstructor(opt.getClass())
                    .newInstance(opt);

            // 比较两个Opt的性能，选消耗最小的（做了简化，只比较Cost）
            ExecutableOperator bestOpt = javaOpt.getCost() < sparkOpt.getCost() ? javaOpt : sparkOpt;
            this.executionPlan.add(bestOpt); //将最佳opt加入executionPlan
            this.logging(String.format("Current Operator: %s supported by: \n" +
                    "    %s[Cost=%f], %s[Cost=%f]",
                    opt.getID(),
                    javaOpt.getClass().getSimpleName(), javaOpt.getCost(),
                    sparkOpt.getClass().getSimpleName(), sparkOpt.getCost()));
            this.logging(String.format("> Pick ** %s ** as best Operator", bestOpt.getClass().getSimpleName()));
            Thread.sleep(1500);
        }

    }

    private LinkedList<Operator> optimizePipeline(){
        this.logging("Re-Organizing Pipeline & Operator fusion ...");
        return this.pipeline;
    }

    private void logging(String s){
        System.out.println(s);
    }

    private void printPlan(){
        this.logging("Current Plan:");
        for (Operator opt : this.pipeline){
            this.logging("->    " + opt.getID());
        }
    }


}
