package basic.Visitors;

import basic.Operators.ExecutableOperator;
import basic.Operators.Operator;
import basic.Platform;
import platforms.Java.JavaPlatform;
import platforms.Spark.SparkPlatform;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * 1. Opt. Mapping
 * 1. 管理Execution Plan的处理方式，如输出的格式、文件类型、文件路径等
 * 2. 管理Execution Opt.的输入输出
 *
 */

public class ExecutionGenerationVisitor implements Visitor {
    private List<Platform> supportedPlatforms = new ArrayList<>();
    private LinkedList<ExecutableOperator> executionPlan = new LinkedList<>();

    private static final Map<String, String> platformTable = new HashMap<String, String>() {
        {
            put("java", String.valueOf(JavaPlatform.class).substring(6));
            put("spark", String.valueOf(SparkPlatform.class).substring(6));

        }
    };

    public ExecutionGenerationVisitor(){
        this("java,spark");
    }

    public ExecutionGenerationVisitor(String pltfs) {
        String[] platforms = pltfs.toLowerCase().split(",");
        for (String pltf : platforms){
            String className = platformTable.getOrDefault(pltf, null);
            assert className != null && !className.equals("") : String.format("暂不支持平台: %s", pltf);

            try {
                Class javaOptCls = Class.forName(className);
                Platform platform = (Platform) javaOptCls.getConstructor().newInstance();
                this.supportedPlatforms.add(platform);
            } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void visit(Operator opt) {
        List<ExecutableOperator> eopts = new ArrayList<>();
        for (Platform platform : this.supportedPlatforms){
            String className = platform.mappingOperator(opt.getID());
            if (className == null)
                ; // 平台并不支持该Opt.  继续从下一个平台中找
            Class optCls = null; // 反射

            try {
                optCls = Class.forName(className);
                // 确保平台mapping到了正确的Opt
                assert optCls.equals(ExecutableOperator.class) : String.format("Java的Mapping未得到正确类型的返回值，需要 Executable, 得到 %s", optCls.toString());
                // 使用newInstance创建Java的Operator
                ExecutableOperator eopt = (ExecutableOperator) optCls.getConstructor(opt.getClass()).newInstance(opt);
                eopts.add(eopt);
            } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }

        Double minCost = Double.POSITIVE_INFINITY;
        ExecutableOperator minOpt = null;
        for (ExecutableOperator eopt : eopts){
            if (eopt.getCost() < minCost){
                minCost = eopt.getCost();
                minOpt = eopt;
            }
        }
        this.executionPlan.add(minOpt);
    }

    public LinkedList<ExecutableOperator> getExecutionPlan(){
        return this.executionPlan;
    }
}
