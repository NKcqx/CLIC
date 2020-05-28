package basic.Visitors;

import basic.Operators.Operator;
import org.xml.sax.SAXException;


import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * 1. 调出Opt的配置文件，从所有implement中选择最优的
 * 1. 管理Execution Plan的处理方式，如输出的格式、文件类型、文件路径等
 * 2. 管理Execution Opt.的输入输出
 *
 */
public class ExecutionGenerationVisitor implements Visitor {
    //private List<Platform> supportedPlatforms = new ArrayList<>();
    // private LinkedList<ExecutableOperator> executionPlan = new LinkedList<>();


    public ExecutionGenerationVisitor(){

    }

    /**
     * Walk through Plan, Mapping each Opt to the best(least cost) Executable Opt
     * @param opt Operator to be visited
     */
    @Override
    public void visit(Operator opt) {
        // 比较所有Entity，找到cost最小的
        if (!opt.isLoaded()){
            try {
                opt.loadConfiguration();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        Operator.OperatorEntity bestOperatorEntity = Collections.min(opt.getEntities().values(), new Comparator<Operator.OperatorEntity>() {
            @Override
            public int compare(Operator.OperatorEntity o1, Operator.OperatorEntity o2) {
                return o2.getCost().compareTo(o1.getCost());
            }
        });
        try {
            this.logging(String.format("\n > Pick `%s[%f]` as best Operator\n", bestOperatorEntity.getID(), bestOperatorEntity.getCost()));
            opt.select_entity(bestOperatorEntity.getID());
        } catch (FileNotFoundException e) {
            // 即使出了问题也不要来这找...这只是调用对象内部的ID，错也是别人往里传错了
            e.printStackTrace();
        }
//        this.executionPlan.add(minOpt); // 应该不需要execution plan了吧
    }

    private void logging(String s){
        System.out.print(s);
    }
}
