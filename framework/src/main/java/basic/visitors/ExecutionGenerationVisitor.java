package basic.visitors;

import basic.operators.Operator;
import basic.traversal.AbstractTraversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 1. 调出Opt的配置文件，从所有implement中选择最优的
 * 1. 管理Execution Plan的处理方式，如输出的格式、文件类型、文件路径等
 * 2. 管理Execution Opt.的输入输出
 */
public class ExecutionGenerationVisitor extends Visitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionGenerationVisitor.class);
    private List<Operator> visited = new ArrayList<>();

    public ExecutionGenerationVisitor(AbstractTraversal planTraversal) {
        super(planTraversal);
    }


    /**
     * Walk through Plan, Mapping each Opt to the best(least cost) Executable Opt
     *
     * @param opt Operator to be visited
     */
    @Override
    public void visit(Operator opt) {
        if (!isVisited(opt)) {
            visited.add(opt);
            // 比较所有Entity，找到cost最小的
            if (!opt.isLoaded()) {
                try {
                    opt.getPlatformOptConf();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            // 拿到所有的entities并遍历找到cost最小的
            Operator.OperatorEntity bestOperatorEntity = Collections.min(
                    opt.getEntities().values(), new Comparator<Operator.OperatorEntity>() {
                @Override
                public int compare(Operator.OperatorEntity o1, Operator.OperatorEntity o2) {
                    return o1.getCost().compareTo(o2.getCost());
                }
            });
            try {
                this.logging(String.format("> Pick %s 's `%s[%f]` implement as best Operator\n",
                        opt.getId(), bestOperatorEntity.getID(), bestOperatorEntity.getCost()));
                // 为opt选择最佳的entity
                opt.selectEntity(bestOperatorEntity.getID());

            } catch (FileNotFoundException e) {
                // 即使出了问题也不要来这找...这只是调用对象内部的ID，错也是别人往里传错了
                e.printStackTrace();
            }
        }
        if (planTraversal.hasNextOpt()) {
            Operator nextOpt = planTraversal.nextOpt();
            planTraversal.addSuccessor(nextOpt);
            nextOpt.acceptVisitor(this);
        }
    }

    private boolean isVisited(Operator opt) {
        return this.visited.contains(opt);
    }

    private void logging(String s) {
        LOGGER.info(s);
    }
}
