
package basic.visitors;

import basic.operators.Operator;
import basic.operators.OperatorEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 1. 调出Opt的配置文件，从所有implement中选择最优的
 * 2. 管理Execution Plan的处理方式，如输出的格式、文件类型、文件路径等
 * 3. 管理Execution Opt.的输入输出
 *
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:40 下午
 */
public class ExecutionGenerationVisitor extends Visitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionGenerationVisitor.class);
    private List<Operator> visited = new ArrayList<>();

    public ExecutionGenerationVisitor() {
        super();
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
            if (opt.getSelectedEntities() == null) {
                // 拿到所有的entities并遍历找到cost最小的
                OperatorEntity bestOperatorEntity = Collections.min(
                        opt.getEntities().values(), Comparator.comparing(OperatorEntity::getCost));
                try {
                    // 为opt选择最佳的entity
                    opt.selectEntity(bestOperatorEntity.getEntityID());
                    this.logging(String.format("> Pick %s 's `%s[%f]` implement as best Operator\n",
                            opt.getOperatorID(),
                            opt.getSelectedEntities().getEntityID(),
                            opt.getSelectedEntities().getCost()));

                } catch (FileNotFoundException e) {
                    // 即使出了问题也不要来这找...这只是调用对象内部的ID，错也是别人往里传错了
                    e.printStackTrace();
                }
            }
        }

    }

    private boolean isVisited(Operator opt) {
        return this.visited.contains(opt);
    }

    private void logging(String s) {
        LOGGER.info(s);
    }
}
