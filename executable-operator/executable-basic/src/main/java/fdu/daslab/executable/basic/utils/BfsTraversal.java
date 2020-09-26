package fdu.daslab.executable.basic.utils;

import fdu.daslab.executable.basic.model.Connection;
import fdu.daslab.executable.basic.model.OperatorBase;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * 拓扑遍历DAG
 *
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/8/19 10:26 下午
 */
public class BfsTraversal {
    Queue<OperatorBase> queue;

    public BfsTraversal(OperatorBase root) {
        // 从root开始遍历DAG，将所有入度为0的节点加入到队列中
        queue = new LinkedList<>();
        queue.add(root);
    }

    public OperatorBase nextOpt() {
        if(queue.isEmpty())
            return null;
        OperatorBase opt = queue.poll();
        List<Connection> allNextConnections = opt.getOutputConnections();
        for (Connection connection : allNextConnections) {
            OperatorBase sonOpt = connection.getTargetOpt();
            queue.add(sonOpt);
        }
        return opt;
    }

    public boolean hasNextOpt() {
        return !queue.isEmpty();
    }
}
