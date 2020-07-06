/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:40 下午
 */
package basic;

import basic.operators.Operator;
import channel.Channel;

import java.util.LinkedList;
import java.util.Queue;

/**
 * 用于遍历plan的，提供DFS、BFS TODO:以后改成抽象基类，再在此基础上实现DFSTraversal, BFSTraversal , UDFTraversal等等
 */
public class PlanTraversal {
    private final int traverseType;

    //private Queue<Operator> bfs_queue_downstream = new LinkedList<>();
    private Operator root;
    // private Queue<Operator> bfs_queue_downstream = new PriorityQueue<>();
    private Queue<Operator> bfsQueueDownstream = new LinkedList<>();

    public PlanTraversal(Operator root, int traverseType) {
        this.traverseType = traverseType;
        this.root = root;
        bfsQueueDownstream.add(root);
        // bfs_queue_upstream.add(root);
    }

    public Operator nextOpt() {
        if (!this.bfsQueueDownstream.isEmpty()) {
            // 先拿到队列头部元素 用于返回
            Operator currentOpt = bfsQueueDownstream.poll();
            // 是单纯的拿到opt还是需要将子节点加入队列
            // 再 "顺便" 将子节点加入队列
            for (Channel channel : currentOpt.getOutputChannel()) {
                this.bfsQueueDownstream.add(channel.getTargetOperator());
            }
            // this.bfs_queue_downstream.addAll(currentOpt.getOutgoing_opt());
            return currentOpt;
        }
        return null;
    }

    public int addSuccessor(Operator operator) {
        int numSuccessor = 0;
        for (Channel channel : operator.getOutputChannel()) {
            this.bfsQueueDownstream.add(channel.getTargetOperator());
            numSuccessor++;
        }
        return numSuccessor;
    }

    public boolean hasNextOpt() {
        return !this.bfsQueueDownstream.isEmpty();
    }

}
