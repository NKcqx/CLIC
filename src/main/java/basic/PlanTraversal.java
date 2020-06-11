package basic;

import basic.Operators.Operator;
import channel.Channel;

import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * 用于遍历plan的，提供DFS、BFS TODO:以后改成抽象基类，再在此基础上实现DFSTraversal, BFSTraversal , UDFTraversal等等
 */
public class PlanTraversal {
    private Operator root;

    //private Queue<Operator> bfs_queue_downstream = new LinkedList<>();

    private final int traverse_type;
    // private Queue<Operator> bfs_queue_downstream = new PriorityQueue<>();
    private Queue<Operator> bfs_queue_downstream = new LinkedList<>();

    public PlanTraversal(Operator root, int traverse_type){
        this.traverse_type = traverse_type;
        this.root = root;
        bfs_queue_downstream.add(root);
        // bfs_queue_upstream.add(root);
    }

    public Operator nextOpt(){
        if(!this.bfs_queue_downstream.isEmpty()){
            // 先拿到队列头部元素 用于返回
            Operator currentOpt = bfs_queue_downstream.poll();
            // 是单纯的拿到opt还是需要将子节点加入队列
                // 再 "顺便" 将子节点加入队列
            for (Channel channel:currentOpt.getOutputChannel()){
                this.bfs_queue_downstream.add(channel.getTargetOperator());
            }
            // this.bfs_queue_downstream.addAll(currentOpt.getOutgoing_opt());
            return currentOpt;
        }
        return null;
    }

    public int addSuccessor(Operator operator){
        int num_successor = 0;
        for (Channel channel : operator.getOutputChannel()){
            this.bfs_queue_downstream.add(channel.getTargetOperator());
            num_successor++;
        }
        return num_successor;
    }

    public boolean hasNextOpt(){
        return ! this.bfs_queue_downstream.isEmpty();
    }

}
