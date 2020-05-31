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

    //private final int traverse_type; // 0: BFS; 1: DFS TODO: 以后再支持DFS吧..
    //private Queue<Operator> bfs_queue_downstream = new LinkedList<>();

    private final int traverse_type;
    // private Queue<Operator> bfs_queue_downstream = new PriorityQueue<>();
    private Queue<Operator> bfs_queue_downstream = new LinkedList<>();
    //TODO: BFS 的时候会有opt被遍历多次，需要加入一个flag，该flag需要知道是否拿齐了所有的输入数据

    public PlanTraversal(Operator root, int traverse_type){
        this.traverse_type = traverse_type;
        this.root = root;
        bfs_queue_downstream.add(root);
        // bfs_queue_upstream.add(root);
    }

    /**
     * 将算子加进队列
     * @param root 算子
     */
    public void addOperator(Operator root) {
        this.root = root;
        bfs_queue_downstream.add(root);
    }

    // TODO: 这使用callback的形式传入Visitor 而不是Visitor里拿它去遍历（把遍历和执行逻辑完全分开，现在二者还缺个Container）
    public Operator nextOpt(){
        if(!this.bfs_queue_downstream.isEmpty()){
            Operator currentOpt = bfs_queue_downstream.poll();
            for (Channel channel:currentOpt.getOutput_channel()){
                this.bfs_queue_downstream.add(channel.getTargetOperator());
            }
            // this.bfs_queue_downstream.addAll(currentOpt.getOutgoing_opt());
            return currentOpt;
        }
        return null;
    }

    public boolean hasNextOpt(){
        return ! this.bfs_queue_downstream.isEmpty();
    }

}
