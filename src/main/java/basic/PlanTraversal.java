package basic;

import basic.Operators.Operator;

import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * 用于遍历plan的，提供DFS、BFS TODO:以后改成抽象基类，再在此基础上实现DFSTraversal, BFSTraversal , UDFTraversal等等
 */
public class PlanTraversal {
    private Operator root;
    private final int traverse_type; // 0: BFS; 1: DFS TODO: 以后再支持DFS吧..
    private Queue<Operator> bfs_queue_downstream = new LinkedList<>();
    // private Queue<Operator> bfs_queue_upstream = new PriorityQueue<>();

    public PlanTraversal(Operator root, int traverse_type){
        this.traverse_type = traverse_type;
        this.root = root;
        bfs_queue_downstream.add(root);
        // bfs_queue_upstream.add(root);
    }

    public Operator nextOpt(){
        if(!this.bfs_queue_downstream.isEmpty()){
            Operator currentOpt = bfs_queue_downstream.poll();
            this.bfs_queue_downstream.addAll(currentOpt.getOutgoing_opt());
            return currentOpt;
        }
        return null;
    }

    public boolean hasNextOpt(){
        return ! this.bfs_queue_downstream.isEmpty();
    }

}
