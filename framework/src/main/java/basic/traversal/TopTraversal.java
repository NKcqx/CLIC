package basic.traversal;

import basic.operators.Operator;
import basic.visitors.InDegreeCntVisitor;
import channel.Channel;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class TopTraversal extends AbstractTraversal {
    private final Map<Operator, Integer> indegree;
    private final Queue<Operator> queue;

    public TopTraversal(Operator root) {
        super(root);
        indegree = new HashMap<>();
        AbstractTraversal traversal = new DfsTraversal(root);
        InDegreeCntVisitor visitor = new InDegreeCntVisitor(traversal, this.indegree);
        visitor.startVisit();

        queue = new LinkedList<>();

        for (Map.Entry<Operator, Integer> entry : indegree.entrySet()) {
            if (entry.getValue().equals(Integer.valueOf(0))) {
                this.queue.add(entry.getKey());
            }
        }
    }

    @Override
    public boolean hasNextOpt() {
        return !queue.isEmpty();
    }

    @Override
    public Operator nextOpt() {
        if (this.hasNextOpt()) {
            Operator res = queue.poll();

            for (Channel channel : res.getOutputChannel()) {
                Operator target = channel.getTargetOperator();
                int cnt = indegree.get(target);
                indegree.put(target, --cnt);
                if (cnt == 0) {
                    queue.add(target);
                }
            }

            return res;
        }
        return null;
    }

    @Override
    public int addSuccessor(Operator operator) {
        throw new RuntimeException("topsort not support this method");
    }
}
