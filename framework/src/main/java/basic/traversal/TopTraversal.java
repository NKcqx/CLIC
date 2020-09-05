package basic.traversal;

import basic.operators.Operator;
import basic.visitors.InDegreeCntVisitor;
import channel.Channel;

import java.util.*;
import java.util.function.Predicate;

public class TopTraversal extends AbstractTraversal {
    private final Map<Operator, Integer> indegree;
    private final List<Operator> queue;

    public TopTraversal(Operator root) {
        super(root);
        indegree = new HashMap<>();
        AbstractTraversal traversal = new DfsTraversal(root);
        InDegreeCntVisitor visitor = new InDegreeCntVisitor(traversal, this.indegree);
        visitor.startVisit();

        queue = new ArrayList<>();

        for (Map.Entry<Operator, Integer> entry : indegree.entrySet()) {
            if (entry.getValue().equals(0)) {
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
            Operator res = queue.get(0);

            for (Channel channel : res.getOutputChannel()) {
                Operator target = channel.getTargetOperator();
                int cnt = indegree.get(target);
                indegree.put(target, --cnt);
                if (cnt == 0) {
                    queue.add(target);
                }
            }
            queue.remove(res);
            return res;
        }
        return null;
    }

    /**
     * 从所有的起点Opt中找符合条件的opt作为拓扑排序的起点
     *
     * @param predicate opt的过滤条件
     * @return 如果有满足条件的opt则返回该opt 否则 返回null;
     */
    public Operator nextOptWithFilter(Predicate<Operator> predicate) {
        if (this.hasNextOpt()) {
            Operator res = null;
            for (Operator operator : queue) {
                if (predicate.test(operator)) {
                    res = operator;
                    queue.remove(res);
                    break;
                }
            }
            if (res == null) {
                return null;
            }
            for (Channel channel : res.getOutputChannel()) {
                Operator target = channel.getTargetOperator();
                int cnt = indegree.get(target);
                indegree.put(target, --cnt);
                if (cnt == 0) {
                    queue.add(target);
                }
            }
            return res;
        } else {
            return null;
        }
    }


    @Override
    public int addSuccessor(Operator operator) {
        throw new RuntimeException("topsort not support this method");
    }
}
