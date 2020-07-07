package basic.traversal;

import basic.operators.Operator;
import channel.Channel;

import java.util.LinkedList;
import java.util.Queue;

public class BfsTraversal extends AbstractTraversal {
    private final Queue<Operator> queue;

    public BfsTraversal(Operator root) {
        super(root);
        queue = new LinkedList<>();
        queue.add(root);
    }

    @Override
    public boolean hasNextOpt() {
        return !this.queue.isEmpty();
    }

    @Override
    public Operator nextOpt() {
        if (this.hasNextOpt()) {
            Operator res = this.queue.poll();
            //todo we should put visited check in here,rather than in visitor
            for (Channel channel : res.getOutputChannel()) {
                this.queue.add(channel.getTargetOperator());
            }

            return res;
        } else {
            return null;
        }
    }

    @Override   //todo fix that BUG and remove this
    public int addSuccessor(Operator operator) {
        int numSuccessor = 0;
        for (Channel channel : operator.getOutputChannel()) {
            this.queue.add(channel.getTargetOperator());
            numSuccessor++;
        }
        return numSuccessor;
    }
}
