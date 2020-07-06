package basic.visitors;

import basic.operators.Operator;
import basic.traversal.AbstractTraversal;
import channel.Channel;

import java.util.Map;

public class InDegreeCntVisitor extends Visitor {
    private Map<Operator, Integer> map;

    public InDegreeCntVisitor(AbstractTraversal abstractTraversal, Map<Operator, Integer> map) {
        super(abstractTraversal);
        this.map = map;
    }

    @Override
    public void visit(Operator opt) {

        map.putIfAbsent(opt, Integer.valueOf(0));
        for (Channel channel : opt.getOutputChannel()) {
            int before = map.getOrDefault(channel.getTargetOperator(), Integer.valueOf(0));
            map.put(channel.getTargetOperator(), before + 1);
        }

        if (planTraversal.hasNextOpt()) {
            planTraversal.nextOpt().acceptVisitor(this);
        }

    }
}
