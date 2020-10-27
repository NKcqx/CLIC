
package fdu.daslab.executable.basic.utils;

import fdu.daslab.executable.basic.model.OperatorBase;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/9/28 11:01 下午
 */
public class TopoTraversal {
    Queue<OperatorBase> queue = new LinkedList<>();

    public TopoTraversal(List<OperatorBase> heads) {
        queue.addAll(heads);
    }

    public OperatorBase nextOpt() {
        return queue.poll();
    }

    public void updateInDegree(OperatorBase opt, int delta) {
        opt.updateInDegree(delta);
        if (opt.getInDegree() <= 0) {
            queue.add(opt);
        }
    }

    public boolean hasNextOpt() {
        return !this.queue.isEmpty();
    }
}