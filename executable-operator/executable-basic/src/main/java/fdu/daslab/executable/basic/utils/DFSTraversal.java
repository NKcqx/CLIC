package fdu.daslab.executable.basic.utils;

import fdu.daslab.executable.basic.model.Connection;
import fdu.daslab.executable.basic.model.OperatorBase;

import java.util.List;
import java.util.Stack;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/9/25 12:19 下午
 */
public class DFSTraversal {
    Stack<OperatorBase> stack = new Stack<>();

    public DFSTraversal(OperatorBase rootOperator){
        stack.add(rootOperator);
    }

    public OperatorBase nextOperator(){
        OperatorBase nextOperator =  stack.pop();
        List<Connection> allNextConnections = nextOperator.getOutputConnections();
        for (Connection connection : allNextConnections){
            OperatorBase sonOpt = connection.getTargetOpt();
            stack.push(sonOpt);
        }
        return nextOperator;
    }
    public boolean hasNext(){
        return stack.isEmpty();
    }
}
