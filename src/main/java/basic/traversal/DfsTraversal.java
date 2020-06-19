package basic.traversal;

import basic.Operators.Operator;
import channel.Channel;
import com.sun.org.apache.xerces.internal.xs.datatypes.ObjectList;

import java.util.*;


public class DfsTraversal extends AbstractTraversal{
    private final Deque<Operator> stack;
    private final List<Operator> visited;
    public DfsTraversal(Operator root){
        super(root);
        stack=new ArrayDeque<Operator>();
        visited=new ArrayList<>();
        stack.addLast(root);

    }

    @Override
    public boolean hasNextOpt() {
        return !stack.isEmpty();
    }

    @Override
    public Operator nextOpt() {
        if(hasNextOpt()){
            Operator res=stack.pollLast();
            visited.add(res);

            ListIterator<Channel> iterator=res.getOutputChannel().listIterator(res.getOutputChannel().size());
            while(iterator.hasPrevious()){
                Operator toadd=iterator.previous().getTargetOperator();
                stack.addLast(toadd);
            }

            Operator toRemove=stack.peekLast();
            while(visited.contains(toRemove)){
                 stack.pollLast();
                 toRemove=stack.peekLast();
            }

            return res;
        }
        else return null;
    }

    @Override
    public int addSuccessor(Operator operator) {
        throw new RuntimeException("dfs not support this method");
    }


}
