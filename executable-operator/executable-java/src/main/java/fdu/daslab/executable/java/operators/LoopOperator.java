package fdu.daslab.executable.java.operators;

import fdu.daslab.executable.basic.model.*;
import fdu.daslab.executable.basic.utils.ArgsUtil;
import fdu.daslab.executable.basic.utils.TopTraversal;
import fdu.daslab.executable.java.constants.JavaOperatorFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

/**
 * 实际上功能是 Repeat
 *
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/9/24 2:13 下午
 */
public class LoopOperator extends OperatorBase<Stream<List<String>>, Stream<List<String>>> {
    private List<Connection> realConnections;
    private NextIteration myNextIteration;
    private OperatorBase loopEnd;
    private List<Connection> triggerConnections; // trigger 的应该是 Loop -> loopHead 这条边 todo 那谁把loopVar 交给nextIteration？
    // 这应该有个自己的Executor

    public LoopOperator(String id,
                        List<String> inputKeys,
                        List<String> outputKeys,
                        Map<String, String> params) throws Exception {
        super("LoopOperator", id, inputKeys, outputKeys, params);
        realConnections = new ArrayList<>();
        try {
            this.myNextIteration = (NextIteration) new JavaOperatorFactory().createOperator(
                    "NextIteration",
                    ArgsUtil.randomUUID().toString(),
                    new ArrayList<>(this.inputData.keySet()), // 只是为了 set -> list
                    new ArrayList<>(this.outputData.keySet()),
                    null
            );
            this.myNextIteration.setParams("predicateName", this.params.get("predicateName"));
        } catch (Exception e) {
            throw new Exception("这创建 NextIteration 的时候出错了，可能是这里的硬编码没有和配置文件的修改同步");
        }
        triggerConnections = new ArrayList<>();
        // myNextIteration.setTheLoopOperator(this);
    }

    public void startLoopBody(OperatorBase loopHead, String targetKey) { // loopVar怎么传给body里的Opt ?
        // 因为 loop 的 connectTo被重写了，这里只能直接操作Connection
        Connection triggerConnection = new Connection(this, "result", loopHead, targetKey);
        this.triggerConnections.add(triggerConnection);
    }

    public void endLoopBody(OperatorBase loopEnd, String sourceKey) {
        // 设置双指针
        loopEnd.connectTo(sourceKey, myNextIteration, "data");
        myNextIteration.connectFrom("data", loopEnd, sourceKey);
    }

    @Override
    public List<Connection> getOutputConnections() { // 起到Switch的作用
        if  (this.operatorState == OperatorState.Finished){
            return realConnections;
        }else if (this.operatorState == OperatorState.Running || this.operatorState == OperatorState.Ready){
            return triggerConnections;
        }else {
            return null; // Wait
        }
    }

    @Override
    public void connectTo(String sourceKey, OperatorBase targetOpt, String targetKey) {
        // 先记录下 真正Loop算完之后的下一跳是谁，等回头Loop运行结束后再去找它
        realConnections.add(new Connection(this, sourceKey, targetOpt, targetKey));
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Stream<List<String>>> result) {
        FunctionModel functionModel = inputArgs.getFunctionModel();
        assert functionModel != null;
        try {
            List<String> loopVar = this.getInputData("loopVar").findAny().orElseThrow(NoSuchElementException::new);
            boolean endLoop = (boolean) functionModel.invoke(
                    this.params.get("predicateName"),
                    loopVar);
            // 直接把数据forward到output
            this.setOutputData("result", this.getInputData("data"));
            this.setOutputData("loopVar", this.getOutputData("loopVar"));
            if (endLoop) {
                this.setOperatorState(OperatorState.Finished);
            } else {
                this.setOperatorState(OperatorState.Running);
                // 然后dump给nextIteration
                this.myNextIteration.setInputData("loopVar", this.getOutputData("loopVar"));
            }
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
    }


}
