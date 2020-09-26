package fdu.daslab.executable.java.operators;

import fdu.daslab.executable.basic.model.*;
import fdu.daslab.executable.basic.utils.ArgsUtil;
import fdu.daslab.executable.java.constants.JavaOperatorFactory;

import java.util.*;
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
    private List<Connection> triggerConnections; // trigger 的应该是 Loop -> loopHead 这条边 todo 那谁把loopVar 交给nextIteration？
    // 这应该有个自己的Executor

    public LoopOperator(String id,
                        List<String> inputKeys,
                        List<String> outputKeys,
                        Map<String, String> params) throws Exception {
        super("LoopOperator", id, inputKeys, outputKeys, params);
        realConnections = new ArrayList<>();
        try {
            HashMap<String, String> nextIterationParams = new HashMap<>();
            nextIterationParams.put("loopVarUpdateName", this.params.get("loopVarUpdateName"));
            this.myNextIteration = (NextIteration) new JavaOperatorFactory().createOperator(
                    "NextIteration",
                    ArgsUtil.randomUUID().toString(),
                    new ArrayList<>(this.inputData.keySet()), // 只是为了 set -> list
                    new ArrayList<>(this.outputData.keySet()),
                    nextIterationParams
            );
            this.myNextIteration.setParams("predicateName", this.params.get("predicateName"));
            this.myNextIteration.connectTo("loopVar", this, "loopVar");
            this.myNextIteration.connectTo("result", this, "data");

        } catch (Exception e) {
            e.printStackTrace();
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
            boolean continueLoop = (boolean) functionModel.invoke(
                    this.params.get("predicateName"),
                    loopVar);
            // 直接把数据forward到output
            this.setOutputData("result", this.getInputData("data"));
            this.setOutputData("loopVar", this.getInputData("loopVar"));
            if (!continueLoop) {
                this.setOperatorState(OperatorState.Finished);
            } else {
                this.setOperatorState(OperatorState.Running);
                // 然后dump给nextIteration
                List<List<String>> nextLoopVar = Collections.singletonList(loopVar);
                this.myNextIteration.setInputData("loopVar", nextLoopVar.stream());
            }
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
    }


}
