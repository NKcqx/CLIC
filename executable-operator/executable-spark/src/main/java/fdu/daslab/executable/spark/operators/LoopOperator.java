package fdu.daslab.executable.spark.operators;

import fdu.daslab.executable.basic.model.*;
import fdu.daslab.executable.basic.utils.ArgsUtil;
import fdu.daslab.executable.spark.constants.SparkOperatorFactory;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.javatuples.Pair;

import java.util.*;

/**
 * 实际上功能是 Repeat
 *
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/9/24 2:13 下午
 */
public class LoopOperator extends OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>> {
    private List<Connection> realConnections;
    private NextIteration myNextIteration;
    private List<Connection> triggerConnections; // trigger 的应该是 Loop -> loopHead 这条边
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
            this.myNextIteration = (NextIteration) new SparkOperatorFactory().createOperator(
                    "NextIteration",
                    ArgsUtil.randomUUID().toString(),
                    new ArrayList<>(this.inputData.keySet()), // 只是为了 set -> list
                    new ArrayList<>(this.outputData.keySet()),
                    nextIterationParams
            );
            this.myNextIteration.setParams("predicateName", this.params.get("predicateName"));
            this.myNextIteration.connectTo("loopVar", this, "loopVar");
            this.myNextIteration.connectTo("result", this, "data");


            if (this.params.get("loopBody") != null && !this.params.get("loopBody").isEmpty()) {
                constructBodyFromYAML(this.params.get("loopBody"));
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("这创建 NextIteration 的时候出错了，可能是这里的硬编码没有和配置文件的修改同步");
        }
    }

    /**
     * 将loopBody的字符串解析为DAG
     * @param bodyYaml 用于表示loopBody的YAML格式的字符串
     * @throws Exception 字符串中出现不受支持的Operator时抛出
     */
    public void constructBodyFromYAML(String bodyYaml) throws Exception {
//        String bodyYaml = this.params.get("loopBody");
        Pair<List<OperatorBase>, List<OperatorBase>> loopHeadsAndEnds =
                ArgsUtil.parseArgs(bodyYaml, new SparkOperatorFactory());

        this.triggerConnections = new ArrayList<>();
        this.startLoopBody(loopHeadsAndEnds.getValue0().get(0));
        this.endLoopBody(loopHeadsAndEnds.getValue1().get(0));
    }

    /**
     * 设置loopBody的起点Operator，并将LoopOperator 与其相连
     *
     * @param loopHead 起点Operator
     */
    public void startLoopBody(OperatorBase loopHead) { // loopVar怎么传给body里的Opt ?
        // 因为 loop 的 connectTo被重写了，这里只能直接操作Connection
        Connection triggerConnection = new Connection(this, "result", loopHead, "data");
        this.triggerConnections.add(triggerConnection);
        try {
            loopHead.connectFrom(triggerConnection);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void endLoopBody(OperatorBase loopEnd) {
        // 设置双指针
        loopEnd.connectTo("result", myNextIteration, "data");
        myNextIteration.connectFrom("data", loopEnd, "result");
    }

    @Override
    public List<Connection> getOutputConnections() { // 起到Switch的作用
        if (this.operatorState == OperatorState.Finished) {
            return realConnections;
        } else {
            return triggerConnections;
        }
    }

    @Override
    public void connectTo(String sourceKey, OperatorBase targetOpt, String targetKey) {
        // 先记录下 真正Loop算完之后的下一跳是谁，等回头Loop运行结束后再去找它
        realConnections.add(new Connection(this, sourceKey, targetOpt, targetKey));
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<JavaRDD<List<String>>> result) {
        final JavaSparkContext javaSparkContext = SparkInitUtil.getDefaultSparkContext();
        FunctionModel functionModel = inputArgs.getFunctionModel();
        assert functionModel != null;
        try {
            List<String> loopVar = this.getInputData("loopVar").first();
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
                this.myNextIteration.setInputData("loopVar", javaSparkContext.parallelize(nextLoopVar));
            }
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
    }

}
