package fdu.daslab.executable.java.operators;

import fdu.daslab.executable.basic.model.*;

import java.util.*;
import java.util.stream.Stream;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/9/24 11:42 下午
 */
public class NextIteration extends OperatorBase<Stream<List<String>>, Stream<List<String>>> {
    private LoopOperator theLoopOperator;

    public NextIteration(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("NextIteration", id, inputKeys, outputKeys, params);
        // 该控制节点不会在前端有配置文件，因此无法自动添加输入输出的key
        this.inputData.put("data", null);
        this.inputData.put("loopVar", null);
        this.outputData.put("result", null);
        this.outputData.put("loopVar", null);
    }

    public void setTheLoopOperator(LoopOperator loopOperator) {
        this.theLoopOperator = loopOperator;
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Stream<List<String>>> result) {
        FunctionModel functionModel = inputArgs.getFunctionModel();
        assert functionModel != null;
        try {
            List<String> loopVar = this.getInputData("loopVar").findAny().orElseThrow(NoSuchElementException::new);
            // 只更新 loopVar
            // 按理说是不是应该结束的时候再更新呢，即放到nextIteration里面
            int nextLoopVar = (int) functionModel.invoke(
                    this.params.get("loopVarUpdateName"),
                    loopVar);
            loopVar = Collections.singletonList(String.valueOf(nextLoopVar));
            this.setOutputData("loopVar", Stream.of(loopVar));
            this.setOutputData("result", this.getInputData("data"));
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }

    }
}
