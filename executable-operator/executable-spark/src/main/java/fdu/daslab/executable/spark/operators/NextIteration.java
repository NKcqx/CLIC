package fdu.daslab.executable.spark.operators;

import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;


/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/9/24 11:42 下午
 */
public class NextIteration extends OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>> {
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
    public void execute(ParamsModel inputArgs, ResultModel<JavaRDD<List<String>>> result) {
        final JavaSparkContext javaSparkContext = SparkInitUtil.getDefaultSparkContext();

        FunctionModel functionModel = inputArgs.getFunctionModel();
        assert functionModel != null;
        try {
            List<String> loopVar = this.getInputData("loopVar").first();
            // 只更新 loopVar
            // 按理说是不是应该结束的时候再更新呢，即放到nextIteration里面
            int nextLoopVar = (int) functionModel.invoke(
                    this.params.get("loopVarUpdateName"),
                    loopVar);
            loopVar = Collections.singletonList(String.valueOf(nextLoopVar));
            List<List<String>> wrappedLoopVar = Collections.singletonList(loopVar);
            this.setOutputData("loopVar", javaSparkContext.<List<String>>parallelize(wrappedLoopVar));
            this.setOutputData("result", this.getInputData("data"));
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }

    }
}
