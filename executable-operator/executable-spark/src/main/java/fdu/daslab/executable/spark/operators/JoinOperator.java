package fdu.daslab.executable.spark.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.BiOptParamsModel;
import fdu.daslab.executable.basic.model.BinaryBasicOperator;
import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * spark平台的join算子
 *
 * @author 刘丰艺
 * @since 2020/8/22 4:50 PM
 * @version 1.0
 */
@Parameters(separators = "=")
public class JoinOperator implements BinaryBasicOperator<JavaRDD<List<String>>> {

    @Parameter(names = {"--leftTableKeyName"})
    String leftTableKeyExtractFunctionName;

    @Parameter(names = {"--rightTableKeyName"})
    String rightTableKeyExtractFunctionName;

    @Parameter(names = {"--leftTableFuncName"})
    String leftTableFuncName;

    @Parameter(names = {"--rightTableFuncName"})
    String rightTableFuncName;

    @Override
    public void execute(BiOptParamsModel<JavaRDD<List<String>>> inputArgs,
                        ResultModel<JavaRDD<List<String>>> input1,
                        ResultModel<JavaRDD<List<String>>> input2,
                        ResultModel<JavaRDD<List<String>>> result) {
        JoinOperator joinArgs = (JoinOperator) inputArgs.getOperatorParam();

        List<String> leftKeys = new ArrayList();
        List<String> rightKeys = new ArrayList();
        List<List<String>> leftTable = new ArrayList();
        List<List<String>> rightTable = new ArrayList();

        input1.getInnerResult().foreach(item -> {
            FunctionModel joinFunction = inputArgs.getFunctionModel();
            // 用户指定key
            leftKeys.add((String) joinFunction.invoke(joinArgs.leftTableKeyExtractFunctionName, item));
            // 用户指定join时左表要select哪几列
            leftTable.add((List<String>) joinFunction.invoke(joinArgs.leftTableFuncName, item));
        });
        input2.getInnerResult().foreach(item -> {
            FunctionModel joinFunction = inputArgs.getFunctionModel();
            // 用户指定key
            rightKeys.add((String) joinFunction.invoke(joinArgs.rightTableKeyExtractFunctionName, item));
            // 用户指定join时右表要select哪几列
            rightTable.add((List<String>) joinFunction.invoke(joinArgs.rightTableFuncName, item));
        });

        List<String> resultLine = new ArrayList<>();
        List<List<String>> resultList = new ArrayList<>();
        for (int i = 0; i < leftTable.size(); i++) {
            for (int j = 0; j < rightTable.size(); j++) {
                if (leftKeys.get(i).equals(rightKeys.get(j))) {
                    resultLine.add(leftKeys.get(i));
                    resultLine.addAll(leftTable.get(i));
                    resultLine.addAll(rightTable.get(j));

                    resultList.add(Collections.singletonList(String.join(",", resultLine)));
                    resultLine.clear();
                }
            }
        }

        final JavaRDD<List<String>> nextStream = (JavaRDD<List<String>>) resultList.parallelStream();

        result.setInnerResult(nextStream);
    }
}
