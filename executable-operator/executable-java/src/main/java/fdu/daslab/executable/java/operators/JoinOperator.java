package fdu.daslab.executable.java.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author 唐志伟，刘丰艺，陈齐翔
 * @version 1.0
 * @since 2020/7/6 14:05
 */
@Parameters(separators = "=")
public class JoinOperator extends OperatorBase<Stream<List<String>>, Stream<List<String>>> {

    public JoinOperator(String id,
                        List<String> inputKeys,
                        List<String> outputKeys,
                        Map<String, String> params) {
        super("JavaJoinOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<Stream<List<String>>> result) {
        FunctionModel joinFunction = inputArgs.getFunctionModel();
        assert joinFunction != null;

        List<String> leftKeys = new ArrayList();
        List<String> rightKeys = new ArrayList();
        List<List<String>> leftTable = new ArrayList();
        List<List<String>> rightTable = new ArrayList();

        this.getInputData("leftTable").forEach(item -> {
            // 用户指定key
            leftKeys.add((String) joinFunction.invoke(this.params.get("leftTableKeyName"), item));
            // 用户指定join时左表要select哪几列
            leftTable.add((List<String>) joinFunction.invoke(this.params.get("leftTableFuncName"), item));
        });
        this.getInputData("rightTable").forEach(item -> {
            // 用户指定key
            rightKeys.add((String) joinFunction.invoke(this.params.get("rightTableKeyName"), item));
            // 用户指定join时右表要select哪几列
            rightTable.add((List<String>) joinFunction.invoke(this.params.get("rightTableFuncName"), item));
        });
        result.getInnerResult("leftTable").forEach(item -> {
            // 用户指定key
            leftKeys.add((String) joinFunction.invoke(this.params.get("leftTableKeyName"), item));
            // 用户指定join时左表要select哪几列
            leftTable.add((List<String>) joinFunction.invoke(this.params.get("leftTableFuncName"), item));
        });
        result.getInnerResult("rightTable").forEach(item -> {
            // 用户指定key
            rightKeys.add((String) joinFunction.invoke(this.params.get("rightTableKeyName"), item));
            // 用户指定join时右表要select哪几列
            rightTable.add((List<String>) joinFunction.invoke(this.params.get("rightTableFuncName"), item));
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

        Stream<List<String>> nextStream = resultList.stream();
        this.setOutputData("result", nextStream);
    }
}
