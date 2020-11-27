package fdu.daslab.executable.java.operators;

import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.*;

import java.util.*;
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
        super("JoinOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<Stream<List<String>>> result) {
        FunctionModel joinFunction = inputArgs.getFunctionModel();
        assert joinFunction != null;

        String leftKey = this.params.get("leftKey");
        String rightKey = this.params.get("rightKey");
        String leftCols = this.params.get("leftCols");
        String rightCols = this.params.get("rightCols");

        List<String> leftKeys = new ArrayList();
        List<String> rightKeys = new ArrayList();
        List<List<String>> leftTable = new ArrayList();
        List<List<String>> rightTable = new ArrayList();

        if (leftKey == null) {
            throw new NoSuchElementException("key of left table must not be empty!");
        }
        if (rightKey == null) {
            throw new NoSuchElementException("key of right table must not be empty!");
        }

        this.getInputData("leftTable").forEach(item -> {
            // 用户指定key
            leftKeys.add((String) joinFunction.invoke(leftKey, item));
            if (leftCols != null) {
                // 用户指定join时左表要select哪几列
                leftTable.add((List<String>) joinFunction.invoke(leftCols, item));
            } else {
                leftTable.add(item);
            }
        });

        this.getInputData("rightTable").forEach(item -> {
            // 用户指定key
            rightKeys.add((String) joinFunction.invoke(rightKey, item));
            if (rightCols != null) {
                // 用户指定join时右表要select哪几列
                rightTable.add((List<String>) joinFunction.invoke(rightCols, item));
            } else {
                rightTable.add(item);
            }
        });

        List<String> resultLine = new ArrayList<>();
        List<List<String>> resultList = new ArrayList<>();
        for (int i = 0; i < leftTable.size(); i++) {
            for (int j = 0; j < rightTable.size(); j++) {
                if (leftKeys.get(i).equals(rightKeys.get(j))) {
                    if (leftCols != null) {
                        // key
                        resultLine.add(leftKeys.get(i));
                    }
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
