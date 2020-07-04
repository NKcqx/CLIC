package fdu.daslab.executable.java.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Java平台的join算子
 */
@Parameters(separators = "=")
public class JoinOperator implements BinaryBasicOperator<Stream<List<String>>> {

    @Parameter(names={"--leftTableKeyName"})
    String leftTableKeyExtractFunctionName;

    @Parameter(names={"--rightTableKeyName"})
    String rightTableKeyExtractFunctionName;

    @Parameter(names={"--leftTableFuncName"})
    String leftTableFuncName;

    @Parameter(names={"--rightTableFuncName"})
    String rightTableFuncName;

    @Override
    public void execute(BiOptParamsModel<Stream<List<String>>> inputArgs,
                        ResultModel<Stream<List<String>>> input1,
                        ResultModel<Stream<List<String>>> input2,
                        ResultModel<Stream<List<String>>> result) {
        JoinOperator joinArgs = (JoinOperator) inputArgs.getOperatorParam();
        FunctionModel joinFunction = inputArgs.getFunctionModel();
        assert joinFunction != null;

        List<String> leftKeys = new ArrayList();
        List<String> rightKeys = new ArrayList();
        List<List<String>> leftTable = new ArrayList();
        List<List<String>> rightTable = new ArrayList();

        input1.getInnerResult().forEach(item -> {
            // 用户指定key
            leftKeys.add((String) joinFunction.invoke(joinArgs.leftTableKeyExtractFunctionName, item));
            // 用户指定join时左表要select哪几列
            leftTable.add((List<String>) joinFunction.invoke(joinArgs.leftTableFuncName, item));
        });
        input2.getInnerResult().forEach(item -> {
            // 用户指定key
            rightKeys.add((String) joinFunction.invoke(joinArgs.rightTableKeyExtractFunctionName, item));
            // 用户指定join时右表要select哪几列
            rightTable.add((List<String>) joinFunction.invoke(joinArgs.rightTableFuncName, item));
        });

        List<String> resultLine = new ArrayList<>();
        List<List<String>> resultList = new ArrayList<>();
        for(int i=0;i<leftTable.size();i++) {
            for(int j=0;j<rightTable.size();j++) {
                if(leftKeys.get(i).equals(rightKeys.get(j))) {
                    resultLine.add(leftKeys.get(i));
                    resultLine.addAll(leftTable.get(i));
                    resultLine.addAll(rightTable.get(j));

                    resultList.add(Collections.singletonList(String.join(",", resultLine)));
                    resultLine.clear();
                }
            }
        }

        Stream<List<String>> nextStream = resultList.stream();
        result.setInnerResult(nextStream);
    }
}
