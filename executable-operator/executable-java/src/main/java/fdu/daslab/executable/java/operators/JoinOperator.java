package fdu.daslab.executable.java.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.*;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Java平台的join算子
 */
@Parameters(separators = "=")
public class JoinOperator implements BinaryBasicOperator<Stream<List<String>>, Stream<List<String>>, Stream<List<String>>> {

    @Parameter(names={"--rightTableKeyName"})
    String rightTableKeyExtractFunctionName;

    @Parameter(names={"--leftTableKeyName"})
    String leftTableKeyExtractFunctionName;

    @Parameter(names={"--rightTableFuncName"})
    String rightTableFuncName;

    @Parameter(names={"--leftTableFuncName"})
    String leftTableFuncName;

    @Override
    public void execute(ParamsModel<Stream<List<String>>> inputArgs,
                        ResultModel<Stream<List<String>>> input1,
                        ResultModel<Stream<List<String>>> input2) {
        JoinOperator joinArgs = (JoinOperator) inputArgs.getOperatorParam();
        FunctionModel joinFunction = inputArgs.getFunctionModel();
        assert joinFunction != null;
        // 新右表
        Stream<List<String>> newRightTable = input2.getInnerResult()
                .map(data -> (List<String>) joinFunction.invoke(joinArgs.rightTableFuncName, data));
        // 新左表
        Stream<List<String>> newLeftTable = input1.getInnerResult()
                .map(data -> (List<String>) joinFunction.invoke(joinArgs.leftTableFuncName, data));
        // 右表转化成Map
        Map<String, List<String>> rightTableMap = newRightTable
                .collect(Collectors.toMap(
                        data -> (String) joinFunction.invoke(joinArgs.rightTableKeyExtractFunctionName, data),
                        value -> value,
                        (value1, value2) -> value1
                        ));
        // 左表转化成Map
        Map<String, List<String>> leftTableMap = newLeftTable
                .collect(Collectors.toMap(
                        data -> (String) joinFunction.invoke(joinArgs.leftTableKeyExtractFunctionName, data),
                        value -> value,
                        (value1, value2) -> value1
                ));
        // 左表 join 右表
        List<String> resultLine = new ArrayList<>();
        List<String> resultList = new ArrayList<>();
        for(Map.Entry<String, List<String>> entry1 : leftTableMap.entrySet()) {
            for(Map.Entry<String, List<String>> entry2 : rightTableMap.entrySet()) {
                if(entry1.getKey().equals(entry2.getKey())) {
                    resultLine.addAll(entry1.getValue());
                    resultLine.addAll(entry2.getValue());
                }
            }
            resultList.add(String.join(",",resultLine));
            resultLine.clear();
        }
        Stream<List<String>> nextStream = (Stream<List<String>>) resultList;
        input1.setInnerResult(nextStream);
    }
}
