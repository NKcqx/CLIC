package edu.daslab.executable.java.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.daslab.executable.basic.model.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Java平台的join算子 暂时有点bug
 */
@Parameters(separators = "=")
public class JoinOperator implements BasicOperator<Stream<List<String>>> {

    // 通过指定路径来获取代码的udf
    @Parameter(names={"--udfName"})
    String joinFunctionName;

    @Override
    public void execute(ParamsModel<Stream<List<String>>> inputArgs,
                        ResultModel<Stream<List<String>>> result) {
        JoinOperator joinArgs = (JoinOperator) inputArgs.getOperatorParam();
        FunctionModel joinFunction = inputArgs.getFunctionModel();
        assert joinFunction != null;
//        另一条stream从本地文件读取
//        try {
//            BufferedReader inBuf = new BufferedReader(
//                    new FileReader("D:/executable-operator/executable-basic/src/main/resources/data/webBlackList.txt")
//            );
//            List<String> list2 = new ArrayList<>();
//            String lineTemp = "";
//            while ((lineTemp = inBuf.readLine()) != null) {
//                list2.add(lineTemp);
//            }
//            inBuf.close();
//
//            Stream<List<String>> nextStream = result.getInnerResult()
//                .flatMap(i -> {
//                    return list2.stream().map(j -> new String[]{i, j});
//                });
//
//            result.setInnerResult(nextStream);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }
}
