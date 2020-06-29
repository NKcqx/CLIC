package edu.daslab.executable.java.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.daslab.executable.basic.model.BasicOperator;
import edu.daslab.executable.basic.model.ParamsModel;
import edu.daslab.executable.basic.model.ResultModel;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * 文件读取，返回一个二维数组，不指定类型
 */
@Parameters(separators = "=")
public class FileSource implements BasicOperator<Stream<List<String>>> {

    // 输入路径
    @Parameter(names={"--input"}, required = true)
    String inputFileName;

    // 输入的分隔符
    @Parameter(names={"--sep"})
    String separateStr = ",";

    @Override
    public void execute(ParamsModel<Stream<List<String>>> inputArgs, ResultModel<Stream<List<String>>> result) {
        FileSource sourceArgs = (FileSource) inputArgs.getOperatorParam();
        try {
            FileInputStream inputStream = new FileInputStream(sourceArgs.inputFileName);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

            String line;
            List<List<String>> resultList = new ArrayList<>();
            while ((line = bufferedReader.readLine()) != null) {
                resultList.add(Arrays.asList(line.split(sourceArgs.separateStr)));
            }
            result.setInnerResult(resultList.stream());// 设置最后的stream
            bufferedReader.close();
            inputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
