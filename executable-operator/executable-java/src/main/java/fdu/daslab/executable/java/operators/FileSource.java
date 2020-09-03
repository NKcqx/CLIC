package fdu.daslab.executable.java.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 文件读取，返回一个二维数组，不指定类型
 *
 * @author 唐志伟
 * @since 2020/7/6 1:46 PM
 * @version 1.0
 */
@Parameters(separators = "=")
public class FileSource extends OperatorBase<Stream<List<String>>, Stream<List<String>>>{

    // 输入路径
    @Parameter(names = {"--input"}, required = true)
    String inputFileName;

    // 输入的分隔符
    @Parameter(names = {"--separator"})
    String separateStr = ",";

    public FileSource(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("FileSource", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Stream<List<String>>> result) {
        try {
            FileInputStream inputStream = new FileInputStream(this.params.get("inputPath"));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

            String line;
            List<List<String>> resultList = new ArrayList<>();
            while ((line = bufferedReader.readLine()) != null) {
                resultList.add(Arrays.asList(line.split(this.params.get("separator"))));
            }
            this.setOutputData("result", resultList.stream());
            // result.setInnerResult("result", resultList.stream()); // 设置最后的stream
            bufferedReader.close();
            inputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
