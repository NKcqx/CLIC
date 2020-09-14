package fdu.daslab.executable.java.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 文件写入的算子
 *
 * @author 唐志伟
 * @since 2020/7/6 1:41 PM
 * @version 1.0
 */
@Parameters(separators = "=")
public class FileSink extends OperatorBase<Stream<List<String>>, Stream<List<String>>> {

    // 输入路径
    @Parameter(names = {"--output"}, required = true)
    String outputFileName;

    // 输出的分隔符
    @Parameter(names = {"--separator"})
    String separateStr = ",";

    public FileSink(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("FileSink", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<Stream<List<String>>> result) {
        // FileSink fileSinkArgs = (FileSink) inputArgs.getOperatorParam();
        try {
            FileWriter fileWritter = new FileWriter(this.params.get("outputPath"), true);
            BufferedWriter out = new BufferedWriter(fileWritter);
            this.getInputData("data")
            // result.getInnerResult("data")
                    .forEach(record -> {
                        StringBuilder writeLine = new StringBuilder();
                        record.forEach(field -> {
                            writeLine.append(field);
                            writeLine.append(this.params.get("separator"));
                        });
                        writeLine.deleteCharAt(writeLine.length() - 1);
                        writeLine.append("\n");
                        try {
                            out.write(writeLine.toString());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
            out.close();
            fileWritter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
