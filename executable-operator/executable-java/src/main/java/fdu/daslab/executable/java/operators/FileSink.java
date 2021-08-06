package fdu.daslab.executable.java.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 文件写入的算子
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/7/6 1:41 PM
 */
@Parameters(separators = "=")
public class FileSink extends OperatorBase<Stream<List<String>>, Stream<List<String>>> {
    Logger logger = LoggerFactory.getLogger(FileSink.class);
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
            File file = new File(this.params.get("outputPath"));
            if (file.exists() && file.isFile()) {
                logger.info("Stage(java) ———— Output file size :" + file.length());
            } else {
                logger.info("Stage(java) ———— File doesn't exist or it is not a file");
            }
            FileWriter fileWritter = new FileWriter(file, true);
            BufferedWriter out = new BufferedWriter(fileWritter);
            this.getInputData("data")
                    // result.getInnerResult("data")
                    .forEach(record -> {
                        System.out.println();
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
            // 数据准备好
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
