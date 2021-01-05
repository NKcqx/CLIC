package fdu.daslab.executable.graphchi.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Qinghua Du
 * @version 1.0
 * @since 2020/11/24 10:20
 */
public class FileSinkOperator extends OperatorBase<Stream<List<String>>, File> {
    Logger logger = LoggerFactory.getLogger(FileSinkOperator.class);

    public FileSinkOperator(String id, List<String> inputKeys,
                            List<String> outputKeys, Map<String, String> params) {
        super("FileSink", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<File> result) {
        File file = new File(this.params.get("outputPath"));
        if (file.exists() && file.isFile()) {
            logger.info("Stage(graphchi) ———— Output file size :" + file.length());
        } else {
            logger.info("Stage(graphchi) ———— File doesn't exist or it is not a file");
        }
        try {
            FileWriter fileWritter = new FileWriter(file, true);
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
            // 数据准备好
            this.getMasterClient().postDataPrepared();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
