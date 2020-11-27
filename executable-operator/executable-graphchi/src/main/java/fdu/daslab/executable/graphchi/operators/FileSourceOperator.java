package fdu.daslab.executable.graphchi.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Map;

/**
 * @author Qinghua Du
 * @version 1.0
 * @since 2020/11/24 10:20
 */
public class FileSourceOperator extends OperatorBase<File, InputStream> {
    Logger logger = LoggerFactory.getLogger(FileSourceOperator.class);

    public FileSourceOperator(String name, String id, List<String> inputKeys,
                              List<String> outputKeys, Map<String, String> params) {
        super(name, id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<InputStream> result) {
        File file = new File(this.params.get("inputPath"));
        try {
            final FileInputStream inputStream = new FileInputStream(file);
            if (file.exists() && file.isFile()) {
                logger.info("Stage(graphchi) ———— Input file size:  " + file.length());
            } else {
                logger.info("Stage(graphchi) ———— File doesn't exist or it is not a file");
            }
            this.setOutputData("result", inputStream);
        } catch (Exception e) {
            e.printStackTrace();

        }
    }
}
