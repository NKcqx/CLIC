package fdu.daslab.executable.java.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import joinery.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 从文件读取table的算子
 *
 * @author 刘丰艺
 * @since 2020/10/27 9:30 PM
 * @version 1.0
 */
public class TableSource extends OperatorBase<DataFrame<Object>, DataFrame<Object>> {

    Logger logger = LoggerFactory.getLogger(TableSource.class);

    public TableSource(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("JavaTableSource", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<DataFrame<Object>> result) {
        try {
            File file = new File(this.params.get("inputPath"));
            FileInputStream inputStream = new FileInputStream(file);
            if (file.exists() && file.isFile()) {
                logger.info("Stage(java) ———— Input file size:  " + file.length());
            } else {
                logger.info("Stage(java) ———— File doesn't exist or it is not a file");
            }
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

            List<String> tableSchema = Arrays.asList(bufferedReader.readLine().split(","));
            DataFrame<Object> resultDF = new DataFrame<>(tableSchema);

            String line;
            while ((line = bufferedReader.readLine()) != null) {
                resultDF.append(Arrays.asList(line.split(",")));
            }
            this.setOutputData("result", resultDF);
            bufferedReader.close();
            inputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
