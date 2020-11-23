package fdu.daslab.executable.java.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
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
import java.util.stream.Stream;

/**
 * 从文件读取table的算子
 * 目前与FileSource的区别是从第二行开始读
 *
 * @author 刘丰艺
 * @since 2020/10/27 9:30 PM
 * @version 1.0
 */
public class TableSource extends OperatorBase<Stream<List<String>>, Stream<List<String>>> {

    Logger logger = LoggerFactory.getLogger(TableSource.class);

    public TableSource(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("JavaTableSource", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Stream<List<String>>> result) {
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
            String line;
            List<List<String>> resultList = new ArrayList<>();
            while ((line = bufferedReader.readLine()) != null) {
                resultList.add(Arrays.asList(line.split(this.params.get("separator"))));
            }
            this.setOutputData("result", resultList.stream());
            bufferedReader.close();
            inputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
