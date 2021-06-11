package fdu.daslab.executable.spark.operators;

import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;
import java.util.Map;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/10/10 2:56 下午
 */
@Parameters(separators = "=")
public class CollectionSink extends OperatorBase<JavaRDD<List<String>>, List<String>> {
    public CollectionSink(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("CollectionSink", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<List<String>> result) {
        try {
            // split使用的分隔符就不放到配置文件里了，反正从String 解析 Array的方式也是暂时的
            List<List<String>> sinkData = this.getInputData("data").collect();
            this.setOutputData("result", sinkData.get(0));
        } catch (Exception e) {
//            while (true) ;
            // e.printStackTrace();
        }
    }

}
