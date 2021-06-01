package fdu.daslab.executable.spark.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.*;
import org.apache.spark.api.java.JavaRDD;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * spark平台的sort算子，可执行
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/7/6 1:53 PM
 */
@Parameters(separators = "=")
public class SortOperator extends OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>> {

    // 通过指定路径来获取代码的udf
    @Parameter(names = {"--udfName"})
    String sortFunctionName;

    // 升序还是降序
    @Parameter(names = {"--ascending"})
    Boolean isAscending = true;

    // 排序之后的partition数量
    @Parameter(names = {"--partitionNum"})
    Integer partitionNum = 5;

    public SortOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkSortOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<JavaRDD<List<String>>> result) {
        // SortOperator sortOperator = (SortOperator) inputArgs.getOperatorParam();
        final JavaRDD<List<String>> nextStream = this.getInputData("data")
                .sortBy(data -> new SerializableComparator(data, inputArgs, this.params.get("udfName")),
                        Boolean.parseBoolean(this.params.get("ascending")),
                        Integer.parseInt(this.params.get("partitionNum")));
        this.setOutputData("result", nextStream);
    }

    // 默认的比较器Comparable是不能序列化的，因此需要重新定义一个
    static class SerializableComparator implements Serializable, Comparable<SerializableComparator> {
        private List<String> data;
        private ParamsModel inputArgs;
        private String sortFuncName;

        SerializableComparator(List<String> data, ParamsModel inputArgs,
                               String sortFuncName) {
            this.data = data;
            this.inputArgs = inputArgs;
            this.sortFuncName = sortFuncName;
        }

        @Override
        public int compareTo(@Nonnull SerializableComparator that) {
            // 因为无法序列化，只能传入可序列化的ParamsModel
            FunctionModel functionModel = inputArgs.getFunctionModel();
            return (int) functionModel.invoke(this.sortFuncName,
                    this.data, that.data);
        }
    }
}
