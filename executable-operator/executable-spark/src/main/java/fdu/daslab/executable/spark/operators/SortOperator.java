package fdu.daslab.executable.spark.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.BasicOperator;
import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.spark.api.java.JavaRDD;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.List;

/**
 * spark平台的sort算子，可执行
 *
 * @author 唐志伟
 * @since 2020/7/6 1:53 PM
 * @version 1.0
 */
@Parameters(separators = "=")
public class SortOperator implements BasicOperator<JavaRDD<List<String>>> {

    // 通过指定路径来获取代码的udf
    @Parameter(names = {"--udfName"})
    String sortFunctionName;

    // 升序还是降序
    @Parameter(names = {"--ascending"})
    Boolean isAscending = true;

    // 排序之后的partition数量
    @Parameter(names = {"--partitionNum"})
    Integer partitionNum = 5;

    @Override
    public void execute(ParamsModel<JavaRDD<List<String>>> inputArgs,
                        ResultModel<JavaRDD<List<String>>> result) {
        SortOperator sortOperator = (SortOperator) inputArgs.getOperatorParam();
        final JavaRDD<List<String>> nextStream = result.getInnerResult()
                .sortBy(data -> new SerializableComparator(data, inputArgs, sortOperator),
                        sortOperator.isAscending, sortOperator.partitionNum);
        result.setInnerResult(nextStream);
    }

    // 默认的比较器Comparable是不能序列化的，因此需要重新定义一个
    static class SerializableComparator implements Serializable, Comparable<SerializableComparator> {
        private List<String> data;
        private ParamsModel<JavaRDD<List<String>>> inputArgs;
        private SortOperator sortOperator;

        SerializableComparator(List<String> data, ParamsModel<JavaRDD<List<String>>> inputArgs,
                                      SortOperator sortOperator) {
            this.data = data;
            this.inputArgs = inputArgs;
            this.sortOperator = sortOperator;
        }

        @Override
        public int compareTo(@Nonnull SerializableComparator that) {
            // 因为无法序列化，只能传入可序列化的ParamsModel
            FunctionModel functionModel = inputArgs.getFunctionModel();
            return (int) functionModel.invoke(sortOperator.sortFunctionName,
                    this.data, that.data);
        }
    }
}
