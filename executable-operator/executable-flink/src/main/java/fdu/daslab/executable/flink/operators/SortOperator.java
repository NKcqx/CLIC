package fdu.daslab.executable.flink.operators;

import com.beust.jcommander.Parameter;
import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Flink平台的批处理Sort算子
 * @author 李姜辛
 * @version 1.0
 * @since 2021/3/11 15:00
 */

public class SortOperator extends OperatorBase<DataSet<List<String>>, DataSet<List<String>>> {

    // 通过指定路径来获取代码的udf
    @Parameter(names = {"--udfName"})
    String sortFunctionName;

    // 升序还是降序
    @Parameter(names = {"--ascending"})
    Boolean isAscending = true;

    // 排序之后的partition数量
//    @Parameter(names = {"--partitionNum"})
//    Integer partitionNum = 5;

    public SortOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("FlinkSortOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<DataSet<List<String>>> result) {
        // SortOperator sortOperator = (SortOperator) inputArgs.getOperatorParam();


        final String udfName = this.params.get("udfName");

        final DataSet<List<String>> nextStream = this.getInputData("data")
                .sortPartition(data -> new SerializableComparator(data, inputArgs, udfName),
                        Boolean.parseBoolean(this.params.get("ascending")) ? Order.ASCENDING : Order.DESCENDING)
                .setParallelism(1); // sort in one partition: global sort;
        this.setOutputData("result", nextStream);

        // 调试代码
//        final DataSet<List<String>> res = this.getOutputData("result");

//        try {
//            res.print();
//        } catch (Exception e){
//            e.printStackTrace();
//        }

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
