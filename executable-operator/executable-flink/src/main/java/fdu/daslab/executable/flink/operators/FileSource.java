package fdu.daslab.executable.flink.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Flink批处理读入source
 * @author 李姜辛
 * @version 1.0
 * @since 2021/3/14 15:13
 */
public class FileSource extends OperatorBase<DataSet<List<String>>, DataSet<List<String>>> {


    public FileSource(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("FlinkFileSource", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<DataSet<List<String>>> result) {
        // flink batch env
        final ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
        fbEnv.setParallelism(1);
//        final StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // FileSource sourceArgs = (FileSource) inputArgs.getOperatorParam();
        // 读取文件，并按照分割符分隔开来
        // QUESTION(WAIT): 是否需要设置并行度？通过传入的参数设置？spark中的partition是全局的并行度设置吗？
        // TODO: 设置并行度

        // QUESTION: 把split方法的参数换成常字符串就可以打印输出了？
        final String separator = this.params.get("separator");
        final DataSet<List<String>> listDataSet = fbEnv
                .readTextFile(this.params.get("inputPath"))
                .map(line -> Arrays.asList(line.split(separator)))
                .returns(TypeInformation.of(new TypeHint<List<String>>(){}))
                ;

        this.setOutputData("result", listDataSet);
        /* 打印输出有数据 */
//        try {
//            listDataSet.print();
//        } catch (Exception e){
//
//        }
        final DataSet<List<String>> res = this.getOutputData("result");

//        output.put("result", listDataSet);
//        final DataSet<List<String>> res = output.get("result");

        /* 用父类的outputData Map存取，打印输出结果为空 */
//        try {
//            res.print();
//
//        } catch (Exception e){
//            e.printStackTrace();
//        }

//        final List<List<String>> listRes = this.getOutputData("result");
//        final DataSet<List<String>> res = fbEnv.fromCollection(listRes);


        /* 用自己new的Map存取，打印输出有结果 */
//        Map<String, DataSet<List<String>>> mp = new HashMap<>();
//        mp.put("result", listDataSet);
//        final DataSet<List<String>> res = mp.get("result");


        /* 打印输出 */

//        try {
//            res.print();
//        } catch (Exception e){
//
//        }

    }
}
