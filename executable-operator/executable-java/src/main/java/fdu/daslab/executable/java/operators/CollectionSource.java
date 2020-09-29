package fdu.daslab.executable.java.operators;

import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;

import java.util.*;
import java.util.stream.Stream;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/9/28 10:22 下午
 */
@Parameters(separators = "=")
public class CollectionSource extends OperatorBase<Stream<List<String>>, Stream<List<String>>> {
    public CollectionSource(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("CollectionSource", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Stream<List<String>>> result) {
        try {
            // split使用的分隔符就不放到配置文件里了，反正从String 解析 Array的方式也是暂时的
            List<String> loopVars = Arrays.asList(this.params.get("inputList").split(" "));
            List<List<String>> loopVarBox = Collections.singletonList(loopVars);
            this.setOutputData("result", loopVarBox.stream());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
