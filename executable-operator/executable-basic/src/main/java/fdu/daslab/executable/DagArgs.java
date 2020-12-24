package fdu.daslab.executable;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import java.util.HashMap;
import java.util.Map;

/**
 * 每个平台的参数，这些参数要求构建时均需要传入,
 *  其中：
 *      - dagPath：维护这个stage的dag的描述的文件
 *      - udfPath：udf的路径（少数平台可能没有）
 *      - stageId，masterHost，masterPort：需要和master交互需要的参数
 *
 * @author 唐志伟
 * @version 1.0
 * @since 12/23/20 3:11 PM
 */
@Parameters(separators = "=")
public class DagArgs {

    @Parameter(names = {"--stageId", "-sid"})
    String stageId = null;    // stage的全局唯一标识

    @Parameter(names = {"--udfPath", "-udf"})
    String udfPath = null;

    @Parameter(names = {"--dagPath", "-dag"})
    String dagPath = null;

    // @Parameter(names = {"--port", "-p"})
    // Integer thriftPort; // 本server启动的thrift端口

    @Parameter(names = {"--masterHost", "-mh"})
    String masterHost = null; // master的thrift地址

    @Parameter(names = {"--masterPort", "-mp"})
    Integer masterPort = null; // master的thrift端口

    // 其他的参数，使用--D开头
    @DynamicParameter(names = {"--D"})
    Map<String, String> platformArgs = new HashMap<>();


}
