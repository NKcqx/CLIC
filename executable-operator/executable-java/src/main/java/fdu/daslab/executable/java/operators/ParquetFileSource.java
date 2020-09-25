package fdu.daslab.executable.java.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import java.util.*;
import java.util.stream.Stream;

/**
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/09/23 12:57
 */

@Parameters(separators = "=")
public class ParquetFileSource extends OperatorBase<Stream<List<String>>, Stream<List<String>>> {

    // 输入路径
    @Parameter(names = {"--input"}, required = true)
    String inputFileName;

    // 输入的分隔符
    @Parameter(names = {"--separator"})
    String separateStr = ",";

    public ParquetFileSource(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("FileSource", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Stream<List<String>>> result) {
        List<List<String>> resultList = new ArrayList<>();
        try {
            Path filePath = new Path(this.params.get("inputPath"));
            Configuration configuration = new Configuration();
//            configuration.set(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://192.168.8.206:9000");
            ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration,
                    filePath, ParquetMetadataConverter.NO_FILTER);
            //获取文件的schema
            MessageType schemas = readFooter.getFileMetaData().getSchema();
            ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), filePath);
            Group group;
            ParquetReader<Group> reader = builder.build();
            //将schema的信息作为list的头元素传递下去。
            resultList.add(Arrays.asList(schemas.toString()));
            this.setOutputData("result", resultList.stream());
            while ((group = reader.read()) != null) {

                List<String> list = new LinkedList<>();
                //使用递归解析group
                list = parseGroup(group, group.getType());

                resultList.add(list);
            }
            reader.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * 迭代解析每一个group
     * 读取parquet文件成list时需要解析group： [[bob0, blue, [3, 2]], [bob1, blue, [3, 2]]]
     * 其中 [3, 2]是一个整体string，不是list
     */
    private List<String>  parseGroup(Group group, GroupType groupType) {
        List<String> res = new LinkedList<String>();
        int fieldSize = groupType.getFieldCount();
        int j = 0;
        while (j < fieldSize) {
            if (!groupType.getType(j).isPrimitive()) { //如果是当前的field是group的话
                Group subgroup = group.getGroup(j, 0);
                String tmp = parseGroup(subgroup, subgroup.getType()).toString();
                res.add(j, tmp); //将读出的group数据放在指定位置
                j++;
            } else {
                int repetition = group.getFieldRepetitionCount(j);
                //根据该field重复repetition去读
                int k = 0;
                while (k < repetition) { //repetition>=1,因此一定会进入循环
                    res.add(group.getValueToString(j, k)); //k读取循环部分
                    k++;
                }
                if (repetition == 0) { //存在空值的情况
                    res.add(null);
                }
                j++;
            }
        }
        return res;
    }

}
