package fdu.daslab.executable.java.operators;

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

    public ParquetFileSource(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("ParquetFileSource", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Stream<List<String>>> result) {
        List<List<String>> resultList = new ArrayList<>();
        try {
            Path filePath = new Path(this.params.get("inputPath"));
            Configuration configuration = new Configuration();
            ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration,
                    filePath, ParquetMetadataConverter.NO_FILTER);
            //获取文件的schema
            MessageType schemas = readFooter.getFileMetaData().getSchema();
            ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), filePath);
            Group group;
            ParquetReader<Group> reader = builder.build();

            while ((group = reader.read()) != null) {

                List<String> list = new LinkedList<>();
                //使用递归解析group
                list = parseGroup(group, group.getType());

                resultList.add(list);
            }
            //存储schema信息。将schema放到baseOperator的信息中。
            this.setSchema(schemas.toString());
            this.setOutputData("result", resultList.stream());
            reader.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * @param group group数据
     * @param groupType  类型信息
     * @return 将group行取后转化的list
     */
    private List<String>  parseGroup(Group group, GroupType groupType) {
        List<String> res = new LinkedList<String>();
        int fieldSize = groupType.getFieldCount();
        for (int j = 0; j < fieldSize; j++) {
            //如果是当前的field是group的话
            if (!groupType.getType(j).isPrimitive()) {
                Group subgroup = group.getGroup(j, 0);
                String tmp = parseGroup(subgroup, subgroup.getType()).toString();
                //将读出的group数据放在指定位置
                res.add(j, tmp);
            } else {
                int repetition = group.getFieldRepetitionCount(j);
                //根据该field重复repetition去读
                for (int k = 0; k < repetition; k++) {
                    //k读取循环部分
                    res.add(group.getValueToString(j, k));
                }
                //存在空值的情况
                if (repetition == 0) {
                    res.add(null);
                }
            }
        }
        return res;
    }

}
