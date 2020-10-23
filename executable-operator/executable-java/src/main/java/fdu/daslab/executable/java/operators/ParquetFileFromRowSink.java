package fdu.daslab.executable.java.operators;

import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Type;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

/**
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/09/23 12:57
 */

@Parameters(separators = "=")
public class ParquetFileFromRowSink extends OperatorBase<Stream<List<String>>, Stream<List<String>>> {

    public ParquetFileFromRowSink(String id,
                                  List<String> inputKeys,
                                  List<String> outputKeys,
                                  Map<String, String> params) {
        super("ParquetFileFromRowSink", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<Stream<List<String>>> result) {

        try {
            //从baseoperator获取schema信息
            String schemaStr = this.getSchema();
            MessageType schema = MessageTypeParser.parseMessageType(schemaStr);

            Path outPath = new Path(this.params.get("outputPath"));
            ExampleParquetWriter.Builder builder = ExampleParquetWriter
                    .builder(outPath).withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                    .withCompressionCodec(CompressionCodecName.SNAPPY) //压缩
                    .withType(schema);

            ParquetWriter<Group> writer = builder.build();
            SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

            this.getInputData("data").forEach(record -> {

                Group group = groupFactory.newGroup();
                int size = record.size();

                for (int i = 0; i < size; i++) {
                    Type type = schema.getFields().get(i);
                    //如果当前值为空值的话，且不是required，跳过不写入该field
                    if (record.get(i) == null && !type.toString().contains("required")) {
                        continue;
                    } //如果类型是required但是值为null的话，在添加field的时，会空指针报错

                    Group tmpGroup = addField(group, type, record.get(i));
                    if (tmpGroup == null) { //如果返回是空值，不写入
                        continue;
                    }
                    group = tmpGroup;
                }
                try {
                    writer.write(group);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据schema的类型进行写入
     *
     * @param group      group数据
     * @param type       类型信息
     * @param fieldValue 需要存储的field值
     * @return 返回写入后的group
     */
    private Group addField(Group group, Type type, String fieldValue) {
        //获取name
        String fieldName = type.getName().toString();
        //获取除name以外的信息
        //暂时支持下面这些类型
        String typeInfo = type.toString().replace(fieldName, "");
        if (typeInfo.contains("int32") || typeInfo.contains("int64")
                || typeInfo.contains("int")) {
            group.append(fieldName, Integer.parseInt(fieldValue));
        } else if (typeInfo.contains("long")) {
            group.append(fieldName, Long.parseLong(fieldValue));
        } else if (typeInfo.contains("double")) {
            group.append(fieldName, Double.parseDouble(fieldValue));
        } else if (typeInfo.contains("float")) {
            group.append(fieldName, Float.parseFloat(fieldValue));
        } else if (typeInfo.contains("boolean")) {
            group.append(fieldName, Boolean.parseBoolean(fieldValue));
        } else if (typeInfo.contains("string") || typeInfo.contains("binary")) {
            group.append(fieldName, fieldValue);
        } else {
            return null;
        }
        return group;
    }
}

