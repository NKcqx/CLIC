package fdu.daslab.executable.java.operators;

import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;


import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

/**
 * 使用该方法读取文件时，schema所有类型均需为required或者不可存在值为null的情况
 *
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/10/09 18:39
 */
@Parameters(separators = "=")
public class ParquetFileToColumnSource extends OperatorBase<Stream<List<String>>, Stream<List<String>>> {

    public ParquetFileToColumnSource(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("ParquetFileToColumnSource", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Stream<List<String>>> result) {
        List<List<String>> resultList = new ArrayList<>();
        try {
            Configuration conf = new Configuration();
            Path filePath = new Path(this.params.get("inputPath"));
            ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, filePath,
                    ParquetMetadataConverter.NO_FILTER);
            MessageType schemas = readFooter.getFileMetaData().getSchema();
            ParquetFileReader parquetFileReader = new ParquetFileReader(conf, filePath, readFooter);
            PageReadStore rowGroup = null;

            try {
                while (null != (rowGroup = parquetFileReader.readNextRowGroup())) {
                    ColumnReader colReader = null;
                    ColumnReadStore colReadStore = new ColumnReadStoreImpl(rowGroup, new GroupRecordConverter(schemas).
                            getRootConverter(), schemas, null);
                    List<ColumnDescriptor> descriptorList = schemas.getColumns();

                    //for each column
                    for (ColumnDescriptor colDescriptor:descriptorList) {

                        //获取 column的数据类型
                        PrimitiveType.PrimitiveTypeName type = colDescriptor.getType();
                        String[] columnNamePath = colDescriptor.getPath();
                        colReader = colReadStore.getColumnReader(colDescriptor);
                        long totalValuesInColumnChunk =  rowGroup.getPageReader(colDescriptor).getTotalValueCount();
                        List<String>  tmp = new LinkedList<>();
                        //every cell in the column chunk
                        for (int i = 0; i < totalValuesInColumnChunk; i++) {

                           tmp.add(getColumn(colReader, type));
                           colReader.consume();

                        }
                        resultList.add(tmp);
                    }
                }
                //存储schema信息。将schema放到baseOperator的信息中。
                this.setSchema(schemas.toString());
                this.setOutputData("result", resultList.stream());

            } catch (Exception exception) {
                exception.printStackTrace();
            } finally {
                parquetFileReader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     *
     * @param colReader reader
     * @param type  column类型
     * @return 返回cell结果
     */
    private String getColumn(ColumnReader colReader, PrimitiveType.PrimitiveTypeName type) {
        String result = null;
        //暂时支持下面这些类型
        if (type.equals(PrimitiveType.PrimitiveTypeName.INT32)) {
            result = String.valueOf(colReader.getInteger());

        } else if (type.equals(PrimitiveType.PrimitiveTypeName.BINARY)) {
            result = colReader.getBinary().toStringUsingUTF8();
        } else if (type.equals(PrimitiveType.PrimitiveTypeName.BOOLEAN)) {
            result = String.valueOf(colReader.getBoolean());
        } else if (type.equals(PrimitiveType.PrimitiveTypeName.DOUBLE)) {
            result = String.valueOf(colReader.getDouble());
        } else if (type.equals(PrimitiveType.PrimitiveTypeName.FLOAT)) {
            result = String.valueOf(colReader.getFloat());
        } else if (type.equals(PrimitiveType.PrimitiveTypeName.INT64)) {
            result = String.valueOf(colReader.getLong());
        } else {
            return null;
        }
        return result;
    }

}
