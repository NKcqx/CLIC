package fdu.daslab.executable.serialize.api;

import fdu.daslab.executable.serialize.schema.FieldInfo;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;

import static fdu.daslab.executable.serialize.schema.SchemaUtil.buildAVROTableSchema;

/**
 * Created by Nathan on 2020/9/25.
 */
public class RowWiseSerializeTable<T> {


    //object 中需要被序列化的元素都有注解
    public ByteArrayOutputStream serializeToBuffer(ArrayList<T> table) throws Exception {
        assert table != null : "matrix can not be null";
        Schema tableSchema = new Schema.Parser().parse(buildAVROTableSchema(table.get(0).getClass()).toString());

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(tableSchema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        ByteArrayOutputStream res = new ByteArrayOutputStream();
        dataFileWriter.create(tableSchema, res);

        Class clazz = table.get(0).getClass();
        Field[] fs = clazz.getDeclaredFields();
        //每条数据是一个list， avro中有多少个entry，整个matrix就有多少行
        for (int i = 0; i < table.size(); i++) {
            T element = table.get(i);
            GenericRecord row = new GenericData.Record(tableSchema);
            for (Field f : fs) {
                f.setAccessible(true);
                FieldInfo column = f.getAnnotation(FieldInfo.class);
                if (column != null) {
                    row.put(column.fieldName(), f.get(element));
                }
            }
            dataFileWriter.append(row);
        }

        dataFileWriter.close();

        return res;
    }

    public void serializeToFile(ArrayList<T> table, String filePath) throws Exception {
        assert table != null : "matrix can not be null";
        Schema tableSchema = new Schema.Parser().parse(buildAVROTableSchema(table.get(0).getClass()).toString());

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(tableSchema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);

        dataFileWriter.create(tableSchema, new File(filePath));

        Class clazz = table.get(0).getClass();
        Field[] fs = clazz.getDeclaredFields();
        //每条数据是一个list， avro中有多少个entry，整个matrix就有多少行
        for (int i = 0; i < table.size(); i++) {
            T element = table.get(i);
            GenericRecord row = new GenericData.Record(tableSchema);
            for (Field f : fs) {
                f.setAccessible(true);
                FieldInfo column = f.getAnnotation(FieldInfo.class);
                if (column != null) {
                    row.put(column.fieldName(), f.get(element));
                }
            }
            dataFileWriter.append(row);
        }

        dataFileWriter.close();
    }
}
