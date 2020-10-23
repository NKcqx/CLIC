package fdu.daslab.executable.serialize.api;

import fdu.daslab.executable.serialize.schema.FieldInfo;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;

/**
 * Created by Nathan on 2020/9/25.
 */
public class RowWiseDeserializeTable<T> {

    public ArrayList<T> deserializeFromBuffer(ByteArrayOutputStream table, Class elementClass) throws Exception {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader =
                new DataFileReader<>(new SeekableByteArrayInput(table.toByteArray()), datumReader);
//        Schema schema = dataFileReader.getSchema();

        //class fields
        Field[] fields = elementClass.getDeclaredFields();
        ArrayList<String> key = new ArrayList<>();
        for (Field f : fields) {
            f.setAccessible(true);
        }
        ArrayList<T> res = new ArrayList<>();
        GenericRecord avroElement = null;
        while (dataFileReader.hasNext()) {
            T element = (T) elementClass.newInstance();
            avroElement = dataFileReader.next();
            for (Field f : fields) {
//                f.set(element, avroElement.get(f.getName()));
                setFieldValue(f, element, avroElement);
            }
            res.add(element);
        }
        return res;
    }


    public ArrayList<T> deserializeFromFile(String table, Class elementClass) throws Exception {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(table), datumReader);

        //class fields
        Field[] fields = elementClass.getDeclaredFields();
        ArrayList<String> key = new ArrayList<>();
        for (Field f : fields) {
            f.setAccessible(true);
        }
        ArrayList<T> res = new ArrayList<>();
        GenericRecord avroElement = null;
        while (dataFileReader.hasNext()) {
            T element = (T) elementClass.newInstance();
            avroElement = dataFileReader.next();
            for (Field f : fields) {
                setFieldValue(f, element, avroElement);
            }
            res.add(element);
        }
        return res;
    }

    public void setFieldValue(Field f, Object element, GenericRecord avroElement) throws Exception {
        FieldInfo fieldInfo = f.getAnnotation(FieldInfo.class);
        switch (fieldInfo.fieldType()) {
            case BOOLEAN:
                f.set(element, (boolean) avroElement.get(f.getName()));
                break;
            case STRING:
                f.set(element, avroElement.get(f.getName()).toString());
                break;
            case DOUBLE:
                f.set(element, (double) avroElement.get(f.getName()));
                break;
            case SHORT:
                f.set(element, (short) avroElement.get(f.getName()));
                break;
            case FLOAT:
                f.set(element, (float) avroElement.get(f.getName()));
                break;
            case LONG:
                f.set(element, (long) avroElement.get(f.getName()));
                break;
            case BYTE:
                f.set(element, (byte) avroElement.get(f.getName()));
                break;
            case INT:
                f.set(element, (int) avroElement.get(f.getName()));
                break;
            default:
                throw new Exception("illegal type");
        }
    }
}
