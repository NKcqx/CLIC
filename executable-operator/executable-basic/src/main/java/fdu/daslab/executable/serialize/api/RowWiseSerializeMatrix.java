package fdu.daslab.executable.serialize.api;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static fdu.daslab.executable.serialize.schema.SchemaUtil.buildAvroMatrixSchema;


/**
 * Created by Nathan on 2020/9/24.
 */

// !! 这里的T只能是 double、int、long、float
public class RowWiseSerializeMatrix<T extends Number> {

    /**
     * @param matrix : 若要按行序列化矩阵，需要传入指定类型的矩阵
     * @return
     * @throws IOException
     */
    public ByteArrayOutputStream serializeToBuffer(ArrayList<ArrayList<T>> matrix) throws Exception {
        assert matrix != null : "matrix can not be null";
        assert checkSameDim(matrix) : "every row should be with same dim";
        assert checkType(matrix.get(0).get(0)) : "element type should be float,int,long,double, self-define class";

        Class elementClazz = matrix.get(0).get(0).getClass();
        Schema matrixSchema = new Schema.Parser().parse(buildAvroMatrixSchema(elementClazz).toString());

        //每条数据是一个list， avro中有多少个entry，整个matrix就有多少行
        ArrayList<GenericArray<T>> matrice = new ArrayList<>();
        for (int i = 0; i < matrix.size(); i++) {
            GenericArray<T> matrix1 = new GenericData.Array<T>(matrixSchema, matrix.get(i));
            matrice.add(matrix1);
        }

        DatumWriter<GenericArray<T>> datumWriter = new GenericDatumWriter<>(matrixSchema);
        DataFileWriter<GenericArray<T>> dataFileWriter = new DataFileWriter<>(datumWriter);

        ByteArrayOutputStream res = new ByteArrayOutputStream();
        dataFileWriter.create(matrixSchema, res);
        for (int i = 0; i < matrix.size(); i++) {
            dataFileWriter.append(matrice.get(i));
        }
        dataFileWriter.close();

        return res;
    }

    public void serializeToFile(ArrayList<ArrayList<T>> matrix, String filePath) throws Exception {
        assert matrix != null : "matrix can not be null";
        assert checkSameDim(matrix) : "every row should be with same dim";
        assert checkType(matrix.get(0).get(0)) : "element type should be float,int,long,double, self-define class";

        Class elementClazz = matrix.get(0).get(0).getClass();
        Schema matrixSchema = new Schema.Parser().parse(buildAvroMatrixSchema(elementClazz).toString());

        //每条数据是一个list， avro中有多少个entry，整个matrix就有多少行
        ArrayList<GenericArray<T>> matrice = new ArrayList<>();
        for (int i = 0; i < matrix.size(); i++) {
            GenericArray<T> matrix1 = new GenericData.Array<T>(matrixSchema, matrix.get(i));
            matrice.add(matrix1);
        }

        DatumWriter<GenericArray<T>> datumWriter = new GenericDatumWriter<>(matrixSchema);
        DataFileWriter<GenericArray<T>> dataFileWriter = new DataFileWriter<>(datumWriter);

        dataFileWriter.create(matrixSchema, new File(filePath));
        for (int i = 0; i < matrix.size(); i++) {
            dataFileWriter.append(matrice.get(i));
        }
        dataFileWriter.close();
    }


    private boolean checkType(Object t) {
        if ((!(t instanceof String)) && (!(t instanceof Integer)) && (!(t instanceof Long))
                && (!(t instanceof Float)) && (!(t instanceof Double))) {
            return false;
        }
        return true;
    }

    private boolean checkSameDim(ArrayList<ArrayList<T>> matrix) {
        int len = -1;
        for (ArrayList<T> e : matrix) {
            if (len == -1) {
                len = e.size();
            } else if (len != e.size()) {
                return false;
            }
        }
        return true;
    }

}
