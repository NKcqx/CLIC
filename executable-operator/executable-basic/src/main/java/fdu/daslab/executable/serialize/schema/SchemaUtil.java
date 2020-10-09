package fdu.daslab.executable.serialize.schema;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Nathan on 2020/9/24.
 */
public class SchemaUtil {
    public static StringBuffer buildAvroElementSchema(Class elementClazz) {
        String[] s = elementClazz.getName().split("\\.");
        String type = s[s.length - 1];
        StringBuffer template = new StringBuffer("{" +
                "\"type\":\"" + jdkType2AvroType.get(type) + "\"" +
                "}");
        return template;
    }

    public static StringBuffer buildAvroMatrixSchema(Class elementClazz) throws Exception {
        Annotation annotation = elementClazz.getAnnotation(UdfClass.class);
        assert annotation != null || checkType(elementClazz) : "unsupported matrix element type";

        if (annotation == null) { //非自定义类
            String[] s = elementClazz.getName().split("\\.");
            String type = s[s.length - 1];
            return buildAvroNumberMatrixSchema(jdkType2AvroType.get(type));
        } else {//自定义类
            return buildAvroObjectMatrixSchema(elementClazz);
        }
    }

    private static boolean checkType(Class t) {
        if ((t != String.class) && (t != Integer.class) && (t != Long.class)
                && (t != Float.class) && (t != Double.class)) {
            return false;
        }
        return true;
    }

    static HashMap<String, String> jdkType2AvroType = new HashMap<String, String>() {{
        put("Integer", "int");
        put("String", "string");
        put("Long", "long");
        put("Float", "float");
        put("Double", "double");
    }};

    public static StringBuffer buildAvroNumberMatrixSchema(String numberString) {
        StringBuffer template = new StringBuffer("{" +
                "\"type\":\"array\"," +
                "\"items\":\"" + numberString + "\"," +
                "\"default\":0" +
                "}");
        return template;
    }

    public static StringBuffer buildAVROTableSchema(Class elementClazz) throws Exception {
        StringBuffer templateHeader = new StringBuffer("{\n" +
                "  \"type\": \"record\", \n" +
                "  \"name\": \"Table\",\n" +
                "  \"fields\" : [");

        Field[] fields = elementClazz.getDeclaredFields();
        for (int i = 0; i < fields.length; i++) fields[i].setAccessible(true);
        HashMap<String, FieldType> fieldTypeHashMap = getFields(fields);


        for (Map.Entry<String, FieldType> e : fieldTypeHashMap.entrySet()) {
            templateHeader.append(buildSingleField(e.getKey(), e.getValue())).append(",");
        }
        templateHeader.deleteCharAt(templateHeader.length() - 1);//delete last coma


        String templateFooter = "  ]\n" + "}";
        StringBuffer template = templateHeader.append(templateFooter);
        return template;

    }

    public static StringBuffer buildAvroObjectMatrixSchema(Class elementClazz) throws Exception {
        StringBuffer templateHeader = new StringBuffer(
                "{ \"type\":\"array\"," +
                        "  \"items\":{" +
                        "     \"type\": \"record\"," +
                        "     \"name\": \"TableObject\",\n" +
                        "     \"fields\":[ \n");
        StringBuffer templateFooter = new StringBuffer(
                "      ]" + "    }" + "}");

        Field[] fields = elementClazz.getDeclaredFields();
        for (int i = 0; i < fields.length; i++) fields[i].setAccessible(true);
        HashMap<String, FieldType> fieldTypeHashMap = getFields(fields);


        for (Map.Entry<String, FieldType> e : fieldTypeHashMap.entrySet()) {
            templateHeader.append(buildSingleField(e.getKey(), e.getValue())).append(",");
        }
        templateHeader.deleteCharAt(templateHeader.length() - 1);//delete last coma
        StringBuffer template = templateHeader.append(templateFooter);
        template.toString();
        return template;
    }


    public static HashMap<String, FieldType> getFields(Field[] fields) {
        HashMap<String, FieldType> res = new HashMap<>();
        for (int i = 0; i < fields.length; i++) {
            Field f = fields[i];
            FieldInfo column = f.getAnnotation(FieldInfo.class);
            if (column != null) {
                res.put(column.fieldName(), column.fieldType());
            }// if column==null 意味着说这一列不进行序列化
        }
        return res;
    }

    public static StringBuffer buildSingleField(String fieldName, FieldType fieldType) throws Exception {
        String fieldTypeStr = null;
        switch (fieldType) {
            case INT:
                fieldTypeStr = "int";
                break;
            case BYTE:
                fieldTypeStr = "byte";
                break;
            case LONG:
                fieldTypeStr = "long";
                break;
            case FLOAT:
                fieldTypeStr = "float";
                break;
            case SHORT:
                fieldTypeStr = "short";
                break;
            case DOUBLE:
                fieldTypeStr = "double";
                break;
            case STRING:
                fieldTypeStr = "string";
                break;
            case BOOLEAN:
                fieldTypeStr = "boolean";
                break;
            default:
                throw new Exception("illegal field type");
        }
        return new StringBuffer("\n    {\"name\":\"" + fieldName + "\",\"type\":\"" + fieldTypeStr + "\"}");
    }


}
