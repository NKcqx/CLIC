package fdu.daslab.executable.java.operators.table;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 从数据库读取table
 *
 * @author 刘丰艺
 * @since 2020/11/17 4:30 PM
 * @version 1.0
 */
public class DBTableSource extends OperatorBase<Connection, Stream<List<String>>> {
    public DBTableSource(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("JavaDBTableSource", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Stream<List<String>>> result) {
        Connection conn = this.getInputData("data");
        try {
            Statement stmt = conn.createStatement();
            String sqlText = "select * from " + this.params.get("dbtable");
            ResultSet rs = stmt.executeQuery(sqlText);
            int columnCount = rs.getMetaData().getColumnCount();
            List<List<String>> resultList = new ArrayList<>();
            while (rs.next()) {
                String[] line = new String[columnCount];
                for (int i=0; i<columnCount; i++) {
                    line[i] = rs.getString(i+1);
                }
                resultList.add(Arrays.asList(line));
            }
            this.setOutputData("result", resultList.stream());
            rs.close();
            stmt.close();
            conn.close();
        } catch (SQLException se) {
            se.printStackTrace();
        }
    }
}
