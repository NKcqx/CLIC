package fdu.daslab.executable.java.operators.table;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * jdbc连接数据库
 *
 * @author 刘丰艺
 * @since 2020/11/17 4:30 PM
 * @version 1.0
 */
public class ConnectDB extends OperatorBase<Connection, Connection> {
    public ConnectDB(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("JavaConnectDB", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Connection> result) {
        Connection conn = null;
        try {
            // 注册JDBC驱动
            Class.forName(this.params.get("driver"));
            // 打开链接
            conn = DriverManager.getConnection(
                    this.params.get("url"), this.params.get("user"), this.params.get("password")
            );
            this.setOutputData("result", conn);
        } catch (ClassNotFoundException ce) {
            ce.printStackTrace();
        } catch (SQLException se) {
            se.printStackTrace();
        }
    }
}
