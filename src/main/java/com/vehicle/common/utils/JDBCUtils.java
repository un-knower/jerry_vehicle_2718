package com.vehicle.common.utils;

import com.vehicle.common.constants.JDBCConstants;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * JDBC工具类
 *
 * @author jerry
 */
public class JDBCUtils {
    /**
     * 获得JDBC连接
     *
     * @return
     */
    public static Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName(ConfigUtils.getString(JDBCConstants.MYSQL_DRIVER, "com.mysql.jdbc.Driver"));
            connection = DriverManager.getConnection(ConfigUtils.getProperty(JDBCConstants.MYSQL_URL)
                    , ConfigUtils.getProperty(JDBCConstants.MYSQL_USER)
                    , ConfigUtils.getProperty(JDBCConstants.MYSQL_PASSWORD));
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 关闭JDBC连接
     *
     * @param connection
     */
    public static void close(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
