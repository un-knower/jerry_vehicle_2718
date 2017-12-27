package com.vehicle.data.resolver;

import com.vehicle.common.constants.JDBCConstants;
import com.vehicle.common.utils.JDBCUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author jerry
 */
public class OriginDataResolver {
    public static void main(String[] args) {
        String tableNameSuffix = "20171226";
        String sql = String.format("select * from %s where 1=1", JDBCConstants.TABLE_NAME_BASE + "20171226");
        try {
            Connection connection = JDBCUtils.getConnection();
            PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery();

            if (resultSet != null) {
                while (resultSet.next()) {
                    //TODO("处理数据")
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
