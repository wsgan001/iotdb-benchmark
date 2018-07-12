package cn.edu.tsinghua.iotdb.benchmark.mysql;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.sql.*;

public class MeteorMySQL {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(MeteorMySQL.class);
    private Connection mysqlConnection = null;
    private Config config = ConfigDescriptor.getInstance().getConfig();

//    private final String RAD_TABLE_NAME =

    public MeteorMySQL() {
        try {
            Class.forName(Constants.MYSQL_DRIVENAME);
            mysqlConnection = DriverManager.getConnection(config.MYSQL_URL);
            initTable();
        } catch (SQLException e) {
            LOGGER.error("mysql 初始化失败，原因是：{}", e.getMessage());
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            LOGGER.error("mysql 连接初始化失败，原因是：{}", e.getMessage());
            e.printStackTrace();
        }
    }


    /**
     * 数据库中是否已经存在名字为table的表
     */
    public Boolean hasTable(String table) {

        Statement stat = null;
        try {
            String checkTable = "show tables like \"" + table + "\"";
            Statement stmt = mysqlConnection.createStatement();
            ResultSet resultSet = stmt.executeQuery(checkTable);
            if (resultSet.next()) {
                return true;
            } else {
                return false;
            }
        } catch (SQLException e) {
            LOGGER.error("判断表" + table + "是否存在 失败,原因是：{}", e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (stat != null)
                    stat.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    // 检查记录本次实验的表格是否已经创建，没有则创建
    public void initTable() {
        Statement stat = null;
        try {
            stat = mysqlConnection.createStatement();

            if (!hasTable("current_wind_profile_station_RAD")) {
                stat.executeUpdate("create table current_wind_profile_station_RAD"
                        + "(station varchar(6000), "
                        + "monitor_time BIGINT, "
                        + "data_file BLOB,"
                        + "primary key(station, monitor_time))");
                LOGGER.info("Table current_wind_profile_station_RAD create success!");
            }

            if (!hasTable("current_wind_profile_station_ROBS")) {
                stat.executeUpdate("create table current_wind_profile_station_ROBS"
                        + "(station varchar(6000), "
                        + "monitor_time BIGINT, "
                        + "data_file BLOB,"
                        + "primary key(station, monitor_time))");
                LOGGER.info("Table current_wind_profile_station_ROBS create success!");
            }

            return;

        } catch (SQLException e) {
            LOGGER.error("mysql 创建表格失败,原因是：{}", e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (stat != null)
                    stat.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void insertIntoTable(String tableName, String station, long time, File file) {
        try {
            String sql = "insert into " + tableName + " (station, monitor_time, data_file) values(?,?,?)";
            PreparedStatement preparedStatement = mysqlConnection.prepareStatement(sql);
            preparedStatement.setString(1, station);
            preparedStatement.setLong(2, time);
            FileInputStream fileInputStream = new FileInputStream(file);
            preparedStatement.setBlob(3, fileInputStream, file.length());
            preparedStatement.execute();
            preparedStatement.close();
        } catch (Exception e) {
            LOGGER.error(
                    "{} save data info into {} failed! Error：{}",
                    Thread.currentThread().getName(),
                    tableName,
                    e.getMessage());
            LOGGER.error("Error station: {}, time: {}", station, time);
            e.printStackTrace();
        }
    }

    public void renameTable(String oldTableName, String newTableName){
        Statement stat = null;
        try {
            stat = mysqlConnection.createStatement();
            stat.executeUpdate("RENAME TABLE " + oldTableName + " TO " + newTableName);
        } catch (SQLException e) {
            LOGGER.error("Rename " + oldTableName +" 失败,原因是：{}", e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (stat != null)
                    stat.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void deleteTable(String tableName){
        Statement stat = null;
        try {
            stat = mysqlConnection.createStatement();
            stat.executeUpdate("DROP TABLE " + tableName);
        } catch (SQLException e) {
            LOGGER.error("删除表 " + tableName +" 失败,原因是：{}", e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (stat != null)
                    stat.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


}
