package cn.edu.tsinghua.iotdb.benchmark.db;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.distribution.ProbTool;
import cn.edu.tsinghua.iotdb.benchmark.loadData.Point;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MeteorMySQL;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class MySQL implements IDatebase {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(MeteorMySQL.class);
    private Connection connection = null;
    private Config config;

    private static final String createStatementSQL = "create timeseries %s with datatype=DOUBLE,encoding=%s";

    private long labID;
    private MySqlLog mySql;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Random sensorRandom;
    private Random timestampRandom;
    private ProbTool probTool;
    private final double unitTransfer = 1000000000.0;

    public MySQL(long labID) throws ClassNotFoundException, SQLException {
        config = ConfigDescriptor.getInstance().getConfig();
        mySql = new MySqlLog();
        this.labID = labID;

    }

    private Boolean hasTable(String table) throws SQLException {
        String checkTable = "show tables like \"" + table + "\"";
        Statement stmt = connection.createStatement();

        ResultSet resultSet = stmt.executeQuery(checkTable);
        if (resultSet.next()) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void init() throws SQLException {
        try {
            Class.forName(Constants.MYSQL_DRIVENAME);
            connection = DriverManager.getConnection(config.MYSQL_URL);
        } catch (ClassNotFoundException e) {
            LOGGER.error("MySQL Database init() 失败，原因是：{}", e.getMessage());
            e.printStackTrace();
        }
        mySql.initMysql(labID);
    }

    @Override
    public void createSchema() throws SQLException {

    }

    @Override
    public long getLabID() {
        return this.labID;
    }

    @Override
    public void insertOneBatch(String device, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException {

    }

    @Override
    public void insertOneBatch(LinkedList<String> cons, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException {

    }

    @Override
    public void close() throws SQLException {
        if (connection != null) {
            connection.close();
        }
        if (mySql != null) {
            mySql.closeMysql();
        }
    }

    @Override
    public long getTotalTimeInterval() throws SQLException {
        return 0;
    }

    @Override
    public void executeOneQuery(List<Integer> devices, int index, long startTime, QueryClientThread client, ThreadLocal<Long> errorCount) {

    }

    @Override
    public void insertOneBatchMulDevice(LinkedList<String> deviceCodes, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException {

    }

    @Override
    public long count(String group, String device, String sensor) {
        return 0;
    }

    @Override
    public void createSchemaOfDataGen() throws SQLException {

    }

    @Override
    public void insertGenDataOneBatch(String device, int i, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException {

    }

    @Override
    public void exeSQLFromFileByOneBatch() throws SQLException, IOException {

    }

    @Override
    public int insertOverflowOneBatch(String device, int loopIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, ArrayList<Integer> before, Integer maxTimestampIndex, Random random) throws SQLException {
        return 0;
    }

    @Override
    public int insertOverflowOneBatchDist(String device, int loopIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, Integer maxTimestampIndex, Random random) throws SQLException {
        return 0;
    }
}
