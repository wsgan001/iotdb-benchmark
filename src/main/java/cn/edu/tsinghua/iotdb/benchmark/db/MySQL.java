package cn.edu.tsinghua.iotdb.benchmark.db;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.distribution.ProbTool;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class MySQL implements IDatebase{
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQL.class);
    private Connection connection;
    private static Config config;
    private long labID;
    private MySqlLog mySql;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Random sensorRandom;
    private Random timestampRandom;
    private ProbTool probTool;
    private final double unitTransfer = 1000000000.0;

    public MySQL(long labID){
        this.labID = labID;
        config = ConfigDescriptor.getInstance().getConfig();
        mySql = new MySqlLog();
        sensorRandom = new Random(1 + config.QUERY_SEED);
    }

    @Override
    public void init() throws SQLException {
        try {
            Class.forName(Constants.MYSQL_DRIVENAME);
            connection = DriverManager.getConnection(config.MYSQL_URL);
        } catch (SQLException e) {
            LOGGER.error("mysql 初始化失败，原因是：{}", e.getMessage());
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            LOGGER.error("mysql 连接初始化失败，原因是：{}", e.getMessage());
            e.printStackTrace();
        }
        mySql.initMysql(labID);
    }

    @Override
    public void createSchema() throws SQLException {
        Statement stat = null;
        try {
            stat = connection.createStatement();

            for(int group_index = 0;group_index<config.GROUP_NUMBER;group_index++){
                String sql = genCreateTableSQL(group_index);
                stat.executeUpdate(sql);
                LOGGER.info("Table SERVER_MODE create success!");
            }
        } catch (SQLException e) {
            LOGGER.error("mysql schema创建失败,原因是：{}", e.getMessage());
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

    private String genCreateTableSQL(int group_index) {
        StringBuilder sb = new StringBuilder();
        sb.append("create table group_" + group_index);
        sb.append(" (timestamp BIGINT, device varchar(30),");
        for(int sensor_id=0;sensor_id<config.SENSOR_NUMBER;sensor_id++){
            sb.append(" s_" + sensor_id + " DOUBLE,");
        }
        sb.append(" primary key(timestamp,device))");
        return sb.toString();
    }

    private String getGroup(String device) {
        String[] spl = device.split("_");
        int deviceIndex = Integer.parseInt(spl[1]);
        int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
        int groupIndex = deviceIndex / groupSize;
        return "group_" + groupIndex;
    }

    public String createSQLStatment(int batch, int index, String device) {
        long currentTime = Constants.START_TIMESTAMP + config.POINT_STEP * (batch * config.CACHE_NUM + index);
        StringBuilder builder = new StringBuilder();
        String group = getGroup(device);
        builder.append("insert into ").append(group).append(" values(");
        builder.append(currentTime).append(",").append("'").append(device).append("'");
        for (String sensor : config.SENSOR_CODES) {
            FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
            builder.append(",").append(Function.getValueByFuntionidAndParam(param, currentTime));
        }
        builder.append(")");

        LOGGER.debug("createSQLStatment:  {}", builder.toString());
        return builder.toString();
    }

    @Override
    public long getLabID() {
        return this.labID;
    }

    @Override
    public void insertOneBatch(String device, int loopIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException {
        Statement statement;
        int[] result;
        long errorNum = 0;
        statement = connection.createStatement();
        for (int i = 0; i < config.CACHE_NUM; i++) {
            String sql = createSQLStatment(loopIndex, i, device);
            statement.addBatch(sql);
        }
        long startTime = System.nanoTime();
        try {
            result = statement.executeBatch();
        } catch (BatchUpdateException e) {
            long[] arr = e.getLargeUpdateCounts();
            for (long i : arr) {
                if (i == -3) {
                    errorNum++;
                }
            }
        }
        statement.clearBatch();
        statement.close();
        long endTime = System.nanoTime();
        long costTime = endTime - startTime;
        if (errorNum > 0) {
            LOGGER.info("Batch insert failed, the failed number is {}! ", errorNum);
        } else {
            LOGGER.info("{} execute {} loop, it costs {}s, totalTime {}s, throughput {} points/s",
                    Thread.currentThread().getName(), loopIndex, costTime / unitTransfer,
                    (totalTime.get() + costTime) / unitTransfer,
                    (config.CACHE_NUM * config.SENSOR_NUMBER / (double) costTime) * unitTransfer);
            totalTime.set(totalTime.get() + costTime);
        }
        errorCount.set(errorCount.get() + errorNum);

        mySql.saveInsertProcess(loopIndex, (endTime - startTime) / unitTransfer, totalTime.get() / unitTransfer, errorNum,
                config.REMARK);
    }

    @Override
    public void insertOneBatch(LinkedList<String> cons, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException {

    }

    @Override
    public void close() throws SQLException {

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
