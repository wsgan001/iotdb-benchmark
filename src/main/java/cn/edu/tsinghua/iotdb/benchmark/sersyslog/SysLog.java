package cn.edu.tsinghua.iotdb.benchmark.sersyslog;

import cn.edu.tsinghua.iotdb.benchmark.App;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.db.IDBFactory;
import cn.edu.tsinghua.iotdb.benchmark.db.IDatebase;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

public class SysLog {
    private static final Logger LOGGER_RESULT = LoggerFactory.getLogger(SysLog.class);
    private static SysLog INSTANCE = new SysLog();
    private Config config;
    private SysLog() {
        config = ConfigDescriptor.getInstance().getConfig();
    }

    public static SysLog getInstance() {
        return INSTANCE;
    }

    public void outputLog(String log){




    }
}
