package com.hansight.kunlun.collector.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Author:zhhui
 * DateTime:2014/7/29 16:05.
 */
public class Common {
    public static final String ES_CLUSTER_NAME = "es.cluster.name";
    public static final String ES_CLUSTER_HOST = "es.cluster.hosts";
    public static final String ZOOKEEPER_SESSION_TIMEOUT_MS = "zookeeper.session.timeout.ms";
    public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
    public static final String AUTO_COMMIT_ENABLE = "auto.commit.enable";
    public static final String AUTO_COMMIT_INTERVAL_MS = "auto.commit.interval.ms";
    public static final String ZOOKEEPER_CONNECTION_TIMEOUT = "zookeeper.connection.timeout.ms";
    public static final String AUTO_OFFSET_RESET = "auto.offset.reset";
    public static final String MONITOR_LOG_TIMES = "monitor.log.times";
    public static final String MONITOR_THREAD_POOL_SIZE = "monitor.thread.pool.size";
    public static final String MARK_DOACTIONTHRESHOLD ="mark.doActionThreshold";
    private final static Logger logger = LoggerFactory.getLogger(Common.class);
    private static final String CONF_FILE = "global.properties";
    private final static Properties GLOBAL = new Properties();

    static {
        InputStream global = Common.class.getClassLoader()
                .getResourceAsStream(CONF_FILE);
        try {
            GLOBAL.load(global);
        } catch (IOException e) {
            logger.trace("error:{}", e);
        }
    }

    public static String get(String name) {
        return GLOBAL.getProperty(name);

    }

    public static String get(String name, String object) {
        return GLOBAL.getProperty(name, object);

    }

    public static Properties getAll() {
        return GLOBAL;

    }
}