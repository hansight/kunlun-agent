package com.hansight.kunlun.collector.agent;


import com.hansight.kunlun.collector.agent.file.dir.LineFileLogReader;
import com.hansight.kunlun.collector.common.base.LogReader;
import com.hansight.kunlun.collector.common.utils.AgentConstants;
import com.hansight.kunlun.collector.common.utils.Common;
import com.hansight.kunlun.collector.coordinator.config.AgentConfig;
import com.hansight.kunlun.collector.coordinator.config.AgentMonitorService;
import com.hansight.kunlun.collector.coordinator.config.ConfigException;
import com.hansight.kunlun.collector.coordinator.config.MonitorException;
import com.hansight.kunlun.collector.coordinator.config.MonitorService;
import com.hansight.kunlun.collector.coordinator.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * guan_yi @config zhhui_yan
 */
public class Agent {
    private final static Logger logger = LoggerFactory.getLogger(Agent.class);
    private static String agent;
    private static ExecutorService threadPool;
    private static Map<String, LogReader> readers = new ConcurrentHashMap<>();
    public final static Properties CLASS_MAPPING = new Properties();

    public final static Properties GLOBAL = new Properties();

    static {
        InputStream global = Agent.class.getClassLoader()
                .getResourceAsStream(AgentConstants.AGENT_CONF);
        InputStream class_mapping = Agent.class.getClassLoader()
                .getResourceAsStream(AgentConstants.AGENT_CLASS_MAP_CONF);
        try {
            GLOBAL.load(global);
            GLOBAL.putAll(Common.getAll());
            CLASS_MAPPING.load(class_mapping);
        } catch (IOException e) {
            logger.trace("error:{}", e);
        }
    }

    public Agent() throws ConfigException {
        super();

    }


    public static void main(String[] args) throws IOException, ConfigException, InterruptedException {
        threadPool = Executors.newCachedThreadPool();
        agent = GLOBAL.getProperty(AgentConstants.AGENT_ID, AgentConstants.AGENT_ID_DEFAULT);
        MonitorService<AgentConfig> service = new AgentMonitorService(agent);
        try {
            service.registerConfigChangedProcessor(new ConfigChangedAction());
            threadPool.submit(service);
        } catch (MonitorException e) {
            logger.error("monitor error:{}", e);
        }

    }

    /**
     * 提供静态接口给
     */
    public void stop() {
        try {
            for (LogReader reader : readers.values()) {
                reader.stop();
            }
            threadPool.shutdownNow();
            while (!threadPool.isTerminated()) {
                Thread.sleep(10_000);
            }
        } catch (Exception e) {
            logger.error("error:{}", e);
        }
    }

    private static class ConfigChangedAction implements
            MonitorService.ConfigChangedProcessor<AgentConfig> {

        @Override
        @SuppressWarnings("unchecked")
        public void process(List<AgentConfig> configList) throws MonitorException {
            for (AgentConfig config : configList) {
                String key = config.getId();
                LogReader reader = readers.remove(key);
                try {
                    if (config.getState() == Config.State.DELETE) {
                        if (reader != null)//changed

                            reader.stop();

                        continue;
                    } else if (config.getState() == Config.State.UPDATE) {
                        if (reader != null)//changed
                            reader.stop();
                    }

                    String category = config.get("category");
                    String clazzName = CLASS_MAPPING.getProperty(category, LineFileLogReader.class.getName());
                    Class<LogReader> clazz = (Class<LogReader>) Class.forName(clazzName);
                    reader = clazz.newInstance();

                    config.put("agent", agent);
                    reader.setConf(config);
                    readers.put(key, reader);
                    threadPool.submit(reader);
                } catch (Exception e) {
                    logger.error("error:{}", e);
                }
                //add

            }
        }
    }
}
