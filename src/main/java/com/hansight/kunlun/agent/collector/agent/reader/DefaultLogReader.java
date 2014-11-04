package com.hansight.kunlun.agent.collector.agent.reader;

import com.hansight.kunlun.agent.collector.common.base.LogReader;
import com.hansight.kunlun.agent.collector.common.model.Event;
import com.hansight.kunlun.agent.collector.coordinator.config.AgentConfig;
import com.hansight.kunlun.agent.collector.coordinator.metric.MetricService;
import com.hansight.kunlun.agent.collector.coordinator.metric.ProcessorType;

import java.util.Queue;

public abstract class DefaultLogReader implements LogReader {
    protected boolean running = true;
    protected Queue<Event> queue;
    protected AgentConfig conf;
    public MetricService metricService;

    @Override
    public void setConf(AgentConfig conf) {
        this.conf = conf;
        metricService = new MetricService(conf.getId(), conf.get("agent"), ProcessorType.AGENT);
    }

    @Override
    public Void call() throws Exception {
        try {
            setup();
            run();
            cleanup();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String get(AgentConfig conf, String key,
                             String defaultValue) {
        Object object = conf.get(key);
        if (object == null) {
            return defaultValue;
        }
        String value = object.toString();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }



    @Override
    public void stop() throws Exception {
        running = false;
    }
}
