package com.hansight.kunlun.agent.collector.agent.file.process;

import com.hansight.kunlun.agent.collector.agent.Agent;
import com.hansight.kunlun.agent.collector.agent.position.store.ReadPositionStore;
import com.hansight.kunlun.agent.collector.common.base.LogWriter;
import com.hansight.kunlun.agent.collector.common.exception.LogWriteException;
import com.hansight.kunlun.agent.collector.common.model.Event;
import com.hansight.kunlun.agent.collector.common.model.ReadPosition;
import com.hansight.kunlun.agent.collector.common.utils.AgentConstants;
import com.hansight.kunlun.agent.collector.coordinator.metric.MetricException;
import com.hansight.kunlun.agent.collector.coordinator.metric.MetricService;
import com.hansight.kunlun.agent.collector.mq.kafka.KafkaProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public abstract class FileProcessor implements Runnable {
    protected int lineSeparatorLength;
    private LogWriter<Event> writer;
    final static Logger logger = LoggerFactory.getLogger(FileProcessor.class);
    protected Integer CACHE_SIZE = Integer.parseInt(Agent.GLOBAL.getProperty(AgentConstants.AGENT_WRITE_CACHE_SIZE, AgentConstants.AGENT_WRITE_CACHE_SIZE_DEFAULT)) + 1;

    protected boolean running = true;
    protected boolean finish = false;
    private boolean confSuc = true;
    public MetricService metricService;
    protected ReadPosition position;


    /**
     * 要处理的文件
     */
    protected BufferedReader reader;
    protected String path;
    /**
     * 文件处理组
     */
    protected ConcurrentHashMap<String, FileProcessor> executors;
    /**
     * 文件是否第一次处理
     */
    protected Boolean first;
    /**
     * 文件是否是新建的
     */
    protected Boolean create;
    /**
     * 文件已读取到的位置
     */
    protected ReadPositionStore store;
    /**
     * 配置的文件读取位置，相关配置忽略 我们的文件位置读取记录
     */
    protected Boolean beginning;
    protected long skipLine = 0;

    protected String topic;

    public void setLineSeparatorLength(int lineSeparatorLength) {
        this.lineSeparatorLength = lineSeparatorLength;
    }

    public void setBeginning(Boolean beginning) {
        this.beginning = beginning;
    }

    public void setReader(BufferedReader reader) {
        this.reader = reader;
    }

    public void setFirst(Boolean first) {
        this.first = first;
    }

    public void setCreate(Boolean create) {
        this.create = create;
    }

    public void setExecutors(ConcurrentHashMap<String, FileProcessor> executors) {
        this.executors = executors;
    }

    public void setStore(ReadPositionStore store) {
        this.store = store;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setSkipLine(long skipLine) {
        this.skipLine = skipLine;
    }

    public void setPath(String path) {
        this.path = path;
    }


    /**
     * 处理文件
     *
     * @return
     * @throws IOException
     */
    public abstract void process() throws IOException, InterruptedException, LogWriteException;

    /**
     * 默认实现了 文件读取开始位置，与结束位置的记录。
     */
    @Override
    public void run() {
        finish=false;
        String key = topic + "_" + path;
     /*   if (executors.get(key) != null)
            return;*/
      //  executors.put(key, this);
        logger.info("  processor reader:{} with topic:{} starting ... ", path, topic);
        try {
            position = store.get(key);

            if (position == null || create)
                position = new ReadPosition(key, 0l, 0l);
            writer = new KafkaProducer(topic, CACHE_SIZE);
            process();
            store.set(position);
            store.flush();
            writer.close();
            reader.close();
        } catch (FileNotFoundException e) {
            confSuc = false;
            logger.error("FileNotFound :" + key, e);
        } catch (IOException e) {
            confSuc = false;
            logger.error("FileIO error :" + key, e);
        } catch (InterruptedException e) {
            confSuc = false;
            logger.error("Interrupted error :" + key, e);
        } catch (LogWriteException e) {
            confSuc = false;
            logger.error("write error :" + key, e);
        }
        executors.remove(key);
        finish=true;
       // logger.info(" processor reader:{} with topic:{} finished. lines:{}", path, topic, position.getRecords());
    }

    public void write(Event event) throws LogWriteException {
       try {
            metricService.mark();
        } catch (MetricException e) {
            logger.error("metric error:{}", e);
        }
        writer.write(event);
    }

    public void stop() {
        running = false;
    }

    public boolean isConfSuc() {
        return confSuc;
    }

    public boolean isFinish() {
        return finish;
    }

    public void setMetricService(MetricService metricService) {
        this.metricService = metricService;
    }
}