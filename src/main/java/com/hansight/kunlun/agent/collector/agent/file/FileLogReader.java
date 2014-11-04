package com.hansight.kunlun.agent.collector.agent.file;

import com.hansight.kunlun.agent.collector.agent.Agent;
import com.hansight.kunlun.agent.collector.agent.file.process.FileProcessor;
import com.hansight.kunlun.agent.collector.agent.position.store.ReadPositionStore;
import com.hansight.kunlun.agent.collector.agent.reader.DefaultLogReader;
import com.hansight.kunlun.agent.collector.common.exception.LogReaderException;
import com.hansight.kunlun.agent.collector.common.utils.AgentConstants;
import com.hansight.kunlun.agent.collector.coordinator.metric.MetricException;
import com.hansight.kunlun.agent.collector.coordinator.metric.WorkerStatus;
import com.hansight.kunlun.agent.collector.utils.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Author:zhhui
 * DateTime:2014/7/29 15:55.
 * iis,nigix
 */
public abstract class FileLogReader extends DefaultLogReader {
    final static Logger logger = LoggerFactory.getLogger(FileLogReader.class);
    protected ConcurrentHashMap<String, FileProcessor> processors = new ConcurrentHashMap<>();
    protected ExecutorService threadPool;
    protected ExecutorService kafkaProducerPool;
    protected int poolSize = Integer.valueOf(Agent.GLOBAL.getProperty(AgentConstants.AGENT_POOL_SIZE, AgentConstants.AGENT_POOL_SIZE_DEFAULT));
    protected int kafkaProducerSize = Integer.valueOf(Agent.GLOBAL.getProperty(AgentConstants.AGENT_KAFKA_PRODUCER_SIZE, AgentConstants.AGENT_KAFKA_PRODUCER_SIZE_DEFAULT));
    protected static Integer AGENT_FILE_READER_THREAD_BUFFER = Integer.valueOf(Agent.GLOBAL.getProperty(AgentConstants.AGENT_FILE_READER_THREAD_BUFFER, AgentConstants.AGENT_FILE_READER_THREAD_BUFFER_DEFAULT));
    protected static Integer AGENT_FILE_POS_STORE_OFFERS = Integer.valueOf(Agent.GLOBAL.getProperty(AgentConstants.AGENT_FILE_POS_STORE_OFFERS, AgentConstants.AGENT_FILE_POS_STORE_OFFERS_DEFAULT));
    ReadPositionStore store;

    public void handle(InputStream file, String path, Boolean first, Boolean create) throws LogReaderException {
        try {
            String topic = conf.get("id");

            FileProcessor processor = processors.get(topic + "_" + path);
            if (processor != null && !processor.isFinish())
                return;
            processor = getFileProcessor();
            processor.setPath(path);
            processor.setExecutors(processors);
            processor.setStore(store);
            processor.setFirst(first);
            processor.setMetricService(metricService);
            processor.setTopic(topic);
            String skipLine = conf.get("skipLine");

            if (skipLine != null && skipLine.matches("\\d+"))
                processor.setSkipLine(Long.valueOf(skipLine));
            processor.setCreate(create);
            processor.setBeginning("beginning".equals(conf.get("start_position")));
            Object enc = conf.get("encoding");
            String encoding;
            if (enc == null) {
                encoding = FileUtils.charset(file);
            } else {
                encoding = (enc.toString());
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(file, encoding), AGENT_FILE_READER_THREAD_BUFFER);
            processor.setLineSeparatorLength(FileUtils.lineSeparator(reader).length());
            processor.setReader(reader);
            processors.put(topic+"_"+path,processor);
            threadPool.execute(processor);
            try {
                metricService.setProcessorStatus(WorkerStatus.ConfigStatus.SUCCESS);
            } catch (MetricException e) {
                logger.error("metric service error :{}", e);
            }
        } catch (IOException e) {
            try {
                metricService.setProcessorStatus(WorkerStatus.ConfigStatus.FAIL);
            } catch (MetricException me) {
                logger.error("metric service error :{}", me);
            }
            throw new LogReaderException(" handler error", e);
        }
    }

    public abstract FileProcessor getFileProcessor();

    @Override
    public void stop() throws Exception {
        super.stop();
        for (FileProcessor processor : processors.values()) {
            processor.stop();
        }
        while (!threadPool.isTerminated()) {
            TimeUnit.SECONDS.sleep(1);
            threadPool.shutdownNow();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setup() throws LogReaderException {
        threadPool = Executors.newFixedThreadPool(poolSize);
        kafkaProducerPool=Executors.newFixedThreadPool(kafkaProducerSize);
        try {
            Class<ReadPositionStore> clazz = (Class<ReadPositionStore>) Class.forName(Agent.GLOBAL.getProperty(AgentConstants.AGENT_READ_POSITION_STORE_CLASS, AgentConstants.AGENT_READ_POSITION_STORE_CLASS_DEFAULT));
            store = clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new LogReaderException("class  FilePosStore init fail,please check you agent.properties :  agent.file.pos.class ", e);
        }
        if (!store.init())
            throw new LogReaderException("cannot init file pos storer  ");
        store.setCacheSize(AGENT_FILE_POS_STORE_OFFERS);
    }
}
