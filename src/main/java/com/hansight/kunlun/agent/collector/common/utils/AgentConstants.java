package com.hansight.kunlun.agent.collector.common.utils;

import com.hansight.kunlun.agent.collector.agent.position.store.FileReadPositionStore;

/**
 * Author:zhhui
 * DateTime:2014/7/29 16:05.
 */
public interface AgentConstants {
    public static final String AGENT_CONF = "agent.properties";
    public static final String AGENT_CLASS_MAP_CONF = "agent-class-mapping.properties";
    public static final String AGENT_POOL_SIZE = "agent.pool.size";
    public static final String AGENT_POOL_SIZE_DEFAULT = "10";
    public static final String AGENT_KAFKA_PRODUCER_SIZE = "agent.kafka.producer.size";
    public static final String AGENT_KAFKA_PRODUCER_SIZE_DEFAULT = "5";
    public static final String AGENT_READ_POSITION_STORE_POSITION = "agent.read.position.store.position";
    public static final String AGENT_READ_POSITION_STORE_POSITION_DEFAULT  = "./";
    public static final String AGENT_READ_POSITION_STORE_CLASS = "agent.read.position.store.class";
    public static final String AGENT_READ_POSITION_STORE_CLASS_DEFAULT = FileReadPositionStore.class.getName();

    public static final String AGENT_ID = "agent.id";
    public static final String AGENT_ID_DEFAULT = "test";
    public static final String AGENT_FILE_READER_THREAD_BUFFER = "agent.file.reader.thread.buffer";
    public static final String AGENT_FILE_READER_THREAD_BUFFER_DEFAULT = "10000";
    public static final String AGENT_FILE_POS_STORE_OFFERS = "agent.reader.pos.store.offers";
    public static final String AGENT_FILE_POS_STORE_OFFERS_DEFAULT = "5000";
    public static final String AGENT_WRITE_CACHE_SIZE = "agent.write.cache.size";
    public static final String AGENT_WRITE_CACHE_SIZE_DEFAULT = "1000";
}