package com.hansight.kunlun.collector.agent.position.store;

import com.hansight.kunlun.collector.agent.Agent;
import com.hansight.kunlun.collector.common.model.ReadPosition;
import com.hansight.kunlun.collector.common.utils.AgentConstants;

import java.io.Closeable;
import java.io.Flushable;

public interface ReadPositionStore extends Flushable, Closeable {
    final String path = Agent.GLOBAL.getProperty(AgentConstants.AGENT_READ_POSITION_STORE_POSITION, AgentConstants.AGENT_READ_POSITION_STORE_POSITION_DEFAULT);

    public boolean init();

    public boolean set(ReadPosition readPosition);

    public void setCacheSize(int size);

    /**
     * @param path
     * @return [0] lineNumber , [1] pos
     */
    public ReadPosition get(String path);

}
