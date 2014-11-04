package com.hansight.kunlun.agent.collector.coordinator.config;

import java.net.ConnectException;
import java.util.List;


/**
 * ConfigService base
 */
public interface ConfigService<CONFIG extends Config> extends Coordinator {

    /**
     * Add Config object
     *
     * @param config
     * @throws ConfigException
     */
    public void add(CONFIG config) throws ConfigException,ConnectException;

    /**
     * Update Config object
     *
     * @param config
     * @throws ConfigException
     */
    public void update(CONFIG config) throws ConfigException,ConnectException;

    /**
     * Delete unused Config
     *
     * @param config
     * @throws ConfigException
     */
    public void delete(CONFIG config) throws ConfigException,ConnectException;

    /**
     * Query all config
     *
     * @return
     * @throws ConfigException
     */
    public List<CONFIG> queryAll() throws ConfigException,ConnectException;

}
