package com.hansight.kunlun.agent.collector.coordinator.metric;

import java.net.ConnectException;
import java.util.List;
import java.util.Map;

public interface Metric {
	
	/**
	 * Set Agent/Forwarder config status if has error
	 * @param status
	 * @throws MetricException
	 */
	public void setProcessorStatus(WorkerStatus.ConfigStatus status) throws MetricException;
	
	/**
	 * Counter and startTimestamp, lastestTimestamp
	 * @throws MetricException
	 */
	public void mark() throws MetricException;
	
	/**
	 * Merge agents and forwarders status
	 * @return
	 * @throws MetricException
	 */
	public List<MetricMsg> getDatasourceStatus() throws MetricException,ConnectException;
	
	/**
	 * get all datasource config status
	 * @return
	 * @throws MetricException
	 */
	public List<ConfigInfo> getConfigStatus() throws MetricException,ConnectException;
	
	/**
	 * Agent/Forwarder stats
	 * @param pType
	 * @return
	 * @throws MetricException
	 */
	public List<LiveInfo> getLives(ProcessorType pType) throws MetricException,ConnectException;
	
	/**
	 * merge unique datasource configStatus
	 * @throws MetricException
	 */
	public Map<String, WorkerStatus.ConfigStatus> getDatasourceConfig() throws MetricException,ConnectException;
}
