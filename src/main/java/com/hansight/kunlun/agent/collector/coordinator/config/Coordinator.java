package com.hansight.kunlun.agent.collector.coordinator.config;

import org.apache.curator.framework.CuratorFramework;

import java.io.Closeable;

public interface Coordinator extends Closeable{
	
	/**
	 * Use this client to process zk API call<br />
	 * Only for test, or use different {@link CuratorFramework} constructor parameters
	 * @param client
	 * @throws ConfigException
	 */
	public void setClient(CuratorFramework client) throws ConfigException;
	
	public CuratorFramework getClient() throws ConfigException;

}
