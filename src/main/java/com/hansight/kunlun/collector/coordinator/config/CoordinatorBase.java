package com.hansight.kunlun.collector.coordinator.config;

import java.io.IOException;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

import com.hansight.kunlun.collector.common.utils.Common;



public abstract class CoordinatorBase implements Coordinator{
	protected CuratorFramework client = null;

	public CoordinatorBase() {
		super();
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);
		this.client = ConfigUtils.getClient(Common.get(Common.ZOOKEEPER_CONNECT, "localhost:2181"),
               Integer.valueOf(Common.get(Common.ZOOKEEPER_SESSION_TIMEOUT_MS, "30000")) ,
                Integer.valueOf( Common.get(Common.ZOOKEEPER_CONNECTION_TIMEOUT,"60000")), retryPolicy);
	}
	
	@Override
	public void setClient(CuratorFramework client) throws ConfigException {
		try {
			this.close(); // close previous CuratorFramework instance
		} catch (IOException e) {
			throw new ConfigException("Can not close zk client ,cause " + e.getMessage());
		} 
		this.client = client;
	}
	
	@Override
	public void close() throws IOException {
		if (null != this.client) {
			CloseableUtils.closeQuietly(this.client);
			this.client = null;
		}
	}

	@Override
	public CuratorFramework getClient() throws ConfigException {
		return this.client;
	}
}
