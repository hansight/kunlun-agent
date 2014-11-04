package com.hansight.kunlun.agent.collector.coordinator.config;

import java.net.ConnectException;
import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;

import com.hansight.kunlun.agent.collector.common.utils.Common;



public final class ConfigUtils {

	/**
	 * Get ZooKeeper client
	 * @param connectString ,eg:localhost:2181
	 * @param connectionTimeoutMs
	 * @param sessionTimeoutMs
	 * @param retryPolicy
	 * @return
	 */
	public static CuratorFramework getClient(String connectString,
			int connectionTimeoutMs, int sessionTimeoutMs,
			RetryPolicy retryPolicy){
		CuratorFramework client  = CuratorFrameworkFactory.builder().connectString(connectString)
				.retryPolicy(retryPolicy)
				.connectionTimeoutMs(connectionTimeoutMs)
				.sessionTimeoutMs(sessionTimeoutMs).build();
		client.start();
		return client;
	}
	
	public static void closeZK(CuratorFramework client){
		if( null !=  client){
			client.close();
			client = null;
		}
	}
	
	/**
	 * Normalization ZNode Path
	 * @param basePathTemplate,token replace template
	 * @param id,uuid
	 */
	public static String normalizationPath(String basePathTemplate,String id) {
		String path = basePathTemplate.replace(ConfigConstants.PATH_REPLACE_TOKEN, id);
		return path;
	}
	
	/**
	 * Normalization with defined token to generate ZNode Path
	 * @param basePathTemplate,token replace template
	 * @param id,uuid
	 */
	public static String normalizationPathWithToken(String token,String basePathTemplate,String id) {
		String path = basePathTemplate.replace(token, id);
		return path;
	}
	
	/**
	 * @param configIds (agentId or forwarderId)
	 * @param category (AGENT OR FORWARDER)
	 * @throws ConfigException
	 */
	public static void deleteAllByIds(List<String> configIds,ConfigCategory category) throws ConfigException,ConnectException{
		String path = "";
		if( (null == configIds) || (null == category) ){
			throw new NullPointerException();
		}else if(null != configIds && null != category){
			switch (category) {
				case AGENT:
					path = ConfigConstants.AGENT_BASE_PATH_TEMPLATE;
					deleteById(configIds,path,ConfigCategory.AGENT);
					break;
				case FORWARDER:
					path = ConfigConstants.FORWARDER_BASE_PATH_TEMPLATE;
					deleteById(configIds,path,ConfigCategory.FORWARDER);
					break;
				default:
					break;
			}
		}
	}
	
	/**
	 * @param configIds (agentId or forwarderId)
	 * @param path (agentPath or forwarderPath)
	 * @throws ConfigException
	 */
	private static void deleteById(List<String> configIds, String path, ConfigCategory category) throws ConfigException,ConnectException{
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);
		// create ZK CuratorFramework
		CuratorFramework client = getClient(Common.get(Common.ZOOKEEPER_CONNECT, "localhost:2181"),
							Integer.valueOf(Common.get(Common.ZOOKEEPER_SESSION_TIMEOUT_MS,"30000")), 
							Integer.valueOf(Common.get(Common.ZOOKEEPER_CONNECTION_TIMEOUT, "60000")),
							retryPolicy);
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {}
		if(!client.getZookeeperClient().isConnected()){
			throw new ConnectException("Zookeeper Connected Error!");
		}
		if (configIds.size() > 0) {
			for (int i = 0; i < configIds.size(); i++) {
				path = path.replace(ConfigConstants.PATH_REPLACE_TOKEN, configIds.get(i));
				try {
					/**
					    删除4个部分(正常顺序):
					  			1./kunlun/metric/ds/ds_id/agentId or forwardId
					  			3./kunlun/config/agent or forward/agent_id or forward_id
					  			4./kunlun/metric/agent/agentId
					     		                /forward/forwardId
					 */
					String afId = configIds.get(i);
					
					//1./kunlun/metric/ds/ds_id/agentId or forwardId
					String metricDsPath = ConfigConstants.KUNLUN_METRIC_DS_ROOT_PATH;
					List<String> sourceDatasouceIds = client.getChildren().forPath(metricDsPath);
					for(String dataSourceId : sourceDatasouceIds){
						String dsPath = ZKPaths.makePath(ConfigConstants.KUNLUN_METRIC_DS_ROOT_PATH, dataSourceId);
						//if datasourceId under no have agentId or forwardId then deleted
						if(0 == client.getChildren().forPath(dsPath).size()){
							client.delete().forPath(dsPath);
						}else{
							String afPath = ZKPaths.makePath(dsPath, afId);
							if(null != client.checkExists().forPath(afPath)){
								client.delete().forPath(afPath);
							}
						}
					}
					
					// 3./kunlun/config/agent or forward/agent_id or forward_id
					client.delete().deletingChildrenIfNeeded().forPath(path);
					
					//4./kunlun/metric/agent/agentId
					//   		     /forward/forwardId
					//agentId or forwardId
					if(ConfigCategory.AGENT == category){
						path = ConfigUtils.normalizationPathWithToken(ConfigConstants.PROCESSOR_UUID_REPLACE_TOKEN, ConfigConstants.KUNLUN_METRIC_AGENT_PATH, afId);
						if(null != client.checkExists().forPath(path)){
							client.delete().forPath(path);
						}
					}else if(ConfigCategory.FORWARDER == category){
						path = ConfigUtils.normalizationPathWithToken(ConfigConstants.PROCESSOR_UUID_REPLACE_TOKEN, ConfigConstants.KUNLUN_METRIC_FORWARD_PATH, afId);
						if(null != client.checkExists().forPath(path)){
							client.delete().forPath(path);
						}
					}
				} catch (Exception e) {
					if (e instanceof KeeperException.NoNodeException) {
						throw new ConfigException(path + " " + "Doesn't exist, cause " + e.getMessage());
					} else {
						throw new ConfigException("Can not delete " + path + ", cause " + e.getMessage());
					}
				}
			}
		} else {
			throw new ConfigException("Sorry, no data. Please put your agentId on List");
		}
		if(null != client){
			closeZK(client);
		}
	}
}
