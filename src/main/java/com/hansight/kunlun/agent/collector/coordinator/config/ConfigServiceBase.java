package com.hansight.kunlun.agent.collector.coordinator.config;

import java.net.ConnectException;
import java.util.List;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.hansight.kunlun.agent.collector.coordinator.metric.ConfigInfo;
import com.hansight.kunlun.agent.collector.coordinator.metric.LiveInfo;
import com.hansight.kunlun.agent.collector.coordinator.metric.ProcessorType;
import com.hansight.kunlun.agent.collector.coordinator.metric.WorkerStatus;
import com.hansight.kunlun.agent.collector.coordinator.metric.WorkerStatus.CommonStatus;
import com.hansight.kunlun.agent.collector.coordinator.metric.WorkerStatus.ConfigStatus;


public abstract class ConfigServiceBase<CONFIG extends Config> extends CoordinatorBase implements ConfigService<CONFIG> {
	protected final static Logger LOG = LoggerFactory
			.getLogger(ConfigServiceBase.class);

	protected String basePath = null; // znode base path,including agent or forwarder uuid
	
	public ConfigServiceBase() {
		super();
	}

	@Override
	public void add(CONFIG config) throws ConfigException,ConnectException{
		LOG.info("Add config - " + config.toString());
		store(config, Config.State.NEW);
	}

	@Override
	public void update(CONFIG config) throws ConfigException,ConnectException{
		LOG.info("Update config - " + config.toString());
		store(config, Config.State.UPDATE);
	}

	/**
	 * 删除4个部分:
	 * 			1./kunlun/config/agent or forward/agent_001 or forward_001 /datasource_0
	 * 
	 * 			2./kunlun/metric/ds/ds_id/agentId or forwardId
	 * 
	 * 			3./kunlun/metric/config/ds_id/config_status/agent_id or forward_id
	 */
	@Override
	public void delete(CONFIG config) throws ConfigException,ConnectException {
		//decide zookeeper whether success
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {}
		if(!this.client.getZookeeperClient().isConnected()){
			throw new ConnectException("Zookeeper Connected Error!");
		}
		LOG.info("Delete config - " + config.toString());
		config.setState(Config.State.DELETE);
		String path = ZKPaths.makePath(this.basePath, config.get(ConfigConstants.DATASOURCE_ID));
		try {
			//1./kunlun/config/agent or forward/agent_001 or forward_001 /datasource_0
			this.client.delete().forPath(path);
			
			String afId = ZKPaths.getPathAndNode(this.basePath).getNode();
			
			
			// 3./kunlun/metric/config/ds_id/agent_id or forward_id/config_status
			String configPath = ZKPaths.makePath(ConfigConstants.KUNLUN_METRIC_DS_CONFIG_ROOT_PATH, config.get(ConfigConstants.DATASOURCE_ID));
			String afPaths = ZKPaths.makePath(configPath, afId);
			if(null != this.client.checkExists().forPath(afPaths)){
				this.client.delete().forPath(afPaths);
			}
			if(null != this.client.checkExists().forPath(configPath)){
				List<String> configNodes = this.client.getChildren().forPath(configPath);
				if(0 == configNodes.size()){
					this.client.delete().forPath(configPath);
				}
			}
			
			
			// 2./kunlun/metric/ds/ds_id/agentId or forwardId
			path = ZKPaths.makePath(ConfigConstants.KUNLUN_METRIC_DS_ROOT_PATH, config.get(ConfigConstants.DATASOURCE_ID));
			
			afPaths = ZKPaths.makePath(path, afId);
			//no have agent_id or forward_id
			if(null != this.client.checkExists().forPath(afPaths)){
				this.client.delete().forPath(afPaths);
			}
			
			if(null != this.client.checkExists().forPath(path)){
				List<String> dsNodes = this.client.getChildren().forPath(path);
				if(0 == dsNodes.size()){
					String confPath = ZKPaths.makePath(ConfigConstants.KUNLUN_METRIC_DS_CONFIG_ROOT_PATH, config.get(ConfigConstants.DATASOURCE_ID));
					if(null != this.client.checkExists().forPath(confPath)){
						List<String> configNodes = this.client.getChildren().forPath(confPath);
						if(0 == configNodes.size()){
							this.client.delete().forPath(path);
						}
					}else{
						this.client.delete().forPath(path);
					}
				}
			}
		} catch (Exception e) {
			if(e instanceof KeeperException.NoNodeException){
				throw new ConfigException(path + "Doesn't exist, cause " + e.getMessage());
			}else{
				throw new ConfigException("Can not delete " + path + ", cause " + e.getMessage());
			}
		}
	}

	@Override
	public List<CONFIG> queryAll() throws ConfigException,ConnectException {
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {}
		if(!this.client.getZookeeperClient().isConnected()){
			throw new ConnectException("Zookeeper Connected Error!");
		}
		List<CONFIG> configs = Lists.newArrayList();
		try {
			List<String> nodes = this.client.getChildren().forPath(this.basePath);
			LOG.info("Get all configs {}",nodes.size());
			int count = 1;
			for(String node: nodes){
				byte[] configClass = this.client.getData().forPath(ZKPaths.makePath(this.basePath, node));
                CONFIG config = (CONFIG)SerializationUtils.deserialize(configClass);
				configs.add(config);
				LOG.info("Config : {}. {}",count,config.toString());
				count++;
			}
		} catch (Exception e) {
			if(e instanceof KeeperException.NoNodeException){
				try {
					this.client.create().creatingParentsIfNeeded().forPath(this.basePath);
					// first time to init basePath , this exception ignore it
				} catch (Exception e1) {
					throw new ConfigException(e1.getMessage());
				}
			}else{
				throw new ConfigException("Can not retrive configs under " + this.basePath + ",cause " + e.getMessage());
			}
		}
		return configs;
	}

	private void store(CONFIG config, Config.State state) throws ConfigException,ConnectException {
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {}
		if(!this.client.getZookeeperClient().isConnected()){
			throw new ConnectException("Zookeeper Connected Error!");
		}
		String path = ZKPaths.makePath(this.basePath, config.get(ConfigConstants.DATASOURCE_ID));
		config.setState(state);
		byte[] configClass = SerializationUtils.serialize(config);
		try {
			this.client.setData().forPath(path, configClass);
		} catch (KeeperException.NoNodeException e) {
			try {
				this.client.create().creatingParentsIfNeeded().forPath(path, configClass);
				
				// /kunlun/metric/ds/{DS_UUID}
				path = ZKPaths.makePath(ConfigConstants.KUNLUN_METRIC_DS_ROOT_PATH , config.get(ConfigConstants.DATASOURCE_ID)); 
				String processorNode = ZKPaths.getPathAndNode(this.basePath).getNode();
				// /kunlun/metric/ds/{DS_UUID}/{PROCESSOR_ID}
				path = ZKPaths.makePath(path, processorNode);
				if(null == this.client.checkExists().forPath(path)){
					WorkerStatus workerStatus = new WorkerStatus();
					workerStatus.setCommonStatus(CommonStatus.PENDING);
					workerStatus.setProcessorId(processorNode);
					workerStatus.setCurrentCounter(0L);
					
					if(config instanceof AgentConfig){
						if(null == this.client.checkExists().forPath(path)){
							workerStatus.setProcessorType(ProcessorType.AGENT);
						}
					}else if(config instanceof ForwarderConfig){
						if(null == this.client.checkExists().forPath(path)){
							workerStatus.setProcessorType(ProcessorType.FORWARDER);
						}
					}
					byte[] data = SerializationUtils.serialize(workerStatus);
					this.client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path,data);
					
					// /kunlun/metric/config/ds_id/agent_id or forward_id/config_status
					// save ConfigStatus path(forever): /kunlun/metric/config/ds_id/agent_id or forward_id/config_status
					ConfigInfo configInfo = new ConfigInfo();
					configInfo.setConfigStatus(ConfigStatus.PENDING);
					configInfo.setDatasourceId(config.get(ConfigConstants.DATASOURCE_ID));
					configInfo.setProcessorId(processorNode);
					path = ZKPaths.makePath(ZKPaths.makePath(ConfigConstants.KUNLUN_METRIC_DS_CONFIG_ROOT_PATH, config.get(ConfigConstants.DATASOURCE_ID)), processorNode);
					if(config instanceof AgentConfig){
						configInfo.setProcessorType(ProcessorType.AGENT);
					}else if(config instanceof ForwarderConfig){
						configInfo.setProcessorType(ProcessorType.FORWARDER);
					}
					if(null == this.getClient().checkExists().forPath(path)){
						byte[] statusSe = SerializationUtils.serialize(configInfo);
						this.getClient().create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path,statusSe);
					}
					
					//agent or forward save(temp)
					// /kunlun/metric/agent or forward
					LiveInfo liveInfo = new LiveInfo();
					liveInfo.setCommonStatus(CommonStatus.UNKNOWN);
					liveInfo.setProcessorId(processorNode);
					liveInfo.setDatasourceId(config.get(ConfigConstants.DATASOURCE_ID));
					String livePath = "";
					if(config instanceof AgentConfig){
						liveInfo.setProcessorType(ProcessorType.AGENT);
						livePath = ConfigUtils.normalizationPathWithToken(ConfigConstants.PROCESSOR_UUID_REPLACE_TOKEN, ConfigConstants.KUNLUN_METRIC_AGENT_PATH, processorNode);
					}else if(config instanceof ForwarderConfig){
						liveInfo.setProcessorType(ProcessorType.FORWARDER);
						livePath = ConfigUtils.normalizationPathWithToken(ConfigConstants.PROCESSOR_UUID_REPLACE_TOKEN, ConfigConstants.KUNLUN_METRIC_FORWARD_PATH, processorNode);
					}
					
					byte[] liveInfoByte = SerializationUtils.serialize(liveInfo);
					if(null == this.client.checkExists().forPath(livePath)){
						this.client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(livePath,liveInfoByte);
					}
				}
				LOG.info("You can graceful ignore 'org.apache.zookeeper.KeeperException$NoNodeException' exception, because this exception for init base znode");
			} catch (Exception e1) {
				throw new ConfigException("Can not create " + path + ", cause " + e1.getCause().getMessage());
			}
		} catch (Exception e) {
			throw new ConfigException("Can not create " + path + ", cause " + e.getCause().getMessage());
		}
	}
}
