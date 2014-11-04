package com.hansight.kunlun.agent.collector.coordinator.metric;

import java.net.ConnectException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.hansight.kunlun.agent.collector.common.utils.Common;
import com.hansight.kunlun.agent.collector.coordinator.config.ConfigConstants;
import com.hansight.kunlun.agent.collector.coordinator.config.ConfigUtils;
import com.hansight.kunlun.agent.collector.coordinator.config.CoordinatorBase;
import com.hansight.kunlun.agent.collector.coordinator.metric.WorkerStatus.CommonStatus;
import com.hansight.kunlun.agent.collector.coordinator.metric.WorkerStatus.ConfigStatus;


public abstract class MetricBase extends CoordinatorBase implements Metric{
	
	private final static Logger LOG = LoggerFactory.getLogger(MetricBase.class);
	
	protected String processorId = "UNKNOWN"; // agent or forwarder id
	
	protected ProcessorType processorType = ProcessorType.UNKNOWN;
	
	protected String datasourceId = "UNKNOWN";
	
	private long counter = 0L;
	
	private int doActionThreshold = Integer.valueOf(Common.get(Common.MARK_DOACTIONTHRESHOLD)); // when counter reach this threshold, will triger to update metric info inside zk
	
	@Override
	public void setProcessorStatus(WorkerStatus.ConfigStatus status) throws MetricException {
		try {
			Thread.sleep(80);
		} catch (InterruptedException e) {}
		if(!this.client.getZookeeperClient().isConnected()){
			LOG.error("Zookeeper Connection Error!");
			return;
		}
		try {
			String path = ConfigUtils.normalizationPathWithToken(ConfigConstants.DS_UUID_REPLACE_TOKEN, ConfigConstants.PROCESSOR_PATH_TEMPLATE, this.datasourceId);
			path = ConfigUtils.normalizationPathWithToken(ConfigConstants.PROCESSOR_UUID_REPLACE_TOKEN, path, this.processorId);
			
			String livePath = "";
			if(ProcessorType.AGENT == this.processorType){
				livePath = ConfigUtils.normalizationPathWithToken(ConfigConstants.PROCESSOR_UUID_REPLACE_TOKEN, ConfigConstants.KUNLUN_METRIC_AGENT_PATH, this.processorId);
			}else if(ProcessorType.FORWARDER == this.processorType){
				livePath = ConfigUtils.normalizationPathWithToken(ConfigConstants.PROCESSOR_UUID_REPLACE_TOKEN, ConfigConstants.KUNLUN_METRIC_FORWARD_PATH, this.processorId);
			}
			
			byte[] liveSe = null;
			LiveInfo liveDe = null;
			try {
				if(null != this.client.checkExists().forPath(livePath)){
					byte[] liveByte = this.client.getData().forPath(livePath);
					liveDe = (LiveInfo)SerializationUtils.deserialize(liveByte);
					liveDe.setCommonStatus(CommonStatus.RUNNING);
					liveSe = SerializationUtils.serialize(liveDe);
					this.client.setData().forPath(livePath, liveSe);
				}else if(null == this.client.checkExists().forPath(livePath)){
					processorNoNode(livePath);
				}
			} catch (KeeperException.NoNodeException e) {
				processorNoNode(livePath);
			}
			
			try {
				if(null != this.client.checkExists().forPath(path)){
					processorNodeExists(status, path);
				}else if(null == this.client.checkExists().forPath(path)){
					processorNoNodeByDs(path);
				}
			} catch (KeeperException.NodeExistsException e) {
				processorNodeExists(status, path);
			} catch (KeeperException.NoNodeException e){
				processorNoNodeByDs(path);
			}
			
			// save ConfigStatus path(forever): /kunlun/metric/config/ds_id/agent_id or forward_id/config_status
			path = ZKPaths.makePath(ZKPaths.makePath(ConfigConstants.KUNLUN_METRIC_DS_CONFIG_ROOT_PATH, this.datasourceId), this.processorId);
			if(null == this.client.checkExists().forPath(path)){
				ConfigInfo configInfo = new ConfigInfo();
				configInfo.setConfigStatus(status);
				configInfo.setDatasourceId(this.datasourceId);
				configInfo.setProcessorId(this.processorId);
				configInfo.setProcessorType(this.processorType);
				byte[] statusSe = SerializationUtils.serialize(configInfo);
				this.client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path,statusSe);
			}else if(null != this.client.checkExists().forPath(path)){
				byte[] cfByte = this.client.getData().forPath(path);
				ConfigInfo infoDe = (ConfigInfo)SerializationUtils.deserialize(cfByte);
				infoDe.setConfigStatus(status);
				byte[] infoByte = SerializationUtils.serialize(infoDe);
				this.client.setData().forPath(path, infoByte);
			}
		} catch (Exception e) {
			throw new MetricException("[METRIC] Can not set processor status, cause: "+ e.getMessage());
		}
		LOG.debug("Set ConfigStatus success, ConfigStatus=" + status);
	}

	private void processorNoNodeByDs(String path) throws Exception {
		WorkerStatus ws = new WorkerStatus();
		ws.setProcessorId(this.datasourceId);
		ws.setCommonStatus(CommonStatus.RUNNING);
		ws.setCurrentCounter(0);
		ws.setStartTimestamp(System.currentTimeMillis());
		ws.setProcessorType(this.processorType);
		byte[] wsSe = SerializationUtils.serialize(ws);
		this.client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, wsSe);
	}

	private void processorNoNode(String livePath) throws Exception {
		byte[] liveSe;
		LiveInfo liveDe;
		liveDe = new LiveInfo();
		liveDe.setProcessorId(this.processorId);
		liveDe.setProcessorType(this.processorType);
		liveDe.setDatasourceId(this.datasourceId);
		liveDe.setCommonStatus(CommonStatus.RUNNING);
		liveSe = SerializationUtils.serialize(liveDe);
		this.client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(livePath, liveSe);
	}

	private void processorNodeExists(WorkerStatus.ConfigStatus status,
			String path) throws Exception {
		this.client.delete().forPath(path);
		WorkerStatus ws = new WorkerStatus();
		if(WorkerStatus.ConfigStatus.SUCCESS == status){
			ws.setCommonStatus(CommonStatus.RUNNING);
		}else if(WorkerStatus.ConfigStatus.FAIL == status){
			ws.setCommonStatus(CommonStatus.STOPED);
		}
		ws.setStartTimestamp(System.currentTimeMillis());
		ws.setProcessorType(this.processorType);
		byte[] wsSe = SerializationUtils.serialize(ws);
		this.client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, wsSe);
	}

	@Override
	public void mark() throws MetricException {
		counter++;
		if(0 == counter % doActionThreshold){ //doActionThreshold put to global.properties
			try {
				Thread.sleep(80);
			} catch (InterruptedException e) {}
			if(this.client.getZookeeperClient().isConnected() == false){
				LOG.error("Zookeeper Connection Error!");
				return;
			}
			//Get workerstatus instance from zk,
			String path = ConfigUtils.normalizationPathWithToken(ConfigConstants.DS_UUID_REPLACE_TOKEN, ConfigConstants.PROCESSOR_PATH_TEMPLATE, this.datasourceId);
			path = ConfigUtils.normalizationPathWithToken(ConfigConstants.PROCESSOR_UUID_REPLACE_TOKEN, path, this.processorId);
			
			byte[] wsByte = null;
			WorkerStatus ws = new WorkerStatus();
			ws.setCommonStatus(CommonStatus.PENDING);
			ws.setProcessorId(this.processorId);
			ws.setProcessorType(this.processorType);
			try {
				if(null == this.client.checkExists().forPath(path)){
					ws.setCurrentCounter(0);
					ws.setStartTimestamp(System.currentTimeMillis());
				}else if(null != this.client.checkExists().forPath(path)){
					this.client.delete().deletingChildrenIfNeeded().forPath(path);
					ws.setCurrentCounter(counter);
					ws.setLastestAccessTimestamp(System.currentTimeMillis());
				}
				wsByte = SerializationUtils.serialize(ws);
				this.client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, wsByte);
			} catch(KeeperException.NodeExistsException e) {
				ws.setCurrentCounter(counter);
				ws.setLastestAccessTimestamp(System.currentTimeMillis());
				try {
					this.client.delete().forPath(path);
					this.client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, wsByte);
				} catch (Exception e1) {}
			} catch(KeeperException.NoNodeException e){
				try {
					ws.setCurrentCounter(0);
					ws.setStartTimestamp(System.currentTimeMillis());
					this.client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, wsByte);
				} catch (Exception e1) {}
			} catch (Exception e) {
				throw new MetricException("[METRIC] Can not get WorkerStatus, cause: " + e.getMessage());
			}
		}
	}
	
	/**
	 
		/kunlun/metric/ds/{datasource_id}/{agent_id or forward_id}
	 
	 */
	@Override
	public List<MetricMsg> getDatasourceStatus() throws MetricException,ConnectException {
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {}
		if(!this.client.getZookeeperClient().isConnected()){
			throw new ConnectException("Zookeeper Connected Error!");
		}
		List<MetricMsg> result = Lists.newArrayList();
		Set<MetricMsg> idSets = Sets.newHashSet();
		List<WorkerStatus> forwardWs = Lists.newArrayList();
		WorkerStatus agentWs = null;
		try {
			if (null != this.client.checkExists().forPath(ConfigConstants.KUNLUN_METRIC_DS_ROOT_PATH)) {
				List<String> nodes = this.client.getChildren().forPath(ConfigConstants.KUNLUN_METRIC_DS_ROOT_PATH);
				for (String dsNode : nodes) {
					String dsPaths = ZKPaths.makePath(ConfigConstants.KUNLUN_METRIC_DS_ROOT_PATH, dsNode);
					List<String> dsChildNodes = this.client.getChildren().forPath(dsPaths);
					if (0 == dsChildNodes.size()) {
						MetricMsg metric = new MetricMsg();
						metric.setCommonStatus(CommonStatus.STOPED);
						metric.setDataSourceId(dsNode);
						idSets.add(metric);
						continue;
					} else {
						for (String id : dsChildNodes) {
							String afPath = ZKPaths.makePath(dsPaths, id);
							byte[] afByte = this.client.getData().forPath(afPath);
							WorkerStatus ws = (WorkerStatus) SerializationUtils.deserialize(afByte);
							if ((null != ws)&& (ProcessorType.AGENT == ws.getProcessorType())) {
								List<LiveInfo> agentIds = getLives(ProcessorType.AGENT);
								List<ConfigInfo> configStatus = getConfigStatus();
								for (ConfigInfo ci : configStatus) {
									for (LiveInfo li : agentIds) {
										if ((ci.getDatasourceId().equals(dsNode) 
											 && ci.getProcessorId().equals(id) 
											 && (ci.getConfigStatus() == ConfigStatus.PENDING || ci.getConfigStatus() == ConfigStatus.SUCCESS))
											 && id.equals(li.getProcessorId())) {
											agentWs = ws;
										}
									}
								}
							} else if ((null != ws) && (ProcessorType.FORWARDER == ws.getProcessorType())) {
								List<LiveInfo> forwardIds = getLives(ProcessorType.FORWARDER);
								List<ConfigInfo> configStatus = getConfigStatus();
								for (ConfigInfo ci : configStatus) {
									for (LiveInfo li : forwardIds) {
										if ((ci.getDatasourceId().equals(dsNode)
												&& ci.getProcessorId().equals(id) 
												&& (ci.getConfigStatus() == ConfigStatus.PENDING || ci.getConfigStatus() == ConfigStatus.SUCCESS))
												&& id.equals(li.getProcessorId())) {
											forwardWs.add(ws);
										}
									}
								}
							}
						}
						if (null != agentWs) {
							if (forwardWs.size() > 0) {
								// if agent exists and forward exists
								for (WorkerStatus ws : forwardWs) {
									/*
									 * PENDING, // default RUNNING, STOPED,
									 * UNKNOWN
									 */
									if ((agentWs.getCommonStatus() == CommonStatus.RUNNING) && (ws.getCommonStatus() == CommonStatus.RUNNING)) {
										MetricMsg metric = new MetricMsg();
										metric.setCommonStatus(CommonStatus.RUNNING);
										metric.setDataSourceId(dsNode);
										idSets.add(metric);
										continue;
									} else if ((agentWs.getCommonStatus() == CommonStatus.PENDING) && (ws.getCommonStatus() == CommonStatus.PENDING)) {
										MetricMsg metric = new MetricMsg();
										metric.setCommonStatus(CommonStatus.PENDING);
										metric.setDataSourceId(dsNode);
										idSets.add(metric);
										continue;
									} else if ((agentWs.getCommonStatus() == CommonStatus.STOPED) && (ws.getCommonStatus() == CommonStatus.STOPED)) {
										MetricMsg metric = new MetricMsg();
										metric.setCommonStatus(CommonStatus.STOPED);
										metric.setDataSourceId(dsNode);
										idSets.add(metric);
										continue;
									} else {
										MetricMsg metric = new MetricMsg();
										metric.setCommonStatus(CommonStatus.STOPED);
										metric.setDataSourceId(dsNode);
										idSets.add(metric);
										continue;
									}
								}
							} else if (forwardWs.size() == 0) {
								// if agent exists and forward not exists must
								// be STOPED
								MetricMsg metric = new MetricMsg();
								metric.setCommonStatus(CommonStatus.STOPED);
								metric.setDataSourceId(dsNode);
								idSets.add(metric);
								continue;
							}
						} else {
							// if agent not exists must be STOPED;
							MetricMsg metric = new MetricMsg();
							metric.setCommonStatus(CommonStatus.STOPED);
							metric.setDataSourceId(dsNode);
							idSets.add(metric);
							continue;
						}
					}
				}
			}
		} catch (Exception e) {
			throw new MetricException("[METRIC] Can not get Datasource Status, cause: " + e.getMessage());
		}
		result.addAll(idSets);
		return result;
	}
	
	/**
	 
 		/kunlun/metric/config/ds_id/agent_id or forward_id/config_status
	 
	 	@param Map<String,String> : first String : dataSourceId, second String: agent_id or forward_id
	 */
	@Override
	public List<ConfigInfo> getConfigStatus() throws MetricException,ConnectException {
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {}
		if(!this.client.getZookeeperClient().isConnected()){
			throw new ConnectException("Zookeeper Connected Error!");
		}
		List<ConfigInfo> configMaps = Lists.newArrayList();
			try {
				if(null != this.client.checkExists().forPath(ConfigConstants.KUNLUN_METRIC_DS_CONFIG_ROOT_PATH)){
					List<String> nodes = this.client.getChildren().forPath(ConfigConstants.KUNLUN_METRIC_DS_CONFIG_ROOT_PATH);
					for(String dsId : nodes){
						String dsPath = ZKPaths.makePath(ConfigConstants.KUNLUN_METRIC_DS_CONFIG_ROOT_PATH, dsId);
						List<String> dsAfNodes = this.client.getChildren().forPath(dsPath);
						for(String afId : dsAfNodes){
							String afPath = ZKPaths.makePath(dsPath, afId);
							byte[] afByte = this.client.getData().forPath(afPath);
							ConfigInfo configInfo = (ConfigInfo)SerializationUtils.deserialize(afByte);
							configMaps.add(configInfo);
						}
					}
				}
			} catch (Exception e) {
				throw new MetricException("[METRIC] Can not get Config Status, cause: " + e.getMessage());
			}
		return configMaps;
	}

	/**
	 
	 	/kunlun/metric/agent or forward/agent_id or forward_id
	 
	 */
	@Deprecated
	@Override
	public List<LiveInfo> getLives(ProcessorType pType) throws MetricException,ConnectException {
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {}
		if(!this.client.getZookeeperClient().isConnected()){
			throw new ConnectException("Zookeeper Connected Error!");
		}
		List<LiveInfo> result = Lists.newArrayList();
		if(null == pType){
			throw new NullPointerException("Please set processorType!");
		}else if(null != pType){
			String path = "";
			switch(pType){
			case AGENT:
				path = ConfigUtils.normalizationPathWithToken("/" + ConfigConstants.PROCESSOR_UUID_REPLACE_TOKEN, ConfigConstants.KUNLUN_METRIC_AGENT_PATH, "");
				result = getLivesUniversal(path);
				LOG.debug("[METRIC] Agent getLives : " + result);
				break;
			case FORWARDER:
				path = ConfigUtils.normalizationPathWithToken("/" + ConfigConstants.PROCESSOR_UUID_REPLACE_TOKEN, ConfigConstants.KUNLUN_METRIC_FORWARD_PATH, "");
				result = getLivesUniversal(path);
				LOG.debug("[METRIC] Forward getLives : " + result);
				break;
			case UNKNOWN:
				LOG.info("[METRIC] getLives is UNKNOWN");
			default:
				break;
			}
		}
		return result;
	}
	
	/**
	 * get all status(agent status or forwardstatus)
	 * @param type
	 * @return
	 */
	public List<LiveInfo> getStatus(ProcessorType type) throws MetricException,ConnectException {
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {}
		if(!this.client.getZookeeperClient().isConnected()){
			throw new ConnectException("Zookeeper Connected Error!");
		}
		List<LiveInfo> infos = Lists.newArrayList();
		if(null == type){
			throw new NullPointerException("Please set processorType!");
		}else if(null != type){
			String path = ConfigConstants.KUNLUN_METRIC_DS_CONFIG_ROOT_PATH;
			try {
				if(null != this.client.checkExists().forPath(path)){
					List<LiveInfo> lives = getLives(type);
					List<String> nodes = this.client.getChildren().forPath(path);
					if(0 == lives.size()){
						for(String dsId : nodes){
							String dsPath = ZKPaths.makePath(path, dsId);
							List<String> childNodes = this.client.getChildren().forPath(dsPath);
							for(String afId : childNodes){
								String afPath = ZKPaths.makePath(dsPath, afId);
								if(null != this.client.checkExists().forPath(afPath)){
									byte[] afByte = this.client.getData().forPath(afPath);
									ConfigInfo ci = (ConfigInfo)SerializationUtils.deserialize(afByte);
									if(type == ci.getProcessorType()){
										LiveInfo li = new LiveInfo();
										li.setCommonStatus(CommonStatus.STOPED);
										li.setProcessorId(ci.getProcessorId());
										li.setProcessorType(ci.getProcessorType());
										li.setDatasourceId(ci.getDatasourceId());
										infos.add(li);
									}
								}
							}
						}
					}else{
						for(String dsId : nodes){
							String dsPath = ZKPaths.makePath(path, dsId);
							List<String> childNodes = this.client.getChildren().forPath(dsPath);
							for(String afId : childNodes){
								String afPath = ZKPaths.makePath(dsPath, afId);
								if(null != this.client.checkExists().forPath(afPath)){
									byte[] afByte = this.client.getData().forPath(afPath);
									ConfigInfo ci = (ConfigInfo)SerializationUtils.deserialize(afByte);
									if(type == ci.getProcessorType()){
										LiveInfo li = new LiveInfo();
										li.setCommonStatus(CommonStatus.STOPED);
										li.setProcessorId(ci.getProcessorId());
										li.setProcessorType(ci.getProcessorType());
										li.setDatasourceId(ci.getDatasourceId());
										infos.add(li);
									}
								}
							}
						}
						if(0 == lives.size()){
							return infos;
						}else if(0 != lives.size()){
							for(LiveInfo li : lives){
								for(LiveInfo lis : infos){
									if(li.getProcessorId().equals(lis.getProcessorId()) && li.getDatasourceId().equals(lis.getDatasourceId())){
										lis.setCommonStatus(li.getCommonStatus());
									}
								}
							}
						}
					}
				}
			} catch (Exception e) {
				throw new MetricException("[METRIC] Can not get getStatus, cause: " + e.getMessage());
			}
		}
		return infos;
	}
	
	/**
	 * @param path
	 * @param pType
	 * @return
	 * @throws MetricException
	 */
	private List<LiveInfo> getLivesUniversal(String path) throws MetricException,ConnectException {
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {}
		if(!this.client.getZookeeperClient().isConnected()){
			throw new ConnectException("Zookeeper Connected Error!");
		}
		Set<LiveInfo> idSet = Sets.newHashSet();
		List<LiveInfo> idList = Lists.newArrayList();
		try {
			if(null != this.client.checkExists().forPath(path)){
				// /kunlun/metric/agent or forward/ agent_Id or forward_id
				List<String> ids = this.client.getChildren().forPath(path);
				for(String id : ids){
					String afPath = ZKPaths.makePath(path, id);
					if(null != this.client.checkExists().forPath(afPath)){
						byte[] afByte = this.client.getData().forPath(afPath);
						LiveInfo liveInfo = (LiveInfo)SerializationUtils.deserialize(afByte);
						idSet.add(liveInfo);
					}
				}
			}
		} catch (Exception e) {
			throw new MetricException("[METRIC] Can not get liveNodes, cause: " + e.getMessage());
		}
		idList.addAll(idSet);
		return idList; 
	}

	@Override
	public Map<String, ConfigStatus> getDatasourceConfig() throws MetricException,ConnectException {
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {}
		if(!this.client.getZookeeperClient().isConnected()){
			throw new ConnectException("Zookeeper Connected Error!");
		}
		Map<String,ConfigStatus> dsConf = Maps.newHashMap();
		try {
			if(null != this.client.checkExists().forPath(ConfigConstants.KUNLUN_METRIC_DS_CONFIG_ROOT_PATH)){
				List<String> nodes = this.client.getChildren().forPath(ConfigConstants.KUNLUN_METRIC_DS_CONFIG_ROOT_PATH);
				for(String dsId : nodes){
					String dsPath = ZKPaths.makePath(ConfigConstants.KUNLUN_METRIC_DS_CONFIG_ROOT_PATH, dsId);
					List<String> dsAfNodes = this.client.getChildren().forPath(dsPath);
					ConfigInfo agentConf = null;
					List<ConfigInfo> forwarderConfs = Lists.newArrayList();
					for(String afId : dsAfNodes){
						String afPath = ZKPaths.makePath(dsPath, afId);
						byte[] afByte = this.client.getData().forPath(afPath);
						ConfigInfo configInfo = (ConfigInfo)SerializationUtils.deserialize(afByte);
						if(ProcessorType.AGENT == configInfo.getProcessorType()){
							agentConf = configInfo;
						}else if(ProcessorType.FORWARDER == configInfo.getProcessorType()){
							forwarderConfs.add(configInfo);
						}
					}
					if(null == agentConf){
						dsConf.put(dsId, WorkerStatus.ConfigStatus.FAIL);
						continue;
					}else if(0 == forwarderConfs.size()){
						dsConf.put(dsId, WorkerStatus.ConfigStatus.FAIL);
						continue;
					}else if((null != agentConf) && (forwarderConfs.size() > 0)){
						//record Fail
						int nums = 0;
						//record PENDING
						int numss = 0;
						for(ConfigInfo fCf : forwarderConfs){
							if((agentConf.getConfigStatus() == ConfigStatus.SUCCESS) && (fCf.getConfigStatus() == ConfigStatus.SUCCESS)){
								dsConf.put(dsId, WorkerStatus.ConfigStatus.SUCCESS);
								continue;
							}else if(agentConf.getConfigStatus() == ConfigStatus.PENDING){
								dsConf.put(dsId, WorkerStatus.ConfigStatus.PENDING);
								continue;
							}else if(agentConf.getConfigStatus() == ConfigStatus.FAIL){
								dsConf.put(dsId, WorkerStatus.ConfigStatus.FAIL);
								continue;
							}else if(fCf.getConfigStatus() == ConfigStatus.FAIL){
								nums++;
								continue;
							}else if(fCf.getConfigStatus() == ConfigStatus.PENDING){
								numss++;
								continue;
							}
						}
						//all forwarder ConfigStatus is : Fail
						if(nums == forwarderConfs.size()){
							dsConf.put(dsId, WorkerStatus.ConfigStatus.FAIL);
						}
						//all forwarder ConfigStatus is : PENDING
						if(numss == forwarderConfs.size()){
							dsConf.put(dsId, WorkerStatus.ConfigStatus.PENDING);
						}
					}
					continue;
				}
			}
			return dsConf;
		} catch (Exception e) {
			throw new MetricException("[Metric] can not get Datasource Config status : " + e.getMessage());
		}
	}
}