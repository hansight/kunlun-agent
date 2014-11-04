package com.hansight.kunlun.collector.coordinator.config;

public class MonitorServiceImpl<CONFIG extends Config> extends MonitorServiceBase<CONFIG>{
	
	/**
	 * Default constructor
	 * @param agentOrForwarderId, agentOrForwarderId component id that construct znode path
	 */
	public MonitorServiceImpl(CONFIG config,String agentOrForwarderId){
		super();
		if( config  instanceof AgentConfig){
			this.basePath = ConfigUtils.normalizationPath(ConfigConstants.AGENT_BASE_PATH_TEMPLATE,agentOrForwarderId);
		}else if(config instanceof ForwarderConfig){
			this.basePath = ConfigUtils.normalizationPath(ConfigConstants.FORWARDER_BASE_PATH_TEMPLATE,agentOrForwarderId);
		}
	}
}
