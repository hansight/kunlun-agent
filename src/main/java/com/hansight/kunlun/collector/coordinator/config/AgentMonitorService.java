package com.hansight.kunlun.collector.coordinator.config;

public class AgentMonitorService extends MonitorServiceBase<AgentConfig> {

    /**
     * Default constructor
     *
     * @param agentId, agent component id that construct znode path
     */
    public AgentMonitorService(String agentId) {
        super();
        this.basePath = ConfigUtils.normalizationPath(ConfigConstants.AGENT_BASE_PATH_TEMPLATE, agentId);
    }
}
