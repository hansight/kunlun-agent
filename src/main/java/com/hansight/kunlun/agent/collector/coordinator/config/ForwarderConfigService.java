package com.hansight.kunlun.agent.collector.coordinator.config;



public class ForwarderConfigService extends ConfigServiceBase<ForwarderConfig> {

    /**
     * Default constructor
     *
     * @param forwarderId, forwarder component id that construct znode path
     */
    public ForwarderConfigService(String forwarderId) {
        super();
        this.basePath = ConfigUtils.normalizationPath(ConfigConstants.FORWARDER_BASE_PATH_TEMPLATE, forwarderId);
    }

}
