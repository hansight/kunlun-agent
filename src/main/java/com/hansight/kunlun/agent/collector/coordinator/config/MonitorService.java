package com.hansight.kunlun.agent.collector.coordinator.config;

import java.util.List;
import java.util.concurrent.ExecutorService;

public interface MonitorService<CONF extends Config> extends Runnable, Coordinator {

    public interface ConfigChangedProcessor<CONF extends Config> {

        /**
         * Only state changed config list will as input parametrs<br />
         * This interface should implements by client
         *
         * @param configList
         */
        public void process(List<CONF> configList) throws MonitorException;
    }

    /**
     * Monitor state changed
     *
     * @param service, to process {@link ConfigChangedProcessor#process(List) method}
     * @throws MonitorException
     */
    public void monitor(ExecutorService service) throws MonitorException;

    /**
     * Set ConfigChanged Processor
     *
     * @param processor
     */
    public void registerConfigChangedProcessor(ConfigChangedProcessor<CONF> processor) throws MonitorException;
}
