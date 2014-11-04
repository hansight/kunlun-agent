package com.hansight.kunlun.agent.collector.coordinator.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.hansight.kunlun.agent.collector.common.utils.Common;


public abstract class MonitorServiceBase<CONFIG extends Config> extends CoordinatorBase implements MonitorService<CONFIG> {
    protected final static Logger LOG = LoggerFactory
            .getLogger(MonitorServiceBase.class);
    private final static int sleeping = Integer.valueOf(Common.get(Common.MONITOR_LOG_TIMES, "2000"));
    private final static int MONITOR_THREAD_POOL_SIZE = Integer.valueOf(Common.get(Common.MONITOR_THREAD_POOL_SIZE, "10"));

    protected String basePath; // agent or forwarder base path

    protected Map<String, CONFIG> configInMap = Maps.newHashMap(); // store agent or forwarder Config object, key is config.getId(), values is config object itself

    protected ConfigChangedProcessor processor = null; // client should implement this interface

    //protected List<Config> stateChangedConfig = Lists.newArrayList();

    protected AtomicReference<List<CONFIG>> stateChangedConfig = new AtomicReference<>(); // only stateChangedConfig will put into this list

    protected PathChildrenCache cache = null; // cache child data in local

    protected CountDownLatch ensureChangedOccurCounter = new CountDownLatch(1);

    /**
     * Default Constructor <br>
     * Init Zookeeper Connection
     */
    public MonitorServiceBase() {
        super();

    }

    @Override
    public void run() {

        ExecutorService service = Executors.newFixedThreadPool(MONITOR_THREAD_POOL_SIZE);
        this.stateChangedConfig.set(new ArrayList<CONFIG>());
        if (null == this.processor) {
            // throw new RuntimeException("Please call registerConfigChangedProcessor(ConfigChangedProcessor processor) method first before to call run method");
        }

        try {
            if (null == this.client.checkExists().forPath(this.basePath)) {
                this.client.create().creatingParentsIfNeeded().forPath(this.basePath); // ensure base path exists first
            }
            this.cache = new PathChildrenCache(this.client, this.basePath, true);
            this.cache.start();
            this.monitor(service);
            while (!Thread.interrupted()) {
                TimeUnit.MILLISECONDS.sleep(sleeping);
                LOG.debug("Monitoring config status");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != service) {
                try {
                    service.shutdown();
                    service.awaitTermination(60, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    // ignore it
                }
            }
        }

    }

    @Override
    public void registerConfigChangedProcessor(ConfigChangedProcessor processor) throws MonitorException {
        this.processor = processor;
    }


    @Override
    public void monitor(final ExecutorService service) throws MonitorException {
        PathChildrenCacheListener listener = new PathChildrenCacheListener() {

            @SuppressWarnings("incomplete-switch")
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            	if(null != event){
	            	if(null != event.getData().getData()){
	            		CONFIG config = (CONFIG) SerializationUtils.deserialize(event.getData().getData());
	            		if((null != config) && (null != event)){
	            			switch (event.getType()) {
	                        case CHILD_ADDED: {
	                            LOG.info("[MONITOR] Config added: " + ZKPaths.getNodeFromPath(event.getData().getPath()) + ",detail info " + config.toString());
	                            if (configInMap.containsKey(config.get(ConfigConstants.DATASOURCE_ID))) { // first time to start agent or forwarder
	                                config.setState(Config.State.NEW);
	                                configInMap.put(config.get(ConfigConstants.DATASOURCE_ID), config);
	                            }
	                            stateChangedConfig.get().add(config);
	                            service.submit(new ConfigChangedProcessorCallable(processor, stateChangedConfig));
	                            break;
	                        }
	                        case CHILD_UPDATED: {
	                            LOG.info("[MONITOR] Config updated: " + ZKPaths.getNodeFromPath(event.getData().getPath()) + ",detail info " + config.toString());
	
	                            stateChangedConfig.get().add(config);
	                            service.submit(new ConfigChangedProcessorCallable(processor, stateChangedConfig));
	                            break;
	                        }
	                        case CHILD_REMOVED: {
	                            LOG.info("[MONITOR] Config deleted: " + ZKPaths.getNodeFromPath(event.getData().getPath()) + ",detail info " + config.toString());
	                            configInMap.remove(config.get(ConfigConstants.DATASOURCE_ID));
	                            config.setState(Config.State.DELETE); // delete can not get config state,manual set
	                            stateChangedConfig.get().add(config);
	                            service.submit(new ConfigChangedProcessorCallable(processor, stateChangedConfig));
	                            break;
	                        }
	                	}
	            		}
	                TimeUnit.MILLISECONDS.sleep(50);
	                stateChangedConfig.get().clear();
	            	}
            	}
            }
        };
        this.cache.getListenable().addListener(listener);
    }

    /**
     * To process ConfigChangedProcessor
     */
    private static class ConfigChangedProcessorCallable<CONFIG extends Config> implements Callable<Void> {
        private ConfigChangedProcessor processor = null;
        private AtomicReference<List<CONFIG>> stateChangedConfig = null;

        public ConfigChangedProcessorCallable(ConfigChangedProcessor processor,
                                              AtomicReference<List<CONFIG>> stateChangedConfig) {
            super();
            this.processor = processor;
            this.stateChangedConfig = new AtomicReference<>();
            this.stateChangedConfig.set(stateChangedConfig.get());
        }

        @Override
        public Void call() throws Exception {
            LOG.info("[MONITOR] Ready to process ConfigChangedProcessor");
            this.processor.process(this.stateChangedConfig.get());
            LOG.info("[MONITOR] Process ConfigChangedProcessor has done");
            return null;
        }
    }
}
