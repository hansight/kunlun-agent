package com.hansight.kunlun.agent.collector.agent.position.store;

import com.hansight.kunlun.agent.collector.common.model.ReadPosition;
import com.hansight.kunlun.agent.collector.coordinator.config.Coordinator;
import com.hansight.kunlun.agent.collector.coordinator.config.CoordinatorBase;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhhuiyan.
 */
public class ZookeeperReadPositionStore implements ReadPositionStore {
    protected Coordinator coordinator;
    private CuratorFramework framework;
    final static Logger logger = LoggerFactory.getLogger(ZookeeperReadPositionStore.class);
    private String store = path + (path.endsWith("/") ? "" : "/") + "position";
    private volatile ReadPosition position;
    private int cacheSize = 0;

    public boolean init() {
    	 coordinator = new CoordinatorBase() {
    	 };
        try {
            framework = coordinator.getClient();
            if (framework.checkExists().forPath(store) == null)
                framework.create().creatingParentsIfNeeded().forPath(store);
        } catch (Exception e) {
            logger.error("zookeeper init error:{}", e);
            return false;
        }
        return true;
    }

    public void close() {
        try {
            flush();
            coordinator.close();
        } catch (IOException e) {
            logger.error("zookeeper init error:{}", e);
        }
    }

    public boolean set(ReadPosition position) {
        this.position = position;
        if (position.getRecords() % cacheSize == 0) {
            try {
                flush();
            } catch (IOException e) {
                logger.error("read position pos store error,when flush to file:{}", e);
                return false;
            }
        }
        return true;
    }

    @Override
    public void setCacheSize(int size) {
        this.cacheSize = size;
    }

    public ReadPosition get(String pathname) {
        try {
            byte[] configClass = this.framework.getData().forPath(this.store+"/"+pathname);
            position = (ReadPosition) SerializationUtils.deserialize(configClass);
        } catch (Exception e) {
            logger.warn("read position pos :{}", e);
            return null;
        }
        return position;
    }


    @Override
    public void flush() throws IOException {
        byte[] bytes = SerializationUtils.serialize(position);
        try {
            if (framework.checkExists().forPath(store+"/"+position.getPath()) == null)
                framework.create().creatingParentsIfNeeded().forPath(store+"/"+position.getPath());
            this.framework.setData().forPath(store + "/" + position.getPath(), bytes);
        } catch (Exception e) {
            throw new IOException("save file pos error in zookeeper:{}", e);
        }
    }

    public static void main(String[] args) throws Exception {
        ZookeeperReadPositionStore store1=new ZookeeperReadPositionStore();
        store1.init();
        List<String> paths= store1.framework.getChildren().forPath("/kunlun/agent/read_position/agent/position");
        for(String path:paths){
            ReadPosition    position = (ReadPosition) SerializationUtils.deserialize( store1.framework.getData().forPath("/kunlun/agent/read_position/agent/position"+"/"+path));
            System.out.println("position = " + position);
        }
    }
}
