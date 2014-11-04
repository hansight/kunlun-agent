package com.hansight.kunlun.collector.common.base;


import com.hansight.kunlun.collector.common.exception.LogReaderException;
import com.hansight.kunlun.collector.coordinator.config.AgentConfig;

import java.util.concurrent.Callable;

public interface LogReader extends Callable<Void> {

    /**
     * 初始化
     *
     * @throws com.hansight.kunlun.collector.common.exception.LogReaderException
     */
    void setup() throws LogReaderException;

    /**
     * 需要实现的方法
     *
     * @throws com.hansight.kunlun.collector.common.exception.LogReaderException
     */
    void run() throws LogReaderException;

    /**
     * 清理资源
     *
     * @throws com.hansight.kunlun.collector.common.exception.LogReaderException
     */
    void cleanup() throws LogReaderException;

    /**
     * 线程调度入口
     *
     * @return
     * @throws Exception
     */
    Void call() throws Exception;

    /**
     * 线程调度入口
     *
     * @return
     * @throws Exception
     */
    void stop() throws Exception;

    /**
     * 设置一些配置信息
     *
     * @param conf
     */
    void setConf(AgentConfig conf);

}
