package com.hansight.kunlun.collector.agent.file.hdfs;

import com.hansight.kunlun.collector.agent.file.process.FileProcessor;
import com.hansight.kunlun.collector.agent.file.process.JSONFileProcessor;

/**
 * Author:zhhui
 * DateTime:2014/7/31 18:33.
 */
public class JSONFileLogReader extends BaseLogReader {

    @Override
    public FileProcessor getFileProcessor() {
        return new JSONFileProcessor() {
            @Override
            public String getApp() {
                return "default json";
            }
        };
    }
}
