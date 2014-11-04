package com.hansight.kunlun.agent.collector.agent.file.hdfs;

import com.hansight.kunlun.agent.collector.agent.file.process.FileProcessor;
import com.hansight.kunlun.agent.collector.agent.file.process.LineFileProcessor;

/**
 * Author:zhhui
 * DateTime:2014/7/31 18:33.
 */
public class LineFileLogReader extends BaseLogReader {
    @Override
    public FileProcessor getFileProcessor() {
        return new LineFileProcessor();
    }
}
