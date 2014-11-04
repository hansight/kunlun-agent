package com.hansight.kunlun.agent.collector.agent.file.dir;

import com.hansight.kunlun.agent.collector.agent.file.process.FileProcessor;
import com.hansight.kunlun.agent.collector.agent.file.process.WinEvtFileProcessor;

/**
 * Author:zhhui
 * DateTime:2014/7/31 18:33.
 */
public class WinEvtLogReader extends BaseLogReader {
    @Override
    public FileProcessor getFileProcessor() {
        return new WinEvtFileProcessor();
    }
}
