package com.hansight.kunlun.agent.collector.agent.file.dir;

import com.hansight.kunlun.agent.collector.agent.file.process.FileProcessor;
import com.hansight.kunlun.agent.collector.agent.file.process.RegexLineFileProcessor;

import java.util.regex.Pattern;

/**
 * Author:zhhui
 * DateTime:2014/7/31 18:33.
 */
public class TomcatFileLogReader extends BaseLogReader {
    @Override
    public FileProcessor getFileProcessor() {
        return new RegexLineFileProcessor() {
            private ThreadLocal<Pattern> pattern = null;

            @Override
            public boolean getFinishedInEnd() {
                return false;
            }

            @Override
            public Pattern getPattern() {
                Pattern ptn = pattern.get();
                if (ptn == null) {
                    ptn = Pattern.compile("^(\\\\d{4}-\\\\d{1,2}-\\\\d{1,2}\\\\s\\\\d{1,2}:\\\\d{1,2}:\\\\d{1,2}).*");
                    pattern.set(ptn);
                }

                return ptn;
            }
        };
    }
}
