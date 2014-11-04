package com.hansight.kunlun.agent.collector.agent.file.process;

import com.hansight.kunlun.agent.collector.common.exception.LogWriteException;
import com.hansight.kunlun.agent.collector.common.model.Event;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class DelimiterFileProcessor extends FileProcessor {
    @Override
    public void process() throws IOException, LogWriteException {
        if (!(first && beginning)) {
            long skipped = reader.skip(position.getPosition());
            if (skipped < position.getPosition()) {
                position.setPosition(skipped);
                position.setRecords(0l);
                return;
            }
        }
        StringBuilder builder = new StringBuilder();
        Event event;
        int c = reader.read();
        String line;
        long i = 0;
        while (c != -1) {
            position.positionAdd(1);
            i++;
            builder.append((char) c);
            line = builder.toString();
            if (line.endsWith(getDelimiter())) {
                line = line.replace(getDelimiter(), "");
                Map<CharSequence, CharSequence> header = new LinkedHashMap<>();
                header.put("device", getApp());
                event = new Event();
                event.setHeader(header);
                event.setBody(ByteBuffer.wrap(line.getBytes()));
                write(event);
                position.recordAdd();
                store.set(position);
                builder = new StringBuilder();
                if (!running) {
                    //停止 文件处理
                    break;
                }
            }
        }
    }

    public abstract String getApp();

    public abstract String getDelimiter();
}