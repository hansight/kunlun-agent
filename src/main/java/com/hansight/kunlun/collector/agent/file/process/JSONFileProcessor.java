package com.hansight.kunlun.collector.agent.file.process;

import com.hansight.kunlun.collector.common.exception.LogWriteException;
import com.hansight.kunlun.collector.common.model.Event;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class JSONFileProcessor extends FileProcessor {
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
        int openPointer = 0;
        int closePointer = 0;
        boolean quotes[] = {false, false};
        int c;
        String line;
        long i = 0;
        label:
        while ((c = reader.read()) != -1) {
            position.positionAdd(1);
            i++;
            builder.append((char) c);
            if (c == '\\') {
                c = reader.read();
                if (c != -1) {
                    builder.append((char) c);
                } else {
                    break;
                }

            }
            switch ((char) c) {
                case '\"': {
                    if (!quotes[1])
                        quotes[0] = !quotes[0];
                    break;
                }
                case '\'': {
                    if (!quotes[0])
                        quotes[1] = !quotes[1];
                    break;
                }
                case '}': {
                    if (quotes[0] || quotes[1]) {
                        break;
                    }

                    closePointer++;
                    if (openPointer == closePointer) {
                        openPointer = closePointer = 0;
                        Map<CharSequence, CharSequence> header = new LinkedHashMap<>();
                        header.put("device", getApp());
                        event = new Event();
                        event.setHeader(header);
                        line = builder.toString();
                        position.recordAdd();
                        store.set(position);
                        builder = new StringBuilder();
                        event.setBody(ByteBuffer.wrap(line.getBytes()));
                        write(event);
                    }
                    break;
                }
                case '{': {
                    if (quotes[0] || quotes[1])
                        break;
                    openPointer++;
                    break;
                }
                default: {
                    if (!running) {
                        //停止 文件处理
                        break label;
                    }
                }
            }
        }
    }

    public abstract String getApp();
}