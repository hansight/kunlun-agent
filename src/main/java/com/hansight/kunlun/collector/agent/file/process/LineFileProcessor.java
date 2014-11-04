package com.hansight.kunlun.collector.agent.file.process;

import com.hansight.kunlun.collector.common.exception.LogWriteException;
import com.hansight.kunlun.collector.common.model.Event;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

public class LineFileProcessor extends FileProcessor {

    public boolean skip() {
        return true;
    }

    @Override
    public void process() throws IOException, InterruptedException, LogWriteException {
        //文件读取位置
        if (!(first && beginning)) {
            long skipped = reader.skip(position.getPosition());
            logger.info("need skip:{} skipped:{}", position.getPosition(), skipped);
            if (skipped < position.getPosition()) {
             //   position.setPosition(skipped);
                //   position.setRecords(0l);
                return;
            }
        } else {
            position.setPosition(0l);
            position.setRecords(0l);
        }

        processWithLine();
    }

    public void processWithLine() throws IOException, InterruptedException, LogWriteException {
        String line;
        while (( line = reader.readLine()) != null && !"".equals(line)) {
            position.recordAdd();
            position.positionAdd((long) line.length() + lineSeparatorLength);
            if (position.getRecords() < skipLine) {
                continue;
            }
            store.set(position);
            save(line);
            if (!running) {
                break;
            }
        }
    }

    public void save(String line) throws LogWriteException {
        if (line == null || "".equals(line))
            return;
        //  logger.debug("line:{}", line);
        Event event = new Event();
        Map<CharSequence, CharSequence> header = new LinkedHashMap<>();
        event.setHeader(header);
        event.setBody(ByteBuffer.wrap(line.getBytes()));
        logger.debug(line);
        write(event);
    }
}