package com.hansight.kunlun.agent.collector.agent.file.process;

import com.hansight.kunlun.agent.collector.common.exception.LogWriteException;
import com.hansight.kunlun.agent.collector.common.model.Event;
import com.hansight.kunlun.agent.collector.common.model.WinEvt;
import com.sun.jna.platform.win32.WinDef;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.hansight.kunlun.agent.collector.common.model.WinEvt.*;

public class WinEvtFileProcessor extends FileProcessor {
    static final WinEvt evt = INSTANCE;

    @Override
    public void process() throws IOException, LogWriteException {
        EVT_HANDLE handle = evt.EvtQuery(null, path, null, EvtQueryFilePath);
        int size = 10;
        EVT_HANDLE[] events = new EVT_HANDLE[size];
        WinDef.DWORD[] returned = new WinDef.DWORD[size];
        long index = 0;
        if (!(first && beginning)) {
            while (evt.EvtNext(handle, size, events, Integer.MAX_VALUE, 0, returned)) {
                index++;
                if (index >= position.getRecords()) {
                    break;
                }
            }
            if (index < position.getRecords()) {
                position.setRecords(index);
                if (index == 0)
                    position.setPosition(0l);
                else
                    position.setPosition((long) returned.length);
                return;
            }
        }

        if (position.getPosition() > returned.length) {
            position.setPosition((long) returned.length);
        } else {
            for (int j = 0; j < returned.length; j++) {
                if (position.getPosition() > j) {
                    continue;
                }
                Event event = new Event();
                Map<CharSequence, CharSequence> header = new LinkedHashMap<>();
                // header.put("device", getApp());
                event.setHeader(header);
                byte[] value = eventContent(events[j]);
                evt.EvtClose(events[j]);
                logger.debug("agent event:{} ", new String(value));
                if (!"".equals(new String(value))) {
                    event.setBody(ByteBuffer.wrap(value));
                    write(event);
                    position.positionAdd(1);
                }
            }
        }
        store.set(position);
        label:
        while (evt.EvtNext(handle, size, events, Integer.MAX_VALUE, 0, returned)) {
            position.recordAdd();
            position.setPosition(0l);
            if (!running) {
                return;
            }
            for (int j = 0; j < returned.length; j++) {
                Event event = new Event();
                Map<CharSequence, CharSequence> header = new LinkedHashMap<>();
                // header.put("device", getApp());
                event.setHeader(header);
                byte[] value = eventContent(events[j]);
                evt.EvtClose(events[j]);
                logger.debug("agent event:{} ", new String(value));
                event.setBody(ByteBuffer.wrap(value));

                write(event);
                position.recordAdd();
                position.positionAdd(1);
                if (!running) {
                    break label;
                }
            }
            store.set(position);
        }

        evt.EvtClose(handle);
      }

    // get the event as an XML string in byte[].
    byte[] eventContent(EVT_HANDLE events) {
        WinDef.DWORD bufferSize = new WinDef.DWORD();
        WinDef.DWORDByReference usedBuffer = new WinDef.DWORDByReference();
        WinDef.DWORDByReference propertyCount = new WinDef.DWORDByReference();
        boolean flag = evt.EvtRender(null, events, EvtRenderEventXml, bufferSize, null, usedBuffer, propertyCount);
        if (!flag) {
            bufferSize = usedBuffer.getValue();
            byte[] content = new byte[bufferSize.intValue()];
            evt.EvtRender(null, events, EvtRenderEventXml, bufferSize, content, usedBuffer, propertyCount);
            byte[] values = new byte[content.length / 2];
            for (int i = 0; i < values.length; i++) {
                values[i] = content[2 * i];
            }
            return values;
        }
        return null;
    }
}