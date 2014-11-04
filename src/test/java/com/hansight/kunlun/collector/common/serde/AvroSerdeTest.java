package com.hansight.kunlun.collector.common.serde;

import com.hansight.kunlun.collector.common.model.Event;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.Map;

/**
 * Author:zhhui
 * DateTime:2014/8/4 16:49.
 */
public class AvroSerdeTest {
    @Test
    public void serialize() throws IOException {
        Event event = new Event();
        Map<CharSequence, CharSequence> header = new Hashtable<>();
        header.put("server", "123");
        event.setHeader(header);
        event.setBody(ByteBuffer.wrap("一二三四五，上山打老虎".getBytes("UTF-8")));
        Serde<Event> serde = new AvroSerde();
        byte[] data = serde.serialize(event);
        System.out.println("data = " + data);
        event = serde.deserialize(data);
        System.out.println("event = " + event.getHeader());
        System.out.println("event = " + new String(event.getBody().array(), "UTF-8"));

    }


}
