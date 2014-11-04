package com.hansight.kunlun.collector.common.serde;

import com.hansight.kunlun.collector.common.model.Event;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Author:zhhui DateTime:2014/7/29 16:09.
 *
 * @see DefaultSerde TODO @see Avro
 */
public class AvroSerde extends DefaultSerde {
    /**
     * 序列化 消息到数组
     *
     * @param event
     * @return
     * @throws IOException
     */
    @Override
    public byte[] serialize(Event event) throws IOException {
        GenericDatumWriter<Event> writer = new GenericDatumWriter<>();
        writer.setSchema(event.getSchema());
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            Encoder e = EncoderFactory.get().binaryEncoder(os, null);
            writer.write(event, e);
            e.flush();
            return os.toByteArray();
        }
    }

    /**
     * 反序列化数据
     *
     * @param body
     * @return
     * @throws IOException
     */
    @Override
    public Event deserialize(byte[] body) throws IOException {
        GenericDatumReader<Event> reader = new GenericDatumReader<>();
        reader.setSchema(new Event().getSchema());
        try (ByteArrayInputStream is = new ByteArrayInputStream(body)) {
            Decoder decoder = DecoderFactory.get().binaryDecoder(is, null);
            Event event = new Event();
            reader.read(event, decoder);
            return event;
        }


    }
}















