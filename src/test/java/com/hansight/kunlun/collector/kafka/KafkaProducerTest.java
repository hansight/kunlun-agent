package com.hansight.kunlun.collector.kafka;

import com.hansight.kunlun.collector.common.exception.CollectorException;

import java.io.IOException;

public class KafkaProducerTest {
	private static String topic = "test1";

	public static void main(String[] args) throws CollectorException,
			IOException, InterruptedException {
//		AvroSerde serde = new AvroSerde();
//		Producer<String, byte[]> producer = new Producer<>(new ProducerConfig());
//		while (true) {
//			Event event = new Event();
//			event.setBody(ByteBuffer.wrap("asdjas".getBytes()));
//			KeyedMessage<String, byte[]> km = new KeyedMessage<>(topic,
//					serde.serialize(event));
//			producer.send(km);
//			TimeUnit.MILLISECONDS.sleep(100);
//		}
	}
}
