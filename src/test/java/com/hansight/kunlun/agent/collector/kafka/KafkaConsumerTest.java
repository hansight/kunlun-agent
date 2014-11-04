package com.hansight.kunlun.agent.collector.kafka;

import com.hansight.kunlun.agent.collector.common.model.Event;
import com.hansight.kunlun.agent.collector.common.serde.AvroSerde;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaConsumerTest {
	private static String topic = "test1";
	private static int partitionsNum = 1;

	public static void main(String[] args) {
		Properties props = new Properties();
		String ZOOKEEPER_CONNECT = "zookeeper.connect";
		String ZOOKEEPER_CONNECTION_TIMEOUT = "zookeeper.connection.timeout.ms";
		String GROUP_ID = "group.id";

		props.put(ZOOKEEPER_CONNECT, "bak:2181");
		props.put(ZOOKEEPER_CONNECTION_TIMEOUT, "1000000");
		props.put(GROUP_ID, "gp");
		ConsumerConfig consumerConfig = new ConsumerConfig(props);
		ConsumerConnector consumerConnector = Consumer
				.createJavaConsumerConnector(consumerConfig);

		ExecutorService executor = Executors.newFixedThreadPool(partitionsNum);
		HashMap<String, Integer> map = new HashMap<>();
		map.put(topic, partitionsNum);
		Map<String, List<KafkaStream<byte[], byte[]>>> topicMessageStreams = consumerConnector
				.createMessageStreams(map);
		List<KafkaStream<byte[], byte[]>> streams = topicMessageStreams
				.get(topic);

		// consume the messages in the threads
		for (final KafkaStream<byte[], byte[]> stream : streams) {

			executor.submit(new Runnable() {
				public void run() {
					AvroSerde serde = new AvroSerde();
					for (MessageAndMetadata<byte[], byte[]> msgAndMetadata : stream) {
						Event event;
						try {
							event = serde.deserialize(msgAndMetadata.message());
							// process message (msgAndMetadata.message())
							try {
								if (msgAndMetadata.key() != null)
									System.out.println("key:"
											+ new String(msgAndMetadata.key()));
								System.out.println("topic: "
										+ msgAndMetadata.topic());
								System.out.println("body: "
										+ new String(event.getBody().array(),
												"utf-8"));
							} catch (UnsupportedEncodingException e) {
								e.printStackTrace();
							}
						} catch (IOException e1) {
							e1.printStackTrace();
						}
					}
				}
			});
		}
	}
}
