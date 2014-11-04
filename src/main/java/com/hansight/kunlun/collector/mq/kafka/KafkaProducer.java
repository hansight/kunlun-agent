package com.hansight.kunlun.collector.mq.kafka;

import com.hansight.kunlun.collector.common.base.LogWriter;
import com.hansight.kunlun.collector.common.exception.LogWriteException;
import com.hansight.kunlun.collector.common.model.Event;
import com.hansight.kunlun.collector.common.serde.AvroSerde;
import com.hansight.kunlun.collector.common.utils.Common;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("serial")
public class KafkaProducer implements LogWriter<Event>, Serializable {

    protected ExecutorService threadPool = Executors.newFixedThreadPool(5);
    private static List<Executor> executors = new LinkedList<>();
    private List<KeyedMessage<String, byte[]>> cache;
    private int cacheSize = 1;
    private long times = 0;
    protected final static Logger logger = LoggerFactory.getLogger(KafkaProducer.class);
    private String topic;
    private final AvroSerde serde = new AvroSerde();

    public void write(Event event) throws LogWriteException {
        try {
            cache(new KeyedMessage<String, byte[]>(
                    topic, serde.serialize(event)));
        } catch (IOException e) {
            throw new LogWriteException("serialize event error", e);
        }

    }

    public boolean cache(KeyedMessage<String, byte[]> stringKeyedMessage) {
        cache.add(stringKeyedMessage);
        if (cache.size() == cacheSize) {
            logger.debug("cache {} take time {} ms", cache.size(), System.currentTimeMillis() - times);
            this.flush();
            if (logger.isDebugEnabled()) {
                times = System.currentTimeMillis();
            }
        }
        return true;
    }

    public void flush() {
    	if (cache.size() == 0) {
    		return;
    	}
        try {
            long start = 0;
            if (logger.isDebugEnabled()) {
                start = System.currentTimeMillis();
            }
            Executor executor = get();
            executor.setCache(cache);
            threadPool.execute(executor);
            logger.debug("send  {} to kafka take time {} ms", cache.size(), System.currentTimeMillis() - start);
            cache = new ArrayList<>(cacheSize);
        } catch (InterruptedException e) {
            logger.error("send to kafka Interrupted", e);
        }
    }

    @SuppressWarnings("unchecked")
    private Executor get() throws InterruptedException {
        while (true) {
            for (Executor temp : executors) {
                if (!temp.isRunning()) {
                    temp.start();
                    return temp;
                }
            }
            if (executors.size() < 5) {
                Executor executor = new Executor();
                executor.start();
                executors.add(executor);
                return executor;
            }
            logger.warn("Data sent to the Kafka too slow, wait for  {} ms", 500);
            TimeUnit.MILLISECONDS.sleep(500);
        }
    }

    public KafkaProducer(String topic, int cacheSize) {
        this.topic = topic;
        cache = new ArrayList<>(cacheSize);
        this.cacheSize = cacheSize;

    }

    class Executor implements Runnable, Closeable {
        private List<KeyedMessage<String, byte[]>> cache;
        private Producer<String, byte[]> producer;
        private boolean running = true;

        Executor() {
            producer = new Producer<>(new ProducerConfig(Common.getAll()));
        }

        @Override
        public void run() {
            long start = 0;
            running = true;
            if (logger.isDebugEnabled()) {
                start = System.currentTimeMillis();
            }
            producer.send(cache);
            running = false;
            logger.debug("send  {} data to kafka take {} ms", cache.size(), System.currentTimeMillis() - start);
        }

        public void setCache(List<KeyedMessage<String, byte[]>> cache) {
            this.cache = cache;
        }

        @Override
        public void close() throws IOException {
            producer.close();
        }

        public boolean isRunning() {
            return running;
        }

        public void start() {
            this.running = true;
        }
    }

    @Override
    public void close() {
        if (cache.size() > 0)
            flush();
        while (executors.size() > 0) {
            for (int i = executors.size() - 1; i >= 0; i--) {
                Executor temp = executors.get(i);
                if (!temp.isRunning()) {
                    try {
                        temp.close();
                        executors.remove(i);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
    }
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(10, TimeUnit.MILLISECONDS)) {
                threadPool.shutdownNow();
                if (!threadPool.awaitTermination(10, TimeUnit.MILLISECONDS)) {
                    logger.warn("Pool did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted.
            threadPool.shutdownNow();
            // Preserve interrupt status.
            Thread.currentThread().interrupt();
        }
    }
}