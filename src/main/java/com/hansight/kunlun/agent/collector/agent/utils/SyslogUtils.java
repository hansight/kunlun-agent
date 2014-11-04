package com.hansight.kunlun.agent.collector.agent.utils;

import com.hansight.kunlun.agent.collector.agent.Agent;
import com.hansight.kunlun.agent.collector.agent.reader.DefaultLogReader;
import com.hansight.kunlun.agent.collector.common.base.LogWriter;
import com.hansight.kunlun.agent.collector.common.model.Event;
import com.hansight.kunlun.agent.collector.common.utils.AgentConstants;
import com.hansight.kunlun.agent.collector.coordinator.config.AgentConfig;
import com.hansight.kunlun.agent.collector.coordinator.metric.MetricService;
import com.hansight.kunlun.agent.collector.mq.kafka.KafkaProducer;



import org.productivity.java.syslog4j.server.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class SyslogUtils {
    private static final Logger LOG = LoggerFactory
            .getLogger(SyslogUtils.class);
    private SyslogServerIF syslogServer;
    private SyslogServerConfigIF syslogServerConfig;
    int CACHE_SIZE = Integer.parseInt(Agent.GLOBAL.getProperty(AgentConstants.AGENT_WRITE_CACHE_SIZE, AgentConstants.AGENT_WRITE_CACHE_SIZE_DEFAULT)) + 1;
    public synchronized void launch(String protocol, final AgentConfig conf, final MetricService metricService) {
        String host = DefaultLogReader.get(conf, "host", "0.0.0.0");
        int port = Integer.parseInt(DefaultLogReader.get(conf, "port", "514"));
        String encoding = DefaultLogReader.get(conf, "encoding", "utf-8");

        syslogServer = SyslogServer.getThreadedInstance(protocol);
        syslogServerConfig = syslogServer.getConfig();
        syslogServerConfig.setHost(host);
        syslogServerConfig.setPort(port);
        syslogServerConfig.setCharSet(encoding);
        /*LOG.debug("syslog protocol:{}, host:{}, port:{}, encoding:{}",
                protocol, host, port, encoding);*/

        final Map<CharSequence, CharSequence> header = new HashMap<>();
        header.put("asset_id", conf.get("id"));
        syslogServerConfig.addEventHandler(new SyslogServerEventHandlerIF() {
            private static final long serialVersionUID = -1334812019329301273L;
            LogWriter<Event> writer = new KafkaProducer( conf.get("agent"), CACHE_SIZE);
            @Override
            public void event(SyslogServerIF syslogServer,
                              SyslogServerEventIF syslogEvent) {
                 Event kafkaEvent = new Event();
                kafkaEvent.setHeader(header);
                kafkaEvent.setBody(ByteBuffer.wrap(getMessage(
                        syslogEvent.getMessage(), syslogEvent.getHost(),
                        syslogEvent.getCharSet())));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("syslog:{}, raw:{}", syslogEvent.getMessage(),
                            new String(syslogEvent.getRaw()));
                }
                try {
                	metricService.mark();
                    writer.write(kafkaEvent);
				} catch (Exception e) {
					e.printStackTrace();
				}
            }
        });
    }

    public byte[] getMessage(String msg, String host, String encoding) {
        if (!"utf-8".equalsIgnoreCase(encoding)) {
            try {
                msg = new String(msg.getBytes(encoding), "utf-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                return msg.getBytes();
            }
        }
        int index = msg.indexOf(host);
        if (index == -1) {
            return msg.getBytes();
        } else {
            return msg.substring(index + host.length() + 1).getBytes();
        }
    }

    public void close() {
    }

    public static void main(String[] args) throws InterruptedException {
        AgentConfig conf = new AgentConfig();
        conf.put("port", "5014");
        conf.put("id", "txx");
//        SyslogUtils syslog = new SyslogUtils();
//        syslog.launch("tcp", queue, conf, null);

    }
}
