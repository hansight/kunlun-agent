package com.hansight.kunlun.agent.collector.agent.utils;

import com.google.gson.Gson;
import com.hansight.kunlun.agent.collector.agent.Agent;
import com.hansight.kunlun.agent.collector.agent.reader.DefaultLogReader;
import com.hansight.kunlun.agent.collector.common.base.LogWriter;
import com.hansight.kunlun.agent.collector.common.model.Event;
import com.hansight.kunlun.agent.collector.common.utils.AgentConstants;
import com.hansight.kunlun.agent.collector.coordinator.config.AgentConfig;
import com.hansight.kunlun.agent.collector.coordinator.metric.MetricService;
import com.hansight.kunlun.agent.collector.mq.kafka.KafkaProducer;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snmp4j.*;
import org.snmp4j.mp.MPv1;
import org.snmp4j.mp.MPv2c;
import org.snmp4j.mp.MPv3;
import org.snmp4j.security.*;
import org.snmp4j.smi.*;
import org.snmp4j.transport.DefaultTcpTransportMapping;
import org.snmp4j.transport.DefaultUdpTransportMapping;
import org.snmp4j.util.MultiThreadedMessageDispatcher;
import org.snmp4j.util.ThreadPool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

public class SnmpUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SnmpUtils.class);
    private TransportMapping<?> transport;
    private Snmp snmp;
    private String encoding = "utf-8";
    int CACHE_SIZE = Integer.parseInt(Agent.GLOBAL.getProperty(AgentConstants.AGENT_WRITE_CACHE_SIZE, AgentConstants.AGENT_WRITE_CACHE_SIZE_DEFAULT)) + 1;

    public void launch(String protocol,final AgentConfig conf, final MetricService metricService)
            throws IOException {
        ThreadPool threadPool = ThreadPool.create("snmptrap-pool", 2);
        MessageDispatcher dispatcher = new MultiThreadedMessageDispatcher(
                threadPool, new MessageDispatcherImpl());

        encoding = DefaultLogReader.get(conf, "encoding", encoding);
        String host = DefaultLogReader.get(conf, "host", "0.0.0.0");
        String port = DefaultLogReader.get(conf, "port", "162");
        switch (protocol) {
            case "tcp":
                transport = new DefaultTcpTransportMapping(new TcpAddress(host
                        + "/" + port));
                break;
            case "udp":
                transport = new DefaultUdpTransportMapping(new UdpAddress(host
                        + "/" + port));
                break;
            default:
                throw new IOException("unexpected protocol:" + protocol);
        }
        // v3
        USM usm = null;
        boolean isV3 = "v3".equals(conf.get("version"));
        //LOG.debug("syslog protocol:{}, host:{}, port:{}, encoding:{}, v3:{}",
          //      protocol, host, port, encoding, isV3);
        if (isV3) {
            usm = new USM(
                    SecurityProtocols.getInstance().addDefaultProtocols(),
                    new OctetString(MPv3.createLocalEngineID()), 0);
            usm.setEngineDiscoveryEnabled(true);
        }
        // snmp
        Snmp snmp = new Snmp(dispatcher, transport);
        snmp.getMessageDispatcher().addMessageProcessingModel(new MPv1());
        snmp.getMessageDispatcher().addMessageProcessingModel(new MPv2c());
        // v3
        if (isV3) {
            snmp.getMessageDispatcher()
                    .addMessageProcessingModel(new MPv3(usm));
            SecurityModels.getInstance().addSecurityModel(usm);
            // configure
            String user = DefaultLogReader.get(conf, "user", "");
            String authKey = DefaultLogReader.get(conf, "auth_key", "");
            String privKey = DefaultLogReader.get(conf, "priv_key", "");
            OID auth = null;
            String authProtocol = DefaultLogReader.get(conf, "auth_protocol",
                    "");
            if ("sha".equals(authProtocol)) {
                auth = AuthSHA.ID;
            } else if ("md5".equals(authProtocol)) {
                auth = AuthMD5.ID;
            }
            OID priv = null;
            String privProtocol = DefaultLogReader.get(conf, "priv_protocol",
                    "");
            switch (privProtocol) {
                case "des":
                    priv = PrivDES.ID;
                    break;
                case "aes128":
                    priv = PrivAES128.ID;
                    break;
                case "aes192":
                    priv = PrivAES192.ID;
                    break;
                case "aes256":
                    priv = PrivAES256.ID;
                    break;
            }

            snmp.getUSM().addUser(
                    new OctetString(user),
                    new UsmUser(new OctetString(user), auth, new OctetString(
                            authKey), priv, new OctetString(privKey)));
        }
        snmp.addCommandResponder(new CommandResponder() {
            LogWriter<Event> writer = new KafkaProducer( conf.get("agent"), CACHE_SIZE);

            @Override
            public void processPdu(CommandResponderEvent snmpEvent) {
                PDU command = snmpEvent.getPDU();
                if (command != null) {
                    Vector<? extends VariableBinding> recVBs = command
                            .getVariableBindings();
                    // write to KAFKA
                    Event kafkaEvent = new Event();
                    kafkaEvent.setBody(trans(recVBs));
                    try {
                        metricService.mark();
                        writer.write(kafkaEvent);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

        });

        transport.listen();
    }

    // translate to JSON
    private ByteBuffer trans(Vector<? extends VariableBinding> recVBs) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < recVBs.size(); i++) {
            VariableBinding recVB = recVBs.elementAt(i);
            map.put(recVB.getOid().toString(), recVB.toValueString());
        }
        Gson gson = new Gson();
        String dist = gson.toJson(map);
        if (LOG.isDebugEnabled()) {
            LOG.debug(dist);
        }
        return ByteBuffer.wrap(dist.getBytes());
    }

    public void close() {
        try {
            transport.close();
            snmp.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
