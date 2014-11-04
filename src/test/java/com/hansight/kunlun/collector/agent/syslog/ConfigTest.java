package com.hansight.kunlun.collector.agent.syslog;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.ConnectException;
import java.util.List;
import java.util.UUID;

public class ConfigTest {
   /* AgentConfigService agent;
    ForwarderConfigService forwarder;
    String ds_uuid = "ds05";
    String agent_name = "agent";
    String forwarder_name = "forwarder";

    @Before
    public void setUp() throws Exception {
        try {
            agent = new AgentConfigService(agent_name);
            forwarder = new ForwarderConfigService(forwarder_name);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() throws Exception {
        agent.close();
        forwarder.close();
    }

    @Test
    public void deleteTopic() throws ConfigException, ConnectException {
        List<ForwarderConfig> forwards = forwarder.queryAll();
        for (ForwarderConfig config : forwards) {
            System.out.println("forwarder topic: " + config.get("id"));
            forwarder.delete(config);
        }
        List<AgentConfig> agents = agent.queryAll();
        for (AgentConfig config : agents) {
            System.out.println("agent topic:" + config.get("id"));
            agent.delete(config);
        }
    }

    @Test
    public void add() throws ConfigException, ConnectException {
        deleteTopic();
        ds_uuid = UUID.randomUUID().toString().replace("-", "");
        AgentConfig agent = new AgentConfig();
        agent.put(ConfigConstants.DATASOURCE_ID, ds_uuid);
        agent.put("category", "default");
        agent.put("uri", "F:\\data\\iis_back");
        agent.put("start_position", "end");
        // config.put("encoding", "GB2312");
        this.agent.add(agent);
        ForwarderConfig forwarder = new ForwarderConfig();
        forwarder.put(ConfigConstants.DATASOURCE_ID, ds_uuid);
        forwarder.put("parser", "regex");
        forwarder.put("type", "log");
        forwarder.put("category", "test");
        forwarder.put("pattern", "%{IIS_LOG}");
        this.forwarder.add(forwarder);
    }


    @Test
    public void forwarderQuery() throws ConfigException, ConnectException {
        List<ForwarderConfig> configs = forwarder.queryAll();
        for (ForwarderConfig config : configs) {
            System.out.println("forwarder topic: " + config.get("id"));
        }
    }

    @Test
    public void agentQuery() throws ConfigException, ConnectException {
        List<AgentConfig> configs = agent.queryAll();
        for (AgentConfig config : configs) {
            System.out.println("agent topic:" + config.get("id"));
            System.out.println("agent topic:" + config.get("start_position"));
        }
    }

    @Test
    public void agentAdd() throws ConfigException, ConnectException {
        AgentConfig config = new AgentConfig();
        config.put(ConfigConstants.DATASOURCE_ID, ds_uuid);
        config.put("category", "hdfs-line");
        config.put("uri", "hdfs://yzh:9000/");
        config.put("start_position", "end");
        config.put("skipLine", "0");
        // config.put("encoding", "GB2312");
        agent.add(config);
    }

    @Test
    public void forwarderAdd() throws ConfigException, ConnectException {
        ForwarderConfig config = new ForwarderConfig();
        config.put(ConfigConstants.DATASOURCE_ID, ds_uuid);
        
         asset.setParser("delimit");
        asset.setProtocol("file");
        asset.setCategory("default");
        asset.setType("event");
          asset.setPattern("{separate:'\\t',fields:['#', '@timestamp', 'generated_date' , 'product_entity' , 'product' , 'product_ip', 'product_mac', 'management_server', 'malware', 'endpoint' , 's_host' , 'user_name' , 'handling', 'results' , 'detections' , 'entry_type' , 'details' ]}");
         asset.setPattern("{separate:',',fields:['@timestamp','protocol_group','protocol','vlan_id','direction','d_ip','d_port','d_mac','s_ip','s_port','s_mac','domain_name','host_name','infect_name','risk_type_group','risk_type','file_name','file_ext_name','file_true_type','file_size','rule_id','desc','confidence','email_receiver','email_sender','topic','bot_command','bot_url','channel_name','nick_name','url','user_name','authentication','user_agent','target_share','detector','potential_risk','has_q_file','q_file_path','file_name_in_arc','restrict_type','d_host','s_host','task_id','severity','s_group','s_zone','d_group','d_zone','detection_type','blocked','dce_hash1','dce_hash2','src_user_name1','src_user_login_time1','src_user_name2','src_user_login_time2','src_user_name3','src_user_login_time3','dst_user_name1','dst_user_login_time1','dst_user_name2','dst_user_login_time2','dst_user_name3','dst_user_login_time3','dst']}");

         

        config.put("parser", "delimit");
        config.put("type", "event");
        config.put("category", "TDA");
        config.put("pattern", "{separate:',',fields:['@timestamp','protocol_group','protocol','vlan_id','direction','d_ip','d_port','d_mac','s_ip','s_port','s_mac','domain_name','host_name','infect_name','risk_type_group','risk_type','file_name','file_ext_name','file_true_type','file_size','rule_id','desc','confidence','email_receiver','email_sender','topic','bot_command','bot_url','channel_name','nick_name','url','user_name','authentication','user_agent','target_share','detector','potential_risk','has_q_file','q_file_path','file_name_in_arc','restrict_type','d_host','s_host','task_id','severity','s_group','s_zone','d_group','d_zone','detection_type','blocked','dce_hash1','dce_hash2','src_user_name1','src_user_login_time1','src_user_name2','src_user_login_time2','src_user_name3','src_user_login_time3','dst_user_name1','dst_user_login_time1','dst_user_name2','dst_user_login_time2','dst_user_name3','dst_user_login_time3','dst']}");
        forwarder.add(config);
    }

    @Test
    public void agentUpdate() throws ConfigException, ConnectException {
        AgentConfig agent = new AgentConfig();
        agent.put(ConfigConstants.DATASOURCE_ID, ds_uuid);
        agent.put("category", "default");
        agent.put("uri", "F:\\workspace\\logger\\data\\csv\\Firewall_Events200.csv");
        agent.put("start_position", "beginning");
        this.agent.update(agent);
    }

    @Test
    public void agentDelete() throws ConfigException, ConnectException {
        AgentConfig config = new AgentConfig();
        config.put(ConfigConstants.DATASOURCE_ID, ds_uuid);
        config.put("category", "default");
        //  config.put("id", "mJr4u9pTRiWhe8-zjcZklA");
        config.put("uri", "F:/workspace/logger/data/csv/AdHoc_2014_04_01.csv");
        config.put("start_position", "beginning");
        agent.delete(config);
    }


    @Test
    public void forwarderUpdate() throws ConfigException, ConnectException {
        ForwarderConfig forwarder = new ForwarderConfig();
        forwarder.put(ConfigConstants.DATASOURCE_ID, ds_uuid);
        forwarder.put("parser", "delimit");
        forwarder.put("type", "event");
        forwarder.put("category", "Firewall");
        forwarder.put("pattern", "{separate:',',fields:['@timestamp','date_us','host','reason','tag','operation','sort','direction','iface','frame_type','protocol','flag','s_ip','s_mac','s_port','d_ip','d_mac','d_port','package_size','repeat_count','end_time','stream','status','notice','data_flag','data_index','data']}");
        this.forwarder.update(forwarder);
    }

    @Test
    public void forwarderDelete() throws ConfigException, ConnectException {
        ForwarderConfig config = new ForwarderConfig();
        config.put(ConfigConstants.DATASOURCE_ID, ds_uuid);
        
         asset.setParser("delimit");
        asset.setProtocol("file");
        asset.setCategory("default");
        asset.setType("event");
          asset.setPattern("{separate:'\\t',fields:['#', '@timestamp', 'generated_date' , 'product_entity' , 'product' , 'product_ip', 'product_mac', 'management_server', 'malware', 'endpoint' , 's_host' , 'user_name' , 'handling', 'results' , 'detections' , 'entry_type' , 'details' ]}");
         asset.setPattern("{separate:',',fields:['@timestamp','protocol_group','protocol','vlan_id','direction','d_ip','d_port','d_mac','s_ip','s_port','s_mac','domain_name','host_name','infect_name','risk_type_group','risk_type','file_name','file_ext_name','file_true_type','file_size','rule_id','desc','confidence','email_receiver','email_sender','topic','bot_command','bot_url','channel_name','nick_name','url','user_name','authentication','user_agent','target_share','detector','potential_risk','has_q_file','q_file_path','file_name_in_arc','restrict_type','d_host','s_host','task_id','severity','s_group','s_zone','d_group','d_zone','detection_type','blocked','dce_hash1','dce_hash2','src_user_name1','src_user_login_time1','src_user_name2','src_user_login_time2','src_user_name3','src_user_login_time3','dst_user_name1','dst_user_login_time1','dst_user_name2','dst_user_login_time2','dst_user_name3','dst_user_login_time3','dst']}");

         
        //  config.put("id", "mJr4u9pTRiWhe8-zjcZklA");
        config.put("parser", "delimit");
        config.put("type", "event");
        config.put("category", "TDA");
        config.put("pattern", "{separate:',',fields:['@timestamp','protocol_group','protocol','vlan_id','direction','d_ip','d_port','d_mac','s_ip','s_port','s_mac','domain_name','host_name','infect_name','risk_type_group','risk_type','file_name','file_ext_name','file_true_type','file_size','rule_id','desc','confidence','email_receiver','email_sender','topic','bot_command','bot_url','channel_name','nick_name','url','user_name','authentication','user_agent','target_share','detector','potential_risk','has_q_file','q_file_path','file_name_in_arc','restrict_type','d_host','s_host','task_id','severity','s_group','s_zone','d_group','d_zone','detection_type','blocked','dce_hash1','dce_hash2','src_user_name1','src_user_login_time1','src_user_name2','src_user_login_time2','src_user_name3','src_user_login_time3','dst_user_name1','dst_user_login_time1','dst_user_name2','dst_user_login_time2','dst_user_name3','dst_user_login_time3','dst']}");
        forwarder.delete(config);
    }*/
}