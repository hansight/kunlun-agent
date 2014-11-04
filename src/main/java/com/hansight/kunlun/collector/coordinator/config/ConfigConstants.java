package com.hansight.kunlun.collector.coordinator.config;

import java.util.UUID;


public final class ConfigConstants {

    public final static String KUNLUN_CONFIG_ROOT_PATH = "/kunlun/config";

    public final static String AGENT_BASE_PATH_TEMPLATE = KUNLUN_CONFIG_ROOT_PATH + "/agent/{UUID}";

    public final static String FORWARDER_BASE_PATH_TEMPLATE = KUNLUN_CONFIG_ROOT_PATH + "/forwarder/{UUID}";

    public final static String PATH_REPLACE_TOKEN = "{UUID}";

    public final static String DATASOURCE_ID = "id";
    
    public final static String KUNLUN_METRIC_DS_ROOT_PATH = "/kunlun/metric/ds";
    
    public final static String KUNLUN_METRIC_ROOT_PATH = "/kunlun/metric";
    
    public final static String KUNLUN_METRIC_DS_CONFIG_ROOT_PATH = KUNLUN_METRIC_ROOT_PATH + "/config"; 
    
    public final static String KUNLUN_METRIC_AGENT_PATH = KUNLUN_METRIC_ROOT_PATH + "/agent/{PROCESSOR_UUID}";
    
    public final static String KUNLUN_METRIC_FORWARD_PATH = KUNLUN_METRIC_ROOT_PATH + "/forward/{PROCESSOR_UUID}";
    
    public final static String DS_UUID_REPLACE_TOKEN = "{DS_UUID}";
    
    public final static String PROCESSOR_UUID_REPLACE_TOKEN = "{PROCESSOR_UUID}"; // agent or forward

    public final static String PROCESSOR_PATH_TEMPLATE = KUNLUN_METRIC_DS_ROOT_PATH + "/{DS_UUID}/{PROCESSOR_UUID}";
    
    public static void main(String[] args) {
        String a = AGENT_BASE_PATH_TEMPLATE.replace(PATH_REPLACE_TOKEN, UUID.randomUUID().toString());
        System.out.println(a);
    }
}
