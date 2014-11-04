package com.hansight.kunlun.agent.collector.kafka;

import kafka.api.OffsetRequest;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Author:zhhui
 * DateTime:2014/8/18 9:23.
 */
public class KafkaOffsetTest {
    @Test
    public void timeTest(){
        SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:ss:mm");
        System.out.println("kafka.api.OffsetRequest.LatestTime() = " +format.format(new Date(OffsetRequest.EarliestTime()))  );
    }
}
