package com.hansight.kunlun.agent.collector.agent.snmp;

import oi.thekraken.grok.api.exception.GrokException;
import org.junit.Assert;
import org.junit.Test;

import java.util.StringTokenizer;

/**
 * Author:zhhui
 * DateTime:2014/8/4 14:12.
 */
public class DelimitTest {
    @Test
    public void testDelimit() throws GrokException {


        String value = ",1,,";
        value=   value.replaceAll(","," , ");
        StringTokenizer tokenizer = new StringTokenizer(value, ",");
        Assert.assertEquals("must be ", 4, value.split(",").length);

    }
}
