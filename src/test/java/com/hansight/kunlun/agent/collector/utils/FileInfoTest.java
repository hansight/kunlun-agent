package com.hansight.kunlun.agent.collector.utils;

import org.junit.Assert;
import org.junit.Test;

import com.hansight.kunlun.agent.collector.utils.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;

/**
 * Author:zhhui
 * DateTime:2014/8/6 10:58.
 */
public class FileInfoTest {
    @Test
    public void charset() throws Exception {
        Assert.assertNotNull(
                " must be eq",
                FileUtils.simpleCharset(
                        new FileInputStream(
                                new File("F:\\workspace\\logger\\data\\编码测试.txt"))));
    }

    @Test
    public void testJUniversalChardet() throws Exception {
        Assert.assertEquals(
                " must be eq", "UTF-8",
                FileUtils.charset(
                        new FileInputStream(
                                new File("F:\\workspace\\logger\\data\\编码测试.txt"))));

    }
    @Test
    public void testLineSeparator() throws Exception {
    String s=    FileUtils.lineSeparator(new BufferedReader(new FileReader(new File("F:\\data\\iis_back\\ex131210.log"))));
        System.out.println("s = " + s.length());

    }
}
