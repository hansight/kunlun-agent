package com.hansight.kunlun.collector.agent.snmp;

import org.junit.Test;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Author:zhhui
 * DateTime:2014/8/26 8:49.
 */
public class FileReaderTest {
    @Test
    public void testReader() throws IOException {
        Path path= Paths.get("F:/data/csv/");
        File file=path.toFile();
        if(file.isFile()){
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            long len = 0;
            long lineLen = 0;
            lineLen = 22512;
            len = 6922957;
            System.out.println("init:{len:" + len + ",lineLen:" + lineLen + "}");
            long skipped = reader.skip(len);
            System.out.println("needSkip:" + len + ",skipped:" + skipped);
            String line;
            while ((line = reader.readLine()) != null && !"".equals(line)) {
                len += line.length();
                len++;
                lineLen++;
            }
            System.out.println("lineLen = " + lineLen);
            System.out.println("len = " + len);
            reader.close();
        }else{
            System.out.println("file is dir ");
        }

    }
}
