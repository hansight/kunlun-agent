package com.hansight.kunlun.collector.agent.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.zookeeper.common.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

/**
 * Author: zhhui
 * Date: 2014/9/12
 */
public class HDFSFileTest {
    String target = "hdfs://yzh:9000/logs/";
    String pathName = "/iis/";
    FileSystem fs;

    @Before
    public void setUp() throws Exception {
        Configuration config = new Configuration();
        URI uri = URI.create(target);
        fs = FileSystem.get(uri, config);
    }


    @Test
    public void testRead() throws IOException {

        Path path = new Path(pathName);
        handlerPath(path);


    }

    private void handlerPath(Path path) throws IOException {
        System.out.println("path = " + path.getName());
        if (fs.isDirectory(path)) {
            RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(path, true);
            while (iterator.hasNext()) {
                LocatedFileStatus status = iterator.next();
                System.out.println(status.getPath());
                //   System.out.println(status.getSymlink());
                System.out.println(status.getOwner());
                handlerPath(status.getPath());

            }
        } else if (fs.isFile(path)) {
            handlerFile(path);
        }
    }

    private void handlerFile(Path path) throws IOException {
        System.out.println("handle file");
        FSDataInputStream in = fs.open(path);
        BufferedReader bis = new BufferedReader(new InputStreamReader(in), 1024 * 1024);
        String line;
        try {
            int i = 0;
            while ((line = bis.readLine()) != null) {
                i++;
                System.out.println(line);
            }
            System.out.println("i = " + i);
        } finally {
            IOUtils.closeStream(in);
        }
    }

    @Test
    public void modify() throws IOException {

        String name = "iis/ex131210";
        String suf = ".log";
        int i = 1;
        FileInputStream fis = new FileInputStream(new File("F:/data/" + name + suf));//读取本地文件
        FSDataOutputStream os = fs.append(new Path(target + name + "-" + i + suf));

        BufferedReader reader = new BufferedReader(new InputStreamReader(fis), 4096);
        String line = reader.readLine();
        os.write(line.getBytes());
        os.flush();
        os.close();
        reader.close();
        fis.close();


    }

    @Test
    public void upload() throws IOException {

        String name = "iis/ex131210";
        String suf = ".log";
        int i = 1;
        FileInputStream fis = new FileInputStream(new File("F:/data/" + name + suf));//读取本地文件
        OutputStream os = fs.create(new Path(target + name + "-" + i + suf));
        //copy
        IOUtils.copyBytes(fis, os, 4096, true);
        System.out.println(name + "-" + i + suf + " copy finished ");

    }

    @Test
    public void delete() throws IOException {
        fs.delete(new Path(target + "log.log"), true);
    }

}
