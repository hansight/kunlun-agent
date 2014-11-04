package com.hansight.kunlun.collector.agent.file.hdfs;

import com.hansight.kunlun.collector.agent.file.FileLogReader;
import com.hansight.kunlun.collector.common.exception.LogReaderException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Author:zhhui
 * DateTime:2014/7/29 15:55.
 * reader from need  hadoop fs -chmod 777 /user/hadoop
 */
public abstract class BaseLogReader extends FileLogReader {
    final static Logger logger = LoggerFactory.getLogger(FileLogReader.class);
    private static FileSystem fs;
    private Path path;
    public Map<Path, Monitor> monitorPaths = new ConcurrentHashMap<>();
    private boolean starting = true;

    @Override
    public void run() throws LogReaderException {
        try {
            String pathName = conf.get("uri");
            logger.debug(" hdfs path:{}", pathName);
            String[] shame = pathName.split("//");
            String[] url = shame[1].split("/");
            String name = shame[0] + "//" + url[0] + "/";
            Configuration config = new Configuration();
            URI uri = URI.create(name);

            path = new Path(pathName.substring(name.length() - 1));
            try {
                fs = FileSystem.get(uri, config);
            } catch (IOException e) {
                e.printStackTrace();
            }
            starting = true;
            monitor(path);
            starting = false;
            while (running && monitorPaths.size() > 0) {

                TimeUnit.MINUTES.sleep(10);
                for (Monitor monitor : monitorPaths.values()) {
                    monitor.check();

                }
            }
        } catch (IOException e) {
            throw new LogReaderException("handle hdfs path error", e);

        } catch (InterruptedException e) {
            throw new LogReaderException("Interrupted hdfs path error", e);
        }
    }

    /**
     * @throws com.hansight.kunlun.collector.common.exception.LogReaderException
     */
    @Override
    public void cleanup() throws LogReaderException {
        if (path != null)
            path = null;
        try {
            if (fs != null) {
                fs.close();
            }
           /* while (queue.size() > 0) {
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            throw new LogReaderException("cleanup interrupter", e);
        */
        } catch (IOException e) {
            throw new LogReaderException("cleanup fs close error", e);
        }
        logger.debug("queue stop" + conf.getId());
        // store.close();
    }

    protected void monitor(Path path) throws IOException, InterruptedException, LogReaderException {

        if (!monitorPaths.containsKey(path)) {
            Monitor monitor = new Monitor(path);
            monitorPaths.put(path, monitor);

        }
        if (fs.isFile(path)) {
            logger.info(" monitor find a new file :{}", path.getName());
            while (processors.size() > poolSize) {
                logger.info(" monitor waiting for fileProcess {} ms", 100);
                TimeUnit.MILLISECONDS.sleep(100);
            }
            if (starting) {
                handle(fs.open(path), path.toString(), true, false);
            } else {
                handle(fs.open(path), path.toString(), false, true);
            }

        } else if (fs.isDirectory(path)) {
            RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(path, true);
            while (iterator.hasNext()) {
                LocatedFileStatus status = iterator.next();
                logger.debug(" file path:{}", status.getPath());
                monitor(status.getPath());
            }
        }
    }


    class Monitor {
        private Path path;
        private long modificationTime;
        private long length;

        Monitor(Path path) throws IOException {
            this.path = path;
            FileStatus status = fs.getFileStatus(path);
            this.modificationTime = status.getModificationTime();
            this.length = status.getLen();
        }

        public void check() throws IOException, LogReaderException, InterruptedException {

            FileStatus status = fs.getFileStatus(path);
            if (status == null) {

                return;
            }

            long modify = status.getModificationTime();
            if (modify > modificationTime) {
                if (fs.isFile(path)) {
                    if (status.getLen() > length) {
                        handle(fs.open(path), path.toString(), false, false);
                    }
                }
                if (fs.isDirectory(path)) {
                    monitor(path);
                }
            }
        }
    }

    @Override
    public void stop() throws Exception {
        super.stop();
        monitorPaths.clear();
    }


}
