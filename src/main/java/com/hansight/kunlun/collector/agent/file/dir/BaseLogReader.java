package com.hansight.kunlun.collector.agent.file.dir;

import com.hansight.kunlun.collector.agent.file.FileLogReader;
import com.hansight.kunlun.collector.agent.file.process.FileProcessor;
import com.hansight.kunlun.collector.common.exception.LogReaderException;
import com.hansight.kunlun.collector.coordinator.metric.MetricException;
import com.hansight.kunlun.collector.coordinator.metric.WorkerStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardWatchEventKinds.*;

/**
 * Author:zhhui
 * DateTime:2014/7/29 15:55.
 * iis,eginx
 */
public abstract class BaseLogReader extends FileLogReader {
    final static Logger logger = LoggerFactory.getLogger(BaseLogReader.class);
    private static WatchService watcher = null;
    private boolean starting = true;
    private Path path;
    public ExecutorService monitorPool = Executors.newCachedThreadPool();
    public Map<Path, Monitor> monitorPaths = new ConcurrentHashMap<>();


    @SuppressWarnings("unchecked")
    static <T> WatchEvent<Path> cast(WatchEvent<?> event) {
        return (WatchEvent<Path>) event;
    }

    static {
        try {
            watcher = FileSystems.getDefault().newWatchService();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() throws LogReaderException {
        try {
            String pathName = conf.get("uri");
            logger.debug("dir path :{}", pathName);
            path = Paths.get(pathName);
                monitor(path);
            starting = false;
            while (running && monitorPaths.size() > 0) {
                boolean error = false;
                for (FileProcessor processor : processors.values()) {
                    if (!processor.isConfSuc()) {
                        error = true;
                        metricService.setProcessorStatus(WorkerStatus.ConfigStatus.SUCCESS);
                    }
                }
                if (!error) {
                    metricService.setProcessorStatus(WorkerStatus.ConfigStatus.SUCCESS);
                } else {
                    metricService.setProcessorStatus(WorkerStatus.ConfigStatus.FAIL);
                }
                TimeUnit.MINUTES.sleep(10);
            }
            cleanup();
        } catch (IOException e) {

            try {
                metricService.setProcessorStatus(WorkerStatus.ConfigStatus.FAIL);
            } catch (MetricException e1) {
                throw new LogReaderException(" MetricException:", e);
            }
            throw new LogReaderException(" you conf file  is not found:" + path.getFileName(), e);
        } catch (InterruptedException e) {
            throw new LogReaderException(" path monitor Interrupted:" + path.getFileName(), e);
        } catch (MetricException e) {
            throw new LogReaderException(" Metric setProcessorStatus error:" + path.getFileName(), e);
        }


    }

    /**
     * @throws com.hansight.kunlun.collector.common.exception.LogReaderException
     */
    @Override
    public void cleanup() throws LogReaderException {
        if (path != null)
            path = null;

      /*  try {
            while (queue.size() > 0) {
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (InterruptedException e) {
            throw new LogReaderException("cleanup interrupter", e);
        }*/
        logger.debug("queue stop" + conf.getId());
        // store.close();
    }


    protected void monitor(Path path) throws IOException, InterruptedException, LogReaderException {
        File file = path.toFile();

        if (file.isFile()) {
            logger.info(" monitor find a new file :{}", file.getName());
            while (processors.size() > poolSize) {
                logger.info(" monitor waiting for fileProcess {} ms", 100);
                TimeUnit.MILLISECONDS.sleep(100);
            }
            if (starting)
                handle(new BufferedInputStream(new FileInputStream(file)), file.getAbsolutePath(), true, false);
            else {
                handle(new BufferedInputStream(new FileInputStream(file)), file.getAbsolutePath(), false, true);
            }
            Path parent = path.getParent();
            Monitor monitor = monitorPaths.get(parent);
            if (monitor == null) {
                monitor = new Monitor(path.getParent(), false);
                monitorPool.execute(monitor);
                monitorPaths.put(path.getParent(), monitor);
            }
            if (!monitor.isPath()) {
                monitor.add(path);
            }
        } else if (file.isDirectory()) {
            if (!monitorPaths.containsKey(path)) {
                Monitor monitor = new Monitor(path, true);
                monitorPool.execute(monitor);
                monitorPaths.put(path, monitor);
            }
            File[] files = file.listFiles();
            if (files != null)
                for (File file1 : files) {
                    monitor(Paths.get(file1.getAbsolutePath()));
                }
        }
    }

    class Monitor implements Runnable {
        private Path path;
        private Set<Path> locations;

        private Boolean flag = true;
        private Boolean isPath = true;

        public void stop() {
            this.flag = false;
        }

        Monitor(Path path, boolean isPath) {
            this.isPath = isPath;
            this.path = path;
            if (!isPath) {
                locations = new LinkedHashSet<>();
            }
        }

        @Override
        public void run() {
            WatchKey key;
            try {
                key = path.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
            } catch (IOException e) {
                logger.error("Monitor IOException:{}", e);
                return;
            }
            while (flag) {
                List<WatchEvent<?>> events = key.pollEvents();
                if (!events.isEmpty()) {
                    for (WatchEvent<?> event : events) {
                        WatchEvent.Kind kind = event.kind();
                        if (kind .equals(OVERFLOW) ) {
                            continue;
                        }
                        // 目录监视事件的上下文是文件名
                        WatchEvent<Path> evt = cast(event);
                        Path name = evt.context();
                        Path child = path.resolve(name);
                        if (isPath() || locations.contains(child)) {
                            if (event.kind().equals(ENTRY_DELETE)) {
                                Monitor monitor = monitorPaths.remove(child);
                                if (monitor != null)
                                    monitor.stop();
                            } else if (event.kind().equals(ENTRY_CREATE)) {
                                try {
                                    monitor(child);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                } catch (InterruptedException e) {
                                    logger.error("Monitor Interrupted:{}", e);
                                    e.printStackTrace();
                                } catch (LogReaderException e) {
                                    logger.error("Monitor LogReader :{}", e);
                                }
                            } else if (event.kind().equals(ENTRY_MODIFY)) {
                                File file = child.toFile();
                                if (file.isFile()) {
                                    try {
                                        handle(new BufferedInputStream(new FileInputStream(file)), file.getAbsolutePath(), false, false);
                                    } catch (LogReaderException e) {
                                        e.printStackTrace();
                                    } catch (FileNotFoundException e) {
                                        logger.error("Monitor FileNotFound :{}", e);
                                    }
                                }

                            }
                        }

                    }
                    key.reset();
                }

            }
        }

        public boolean isPath() {
            return isPath;
        }

        public void add(Path path) {
            locations.add(path);
        }
    }

    @Override
    public void stop() throws Exception {
        super.stop();
        for (Monitor monitor : monitorPaths.values()) {
            monitor.stop();
        }
        monitorPaths.clear();
        if (watcher != null)
            watcher.close();
    }
}
