package com.hansight.kunlun.collector.agent.position.store;

import com.hansight.kunlun.collector.common.model.ReadPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

public class FileReadPositionStore implements ReadPositionStore {
    final static Logger logger = LoggerFactory.getLogger(FileReadPositionStore.class);
    private static Map<String, ReadPosition> store;
    private static final String LINE_SEPARATE = "|";
    private static final String fileName = path + (path.endsWith("/") ? "" : "/") + "pos.2";
    private int offset;

    @Override
    public synchronized boolean init() {
        try {
            store = readMap(fileName);
            return true;
        } catch (IOException ex) {
            ex.printStackTrace();
            return false;
        }
    }

    @SuppressWarnings("resource")
    protected synchronized static Map<String, ReadPosition> readMap(String pathname)
            throws IOException {
        Map<String, ReadPosition> map = new ConcurrentHashMap<>();
        File file = new File(pathname);
        if (!file.exists()) {
            if (!file.createNewFile()) {
                throw new IOException("cannot create file to store");
            }
        }
        BufferedReader in = new BufferedReader(new InputStreamReader(
                new FileInputStream(pathname), "UTF-8"));
        try {
            for (String line = in.readLine(); line != null; line = in
                    .readLine()) {
                String path;
                long pos, lineNumber;
                StringTokenizer st = new StringTokenizer(line, LINE_SEPARATE);
                path = st.nextToken();
                lineNumber = Long.parseLong(st.nextToken().trim());
                pos = Long.parseLong(st.nextToken().trim());
                if (path == null || pos <= 0 || lineNumber <= 0) {
                    continue;
                }
                map.put(path, new ReadPosition(path, lineNumber, pos));
            }
        } finally {
            try {
                in.close();
            } catch (Exception ignored) {
            }
        }
        return map;
    }

    protected synchronized static void writeMap(String pathname, Map<String, ReadPosition> map)
            throws IOException {
        PrintWriter out = new PrintWriter(new OutputStreamWriter(
                new FileOutputStream(pathname), "UTF-8"));
        for (Entry<String, ReadPosition> entry : map.entrySet()) {
            out.println(entry.getKey() + LINE_SEPARATE + entry.getValue().getRecords()
                    + LINE_SEPARATE + entry.getValue().getPosition());
        }
        out.close();
    }

    @Override
    public synchronized void close() {


        try {
            flush();
            store.clear();
        } catch (IOException e) {
            logger.error("read position pos store error,when flush to file:{}", e);
        }
    }

    @Override
    public synchronized boolean set(ReadPosition readPosition) {
        store.put(readPosition.getPath(), readPosition);
        if (readPosition.getRecords() % offset == 0) {
            try {
                flush();
            } catch (IOException e) {
                logger.error("read position pos store error,when flush to file:{}", e);
                return false;
            }
        }
        return true;
    }

    @Override
    public void setCacheSize(int size) {
        this.offset = size;
    }

    @Override
    public synchronized ReadPosition get(String path) {
        return store.get(path);
    }

    @Override
    public void flush() throws IOException {
        writeMap(fileName, store);
    }
}
