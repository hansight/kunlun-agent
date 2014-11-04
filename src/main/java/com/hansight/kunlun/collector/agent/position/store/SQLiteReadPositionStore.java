package com.hansight.kunlun.collector.agent.position.store;

import com.hansight.kunlun.collector.common.model.ReadPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;

/**
 * Created by justinwan on 4/14/14.
 */
public class SQLiteReadPositionStore implements ReadPositionStore {
    final static Logger logger = LoggerFactory.getLogger(FileReadPositionStore.class);
    private Connection conn;

    private String db = path + (path.endsWith("/") ? "" : "/") + "pos_store.db";
    private int cacheSize = 0;
    private volatile ReadPosition position;

    public boolean init() {
        try {
            Class.forName("org.sqlite.JDBC");
            conn = DriverManager.getConnection("jdbc:sqlite:" + db);
            conn.setAutoCommit(true);

            // Check if table file_pos already exists
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT name FROM sqlite_master WHERE type='table' AND name='file_pos'");
            if (!rs.next()) {
                // If not, create a new one
                stmt.executeUpdate("CREATE TABLE file_pos (pathname TEXT PRIMARY KEY NOT NULL, pos INT8 NOT NULL,lineNumber INT8 NOT NULL)");
                stmt.close();
            }
            return true;
        } catch (Exception e) {
            logger.error("init position pos store db error:{}", e);
            return false;
        }
    }

    public void close() {
        try {
            flush();
            conn.close();
        } catch (SQLException | IOException e) {
            logger.error("close position pos store error,when flush to db:{}", e);
        }
    }

    public boolean set(ReadPosition position) {
        this.position = position;
        if (position.getRecords() % cacheSize == 0) {
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
        this.cacheSize = size;
    }

    public ReadPosition get(String pathname) {
        try {
            PreparedStatement selectStmt = conn.prepareStatement("SELECT pos,lineNumber FROM file_pos WHERE pathname=?");
            selectStmt.setString(1, pathname);
            ResultSet rs = selectStmt.executeQuery();
            if (rs.next()) {
                long pos = rs.getLong("pos");
                long lineNumber = rs.getLong("lineNumber");
                return new ReadPosition(pathname, lineNumber, pos);
            } else
                return null;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

    }


    @Override
    public synchronized void flush() throws IOException {
        try {
            PreparedStatement stmt = conn.prepareStatement("UPDATE file_pos SET pos = ?, lineNumber = ? WHERE pathname = ?");
            stmt.setString(3, position.getPath());
            stmt.setLong(2, position.getPosition());
            stmt.setLong(1, position.getRecords());
            boolean flag = stmt.executeUpdate() == 1;
            stmt.close();
            if (!flag) {
                stmt = conn.prepareStatement("INSERT INTO file_pos(pathname, pos, lineNumber) VALUES(?,?,?)");
                stmt.setString(1, position.getPath());
                stmt.setLong(2, position.getPosition());
                stmt.setLong(3, position.getRecords());
                stmt.executeUpdate();
                stmt.close();
            }
        } catch (SQLException e) {
            throw new IOException("sql error", e);
        }
    }
}
