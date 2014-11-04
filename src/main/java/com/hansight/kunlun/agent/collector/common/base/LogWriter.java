package com.hansight.kunlun.agent.collector.common.base;

import com.hansight.kunlun.agent.collector.common.exception.LogWriteException;

import java.io.Closeable;
import java.io.Flushable;

public interface LogWriter<T> extends Flushable, Closeable {
    void write(T t) throws LogWriteException;
}
