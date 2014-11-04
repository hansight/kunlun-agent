package com.hansight.kunlun.collector.common.exception;

/**
 * Author:zhhui
 * DateTime:2014/7/30 9:50.
 */
public class LogWriteException extends CollectorException {
    public LogWriteException(String message) {
        super(message);
    }

    public LogWriteException(String message, Throwable cause) {
        super(message, cause);
    }
}
