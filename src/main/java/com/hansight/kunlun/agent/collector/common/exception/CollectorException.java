package com.hansight.kunlun.agent.collector.common.exception;

/**
 * Author:zhhui
 * DateTime:2014/7/29 13:19.
 */
public class CollectorException extends Exception {
    public CollectorException(String message) {
        super(message);
    }

    public CollectorException(String message, Throwable cause) {
        super(message, cause);
    }
}
