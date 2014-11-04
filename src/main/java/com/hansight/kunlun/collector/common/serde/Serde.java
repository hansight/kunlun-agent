package com.hansight.kunlun.collector.common.serde;

import java.io.IOException;

/**
 * Author:zhhui
 * DateTime:2014/7/29 14:26.
 */
public interface Serde<T> {
    byte[] serialize(T event) throws IOException;

    T deserialize(byte[] body) throws IOException;
}
