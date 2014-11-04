package com.hansight.kunlun.collector.coordinator.config;

import java.util.HashMap;

/**
 * Config domain model
 */
public class Config extends HashMap<String, String> {
    private static final long serialVersionUID = -6641983504710978489L;

    public enum State {
        NEW,
        UPDATE,
        DELETE
    }

    private State state = null; // mark Config state

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public String getId() {
        return this.get(ConfigConstants.DATASOURCE_ID);
    }
}
