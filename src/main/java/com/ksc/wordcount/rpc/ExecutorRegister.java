package com.ksc.wordcount.rpc;

import java.io.Serializable;

public class ExecutorRegister implements Serializable {

    String executorUrl;
    String memory;
    int cores;

    public ExecutorRegister(String executorUrl, String memory, int cores) {
        this.executorUrl = executorUrl;
        this.memory = memory;
        this.cores = cores;
    }

    public String getExecutorUrl() {
        return executorUrl;
    }

    public String getMemory() {
        return memory;
    }

    public int getCores() {
        return cores;
    }
}
