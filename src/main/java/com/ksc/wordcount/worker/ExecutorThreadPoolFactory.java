package com.ksc.wordcount.worker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExecutorThreadPoolFactory {

    private static ExecutorService executorService;

    public  static  ExecutorService getExecutorService() {
        if (executorService == null) {
            executorService = Executors.newFixedThreadPool(ExecutorEnv.core);
        }
        return executorService;
    }



}
