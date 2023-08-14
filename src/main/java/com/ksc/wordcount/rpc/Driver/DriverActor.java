package com.ksc.wordcount.rpc.Driver;

import akka.actor.AbstractActor;
import com.ksc.wordcount.driver.DriverEnv;
import com.ksc.wordcount.rpc.ExecutorRegister;
import com.ksc.wordcount.task.TaskStatus;
import com.ksc.wordcount.task.TaskStatusEnum;

public class DriverActor extends AbstractActor {

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(TaskStatus.class, mapStatus -> {
                    System.out.println("ExecutorActor received mapStatus:"+mapStatus);
                    if(mapStatus.getTaskStatus() == TaskStatusEnum.FAILED) {
                        System.err.println("task status taskId:"+mapStatus.getTaskId());
                        System.err.println("task status errorMsg:"+mapStatus.getErrorMsg());
                        System.err.println("task status errorStackTrace:\n"+mapStatus.getErrorStackTrace());
                    }
                    DriverEnv.taskManager.updateTaskStatus(mapStatus);
                    DriverEnv.taskScheduler.updateTaskStatus(mapStatus);
                })
                .match(ExecutorRegister.class, executorRegister -> {
                    System.out.println("ExecutorActor received executorRegister:"+executorRegister);
                    DriverEnv.executorManager.updateExecutorRegister(executorRegister);
                })
                .match(Object.class, message -> {
                    //处理不了的消息
                    System.err.println("unhandled message:" + message);
                })
                .build();
    }
}
