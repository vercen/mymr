package com.ksc.wordcount.rpc.Executor;

import akka.actor.AbstractActor;
import com.ksc.wordcount.task.map.MapTaskContext;
import com.ksc.wordcount.task.map.ShuffleMapTask;
import com.ksc.wordcount.task.reduce.ReduceTask;
import com.ksc.wordcount.task.reduce.ReduceTaskContext;
import com.ksc.wordcount.worker.ExecutorThreadPoolFactory;

public class ExecutorActor extends AbstractActor {

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(MapTaskContext.class, taskContext -> {
                    System.out.println("ExecutorActor received mapTaskContext:"+taskContext);
                    ExecutorThreadPoolFactory.getExecutorService().submit(new ShuffleMapTask(taskContext));
                })
                .match(ReduceTaskContext.class, taskContext -> {
                    System.out.println("ExecutorActor received reduceTaskContext:"+taskContext);
                    ExecutorThreadPoolFactory.getExecutorService().submit(new ReduceTask(taskContext));
                })
                .match(Object.class, message -> {
                    //处理不了的消息
                    System.out.println("unhandled message:" + message);
                })
                .build();
    }
}
