package com.ksc.wordcount.driver;

import com.ksc.wordcount.rpc.Driver.DriverRpc;
import com.ksc.wordcount.task.TaskContext;
import com.ksc.wordcount.task.TaskStatus;
import com.ksc.wordcount.task.TaskStatusEnum;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class TaskScheduler {

    private TaskManager taskManager;
    private ExecutorManager executorManager ;

    /**
     * taskId和ExecutorUrl的映射
     */
    private Map<Integer,String> taskExecuotrMap=new HashMap<>();

    public TaskScheduler(TaskManager taskManager, ExecutorManager executorManager) {
        this.taskManager = taskManager;
        this.executorManager = executorManager;
    }

    public void submitTask(int stageId) {
        BlockingQueue<TaskContext> taskQueue = taskManager.getBlockingQueue(stageId);

        while (!taskQueue.isEmpty()) {
            //todo 学生实现 轮询给各个executor派发任务
            executorManager.getExecutorAvailableCoresMap().forEach((executorUrl,availableCores)->{
                if(availableCores>0 && taskQueue.isEmpty()){
                    TaskContext task = taskQueue.poll();
                    //建立taskId和executorUrl的映射
                    taskExecuotrMap.put(task.getTaskId(),executorUrl);
                    //更新executor的可用核数
                    executorManager.updateExecutorAvailableCores(executorUrl,-1);
                    // 学生实现 调用DriverRpc的submitTask方法，将taskContext发送给executor
                    DriverRpc.submit(executorUrl,task);
                }
            });
            try {
                String executorAvailableCoresMapStr=executorManager.getExecutorAvailableCoresMap().toString();
                System.out.println("TaskScheduler submitTask stageId:"+stageId+",taskQueue size:"+taskQueue.size()+", executorAvailableCoresMap:" + executorAvailableCoresMapStr+ ",sleep 1000");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void waitStageFinish(int stageId){
        StageStatusEnum stageStatusEnum = taskManager.getStageTaskStatus(stageId);
        while (stageStatusEnum==StageStatusEnum.RUNNING){
            try {
                System.out.println("TaskScheduler waitStageFinish stageId:"+stageId+",sleep 1000");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            stageStatusEnum = taskManager.getStageTaskStatus(stageId);
        }
        if(stageStatusEnum == StageStatusEnum.FAILED){
            System.err.println("stageId:"+stageId+" failed");
            //重试
            submitTask(stageId);
        }
        if (stageStatusEnum == StageStatusEnum.FINISHED){
            System.out.println("stageId:"+stageId+" finished");
            System.exit(1);
        }

    }

    public void updateTaskStatus(TaskStatus taskStatus){
        if(taskStatus.getTaskStatus().equals(TaskStatusEnum.FINISHED)||taskStatus.getTaskStatus().equals(TaskStatusEnum.FAILED)){
            String executorUrl=taskExecuotrMap.get(taskStatus.getTaskId());
            executorManager.updateExecutorAvailableCores(executorUrl,1);
        }
    }


}
