package com.ksc.wordcount.task.reduce;

import com.ksc.wordcount.task.TaskStatus;
import com.ksc.wordcount.task.TaskStatusEnum;

public class ReduceStatus extends TaskStatus {

    public ReduceStatus(int taskId) {
        super(taskId, TaskStatusEnum.FINISHED);
    }

    public ReduceStatus(int taskId,TaskStatusEnum taskStatus) {
        super(taskId,taskStatus);
    }

    public ReduceStatus(int taskId,TaskStatusEnum taskStatus, String errorMsg,String errorStackTrace) {
        super(taskId,taskStatus, errorMsg,errorStackTrace);
    }

}
