package com.ksc.wordcount.task.reduce;

import com.ksc.wordcount.datasourceapi.PartionWriter;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.shuffle.nettyimpl.client.ShuffleClient;
import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.task.Task;
import com.ksc.wordcount.task.TaskStatusEnum;

import java.io.IOException;
import java.util.stream.Stream;

public class ReduceTask extends Task {

    ShuffleBlockId[] shuffleBlockId;
    //String destDir;
    ReduceFunction reduceFunction;
    PartionWriter partionWriter;



    public ReduceTask(ReduceTaskContext reduceTaskContext) {
        super(reduceTaskContext);
        this.shuffleBlockId = reduceTaskContext.getShuffleBlockId();
        //this.destDir = reduceTaskContext.getDestDir();
        this.reduceFunction = reduceTaskContext.getReduceFunction();
        this.partionWriter = reduceTaskContext.getPartionWriter();
    }




    public ReduceStatus runTask() throws Exception {
            Stream<KeyValue> stream=Stream.empty();
            for(ShuffleBlockId shuffleBlockId:shuffleBlockId){
                stream=Stream.concat(stream,new ShuffleClient().fetchShuffleData(shuffleBlockId));
            }

//            HashMap<String, Integer> map = new HashMap<>();
//            stream.forEach(e->{
//                String key = (String) e.getKey();
//                Integer value = (Integer) e.getValue();
//                if (map.containsKey(key)) {
//                    map.put(key, map.get(key) + value);
//                } else {
//                    map.put(key, value);
//                }
//            });
            Stream reduceStream = reduceFunction.reduce(stream);
        System.out.println("测试");
            partionWriter.write(reduceStream);
            return new ReduceStatus(super.taskId, TaskStatusEnum.FINISHED);
        }
}
