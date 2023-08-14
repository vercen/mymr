package com.ksc.wordcount.task.map;

import com.ksc.wordcount.datasourceapi.PartionFile;
import com.ksc.wordcount.datasourceapi.PartionReader;
import com.ksc.wordcount.conf.AppConfig;
import com.ksc.wordcount.shuffle.DirectShuffleWriter;
import com.ksc.wordcount.task.Task;
import com.ksc.wordcount.task.map.MapFunction;
import com.ksc.wordcount.task.map.MapStatus;
import com.ksc.wordcount.task.map.MapTaskContext;

import java.io.IOException;
import java.util.UUID;
import java.util.stream.Stream;

public class ShuffleMapTask extends Task<MapStatus> {

    PartionFile partiongFile;
    PartionReader partionReader;
    int reduceTaskNum;
    MapFunction mapFunction;

    public ShuffleMapTask(MapTaskContext mapTaskContext) {
        super(mapTaskContext);
        this.partiongFile = mapTaskContext.getPartiongFile();
        this.partionReader = mapTaskContext.getPartionReader();
        this.reduceTaskNum = mapTaskContext.getReduceTaskNum();
        this.mapFunction = mapTaskContext.getMapFunction();
    }


    public MapStatus runTask() throws IOException {
        Stream<String> stream = partionReader.toStream(partiongFile);


//        Stream<AbstractMap.SimpleEntry<String, Integer>> simpleEntryStream = stream.flatMap(line -> Arrays.stream(line.split("\\s+")))
//                .map(word -> new AbstractMap.SimpleEntry<String, Integer>(word, 1));
        Stream kvStream = mapFunction.map(stream);

        String shuffleId= UUID.randomUUID().toString();
        //将task执行结果写入shuffle文件中
        DirectShuffleWriter shuffleWriter = new DirectShuffleWriter(AppConfig.shuffleTempDir, shuffleId,applicationId, partionId, reduceTaskNum);
        shuffleWriter.write(kvStream);
        shuffleWriter.commit();
        return shuffleWriter.getMapStatus(taskId);
    }




}
