package com.ksc.wordcount.driver;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.ksc.wordcount.datasourceapi.FileFormat;
import com.ksc.wordcount.datasourceapi.PartionFile;
import com.ksc.wordcount.datasourceapi.PartionWriter;
import com.ksc.wordcount.datasourceapi.UnsplitFileFormat;
import com.ksc.wordcount.rpc.Driver.DriverActor;
import com.ksc.wordcount.rpc.Driver.DriverSystem;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.task.*;
import com.ksc.wordcount.task.map.MapFunction;
import com.ksc.wordcount.task.map.MapTaskContext;
import com.ksc.wordcount.task.reduce.ReduceFunction;
import com.ksc.wordcount.task.reduce.ReduceTaskContext;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;

public class WordCountDriver {


    public static void main(String[] args) {
        //输出当前路径
        System.out.println("当前执行目录"+System.getProperty("user.dir"));

        String inputPath = "F:\\tmp\\inputfile";
        String outputPath = "F:\\tmp\\outputfile";
        String applicationId = "wordcount_001";
        int reduceTaskNum = 2;
        //读取bin/master.conf配置文件,用空格分割
        try (BufferedReader reader = new BufferedReader(new FileReader("bin/master.conf"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split("\\s+");
                if (!Objects.equals(tokens[0], "#")) {
                    DriverEnv.host = tokens[0];
                    DriverEnv.port = Integer.parseInt(tokens[1]);
                    //退出循环
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try (BufferedReader reader = new BufferedReader(new FileReader("bin/urltopn.conf"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split("\\s+");
                if (!Objects.equals(tokens[0], "#")) {
                    //System.out.println(Arrays.toString(tokens));
                     applicationId = tokens[0];
                     inputPath = tokens[1];
                     outputPath = tokens[2];
                     reduceTaskNum = Integer.parseInt(tokens[4]);

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }




        FileFormat fileFormat = new UnsplitFileFormat();
        PartionFile[] partionFiles = fileFormat.getSplits(inputPath, 1000);

        TaskManager taskScheduler = DriverEnv.taskManager;

        ActorSystem executorSystem = DriverSystem.getExecutorSystem();
        ActorRef driverActorRef = executorSystem.actorOf(Props.create(DriverActor.class), "driverActor");
        System.out.println("ServerActor started at: " + driverActorRef.path().toString());


        int mapStageId = 0;
        //添加stageId和任务的映射
        taskScheduler.registerBlockingQueue(mapStageId, new LinkedBlockingQueue());
        for (PartionFile partionFile : partionFiles) {
            MapFunction wordCountMapFunction = new MapFunction<String, KeyValue>() {
                //todo 学生实现 定义maptask处理数据的规则
                @Override
                public Stream<KeyValue> map(Stream<String> stream) {
                    //学生实现 定义maptask处理数据的规则统计URL访问次数
                    //输入格式1 192.168.0.3 - [10/Aug/2023:13:57:38 +0000] POST http://example.com/page3 200
                    //输入格式2 "192.168.0.6","10/Aug/2023:14:02:43 +0000","GET","http://example.com/page2",200
                    //输入格式3 {"IP": "192.168.0.12", "Timestamp": "10/Aug/2023:14:09:50 +0000", "Method": "GET", "URL": "http://example.com/page3", "Status": 200}
                    //输出格式 http://example.com/page3 1

                    return stream.flatMap(line -> {
                        String[] parts = line.split("\"");
                        for (String part : parts) {
                            String[] subParts = part.trim().split(" ");
                            for (String subPart : subParts) {
                                if (subPart.contains("http://")) {
                                    int startIndex = subPart.indexOf("http://");
                                    int endIndex = Math.min(subPart.indexOf(" "), subPart.indexOf(",", startIndex));
                                    if (endIndex == -1) {
                                        endIndex = subPart.length();
                                    }
                                    String url = subPart.substring(startIndex, endIndex);
                                    return Stream.of(new KeyValue(url, 1));
                                }
                            }
                        }
                        return Stream.empty();
                    });
                }
            };
            MapTaskContext mapTaskContext = new MapTaskContext(applicationId, "stage_" + mapStageId, taskScheduler.generateTaskId(), partionFile.getPartionId(), partionFile,
                    fileFormat.createReader(), reduceTaskNum, wordCountMapFunction);
            taskScheduler.addTaskContext(mapStageId, mapTaskContext);
        }

        //提交stageId
        DriverEnv.taskScheduler.submitTask(mapStageId);
        DriverEnv.taskScheduler.waitStageFinish(mapStageId);


        int reduceStageId = 1;
        taskScheduler.registerBlockingQueue(reduceStageId, new LinkedBlockingQueue());
        for (int i = 0; i < reduceTaskNum; i++) {
            ShuffleBlockId[] stageShuffleIds = taskScheduler.getStageShuffleIdByReduceId(mapStageId, i);
            ReduceFunction<String, Integer, String, Integer> reduceFunction = new ReduceFunction<String, Integer, String, Integer>() {

                @Override
                public Stream<KeyValue<String, Integer>> reduce(Stream<KeyValue<String, Integer>> stream) {
                    HashMap<String, Integer> map = new HashMap<>();
                    //todo 学生实现 定义reducetask处理数据的规则
                    stream.forEach(e -> {
                        String key = e.getKey();
                        Integer value = e.getValue();
                        if (map.containsKey(key)) {
                            map.put(key, map.get(key) + value);
                        } else {
                            map.put(key, value);
                        }
                    });
                    return map.entrySet().stream().map(e -> new KeyValue(e.getKey(), e.getValue()));
                }
            };
            PartionWriter partionWriter = fileFormat.createWriter(outputPath, i);
            ReduceTaskContext reduceTaskContext = new ReduceTaskContext(applicationId, "stage_" + reduceStageId, taskScheduler.generateTaskId(), i, stageShuffleIds, reduceFunction, partionWriter);
            taskScheduler.addTaskContext(reduceStageId, reduceTaskContext);
        }

        DriverEnv.taskScheduler.submitTask(reduceStageId);
        DriverEnv.taskScheduler.waitStageFinish(reduceStageId);
        System.out.println("job finished");


    }
}
