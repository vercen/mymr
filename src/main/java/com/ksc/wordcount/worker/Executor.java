package com.ksc.wordcount.worker;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.ksc.wordcount.driver.DriverEnv;
import com.ksc.wordcount.rpc.Executor.ExecutorActor;
import com.ksc.wordcount.rpc.Executor.ExecutorRpc;
import com.ksc.wordcount.rpc.Executor.ExecutorSystem;
import com.ksc.wordcount.rpc.ExecutorRegister;
import com.ksc.wordcount.shuffle.nettyimpl.server.ShuffleService;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class Executor {


    public static void main(String[] args) throws InterruptedException {
        String akkaport = "4040";

        try (BufferedReader reader = new BufferedReader(new FileReader("bin/slave.conf"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split("\\s+");
                if (!Objects.equals(tokens[0], "#")) {
                    ExecutorEnv.host=tokens[0];
                    akkaport = tokens[1];
                    ExecutorEnv.shufflePort = Integer.parseInt(tokens[2]);
                    ExecutorEnv.memory=tokens[3];
                    ExecutorEnv.core=Integer.parseInt(tokens[4]);
                    //结束循环
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


//        ExecutorEnv.host="127.0.0.1";
        ExecutorEnv.port=15050;
//        ExecutorEnv.memory="512m";
        ExecutorEnv.driverUrl="akka.tcp://DriverSystem@127.0.0.1:"+akkaport+"/user/driverActor";
//        ExecutorEnv.core=2;
        ExecutorEnv.executorUrl="akka.tcp://ExecutorSystem@"+ ExecutorEnv.host+":"+ExecutorEnv.port+"/user/executorActor";
//        ExecutorEnv.shufflePort=7337;

        new Thread(() -> {
            try {
                new ShuffleService(ExecutorEnv.shufflePort).start();
            } catch (InterruptedException e) {
                new RuntimeException(e);
            }
        }).start();

        ActorSystem executorSystem = ExecutorSystem.getExecutorSystem();
        ActorRef clientActorRef = executorSystem.actorOf(Props.create(ExecutorActor.class), "executorActor");
        System.out.println("ServerActor started at: " + clientActorRef.path().toString());
        ExecutorRpc.register(new ExecutorRegister(ExecutorEnv.executorUrl,ExecutorEnv.memory,ExecutorEnv.core));


    }

}
