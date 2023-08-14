package com.ksc.wordcount.rpc.Executor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.ksc.wordcount.driver.DriverEnv;
import com.ksc.wordcount.worker.ExecutorEnv;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ExecutorSystem {

    static  ActorSystem system;

    public static ActorSystem getExecutorSystem() {
        if(system!=null){
            return system;
        }
        Map<String, Object> map=new HashMap<>();

        map.put("akka.actor.provider","remote");
        map.put("akka.remote.transport","akka.remote.netty.NettyRemoteTransport");
        map.put("akka.remote.netty.tcp.hostname", ExecutorEnv.host);
        map.put("akka.remote.netty.tcp.port", ExecutorEnv.port);
        Config config = ConfigFactory.parseMap(map).withFallback(ConfigFactory.load());
        system = ActorSystem.create("ExecutorSystem", config);
        return system;
    }

    public static ActorRef getDriverRef(){
        String driverUrl = ExecutorEnv.driverUrl;
        ActorRef driverRef = getExecutorSystem().actorSelection(driverUrl)
                .resolveOne(Duration.ofSeconds(10)).toCompletableFuture().join();
        return driverRef;
    }
}
