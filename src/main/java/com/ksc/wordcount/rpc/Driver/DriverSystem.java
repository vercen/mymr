package com.ksc.wordcount.rpc.Driver;

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

public class DriverSystem {

    static  ActorSystem system;

    public static ActorSystem getExecutorSystem() {
        if(system!=null){
            return system;
        }
        Map<String, Object> map=new HashMap<>();

        map.put("akka.actor.provider","remote");
        map.put("akka.remote.transport","akka.remote.netty.NettyRemoteTransport");
        map.put("akka.remote.netty.tcp.hostname", DriverEnv.host);
        map.put("akka.remote.netty.tcp.port", DriverEnv.port);
        Config config = ConfigFactory.parseMap(map).withFallback(ConfigFactory.load());
        system = ActorSystem.create("DriverSystem", config);
        return system;
    }

    /**
     * executorUrl和akka连接的映射
     */
    static Map<String,ActorRef> executorRefs = new HashMap<>();

    public static ActorRef getExecutorRef(String executorUrl){
        if(executorRefs.get(executorUrl)!=null){
            return executorRefs.get(executorUrl);
        }
        ActorRef driverRef = getExecutorSystem().actorSelection(executorUrl)
                .resolveOne(Duration.ofSeconds(10)).toCompletableFuture().join();
        executorRefs.put(executorUrl,driverRef);
        return executorRefs.get(executorUrl);
    }
}
