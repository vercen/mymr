package com.ksc.wordcount.shuffle.nettyimpl.client;

import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.task.KeyValue;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

import java.util.AbstractMap;
import java.util.stream.Stream;

public class ShuffleClient {

    //static  Bootstrap clientBootstrap = getNettyBootStrap();

    public  Bootstrap getNettyBootStrap(){
        EventLoopGroup group = new NioEventLoopGroup();
        Bootstrap b = new Bootstrap();
        b.group(group);
        return b;
    }


    public Stream<KeyValue> fetchShuffleData(ShuffleBlockId shuffleBlockId) throws InterruptedException {
        ShuffleClientHandler shuffleClientHandler = new ShuffleClientHandler();
        Bootstrap bootstrapChannel = getNettyBootStrap().channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(
                                new ObjectEncoder(),
                                new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                shuffleClientHandler
                        );
                    }
                });
        ChannelFuture channelFuture = bootstrapChannel.connect(shuffleBlockId.getHost(), shuffleBlockId.getPort()).sync();
        channelFuture.addListener(future -> {
                if (future.isSuccess()) {
                    System.out.println("connect File Server: 连接服务器成功");
                } else {
                    System.out.println("connect File Server: 连接服务器失败");
                }
            });
        channelFuture.channel().writeAndFlush(shuffleBlockId);
        System.out.println("connect File Server: 已发送文件请求");
//        channelFuture.channel().closeFuture().sync();
        channelFuture.channel().closeFuture();
        return shuffleClientHandler.getStream();
    }
}
