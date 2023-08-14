package com.ksc.wordcount.shuffle.nettyimpl.client;


import com.ksc.wordcount.shuffle.nettyimpl.FileComplate;
import com.ksc.wordcount.task.KeyValue;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.stream.Stream;

public class ShuffleClientHandler extends SimpleChannelInboundHandler{

    BlockingQueueStream<KeyValue> blockingQueueStream = new BlockingQueueStream<>(2);

    public ShuffleClientHandler() {
    }


    public Stream getStream() {
        return blockingQueueStream.stream();
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // out = new FileOutputStream(fileName);
        System.out.println("channelActive");
    }


    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        System.out.println("ShuffleClientHandler receive msg: " + msg);
        if (msg instanceof KeyValue){
            KeyValue entry = (KeyValue) msg;
            //这么方式数据是放在内存中的，如果数据量大，会导致内存溢出
            blockingQueueStream.add(entry);
        }
        if (msg instanceof String) {
            System.out.println("receive error: " + msg);
        }
        if (msg instanceof FileComplate) {
            blockingQueueStream.done();
            ctx.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        blockingQueueStream.done();
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        System.out.println("ShuffleClientHandler channelInactive");
        blockingQueueStream.done();
    }
}
