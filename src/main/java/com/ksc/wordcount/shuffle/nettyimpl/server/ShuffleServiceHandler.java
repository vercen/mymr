package com.ksc.wordcount.shuffle.nettyimpl.server;

import com.ksc.wordcount.conf.AppConfig;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.shuffle.nettyimpl.FileComplate;
import io.netty.channel.*;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;

public class ShuffleServiceHandler extends ChannelInboundHandlerAdapter {




    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        System.out.println("ShuffleServiceHandler received:"+msg);
        if (msg instanceof ShuffleBlockId) {
            ShuffleBlockId shuffleBlockId =(ShuffleBlockId) msg;
            System.out.println("ShuffleServiceHandler received:"+((ShuffleBlockId) msg).name());
            File file = new File(shuffleBlockId.getShufflePath());
            if (file.exists()) {
                ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(file));
                Object obj = null;
                //(obj=objectInputStream.readObject())!=null
                do{
                    try{
                        obj = objectInputStream.readObject();
                    } catch (EOFException e){
                        break;
                    }
                    ctx.writeAndFlush(obj);
                }while (obj != null);
                System.out.println("ShuffleServiceHandler send FileComplate");
                ctx.writeAndFlush(new FileComplate());
            } else {
                ctx.writeAndFlush("shuffle File not found: " + file.getAbsolutePath());
            }
        } else {
            super.channelRead(ctx, msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}