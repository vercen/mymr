package com.ksc.wordcount.datasourceapi;

import com.ksc.wordcount.task.KeyValue;
import org.apache.http.util.ByteArrayBuffer;

import java.io.*;
import java.util.stream.Stream;

public class TextPartionWriter implements PartionWriter<KeyValue>, Serializable {

    private String destDest;
    private int partionId;

    public TextPartionWriter(String destDest,int partionId){
         this.destDest = destDest;
         this.partionId = partionId;
    }

    //把partionId 前面补0，补成length位
    public String padLeft(int partionId,int length){
        String partionIdStr = String.valueOf(partionId);
        int len = partionIdStr.length();
        if(len<length){
            for(int i=0;i<length-len;i++){
                partionIdStr = "0"+partionIdStr;
            }
        }
        return partionIdStr;
    }

    //todo 学生实现 将reducetask的计算结果写入结果文件中
    @Override
    public void write(Stream<KeyValue> stream) throws IOException {
        File file = new File(destDest + File.separator + "part_" + padLeft(partionId, 3) + ".txt");
        try(FileOutputStream fos = new FileOutputStream(file)){
            //把流中的数据写入到文件中
            stream.forEach(keyValue -> {
                try {
                    fos.write((keyValue.getKey() + "\t" + keyValue.getValue() + "\n").getBytes("UTF-8"));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }

}
