#!/bin/bash
#一键启动脚本

#mymr-1.0.jar在target目录下
JAR_PATH="../target/mymr-1.0.jar"

# 启动 WordCountDriver 的主函数
java -cp "$JAR_PATH" com.ksc.wordcount.driver.WordCountDriver &

# 启动 Executor 的主函数
java -cp "$JAR_PATH" com.ksc.wordcount.worker.Executor &

