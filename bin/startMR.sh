#!/bin/bash
# 一键启动脚本

# mymr-1.0.jar在target目录下
JAR_PATH="../target/mymr-1.0.jar"

# 创建log目录
LOG_DIR="./log"
mkdir -p "$LOG_DIR"

# 启动 WordCountDriver 的主函数
java -cp "$JAR_PATH" com.ksc.wordcount.driver.WordCountDriver > "$LOG_DIR/WordCountDriver.log" 2>&1 &

# 休眠2秒
sleep 2

# 启动 Executor 的主函数
java -cp "$JAR_PATH" com.ksc.wordcount.worker.Executor > "$LOG_DIR/Executor.log" 2>&1 &

