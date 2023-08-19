#!/bin/bash
#一键启动脚本
#com.ksc.wordcount.driver.WordCountDriver
#mymr-1.0.jar在target目录下
java -cp ../target/mymr-1.0.jar com.ksc.wordcount.driver.WordCountDriver
#启动com.ksc.wordcount.worker.Executor
java -cp ../target/mymr-1.0.jar com.ksc.wordcount.worker.Executor

