#!/bin/bash

NUODB_JAR=../dist/jar/nuodbjdbc.jar

javac InsertLoad.java 
java -Xss16m -cp $(dirname $0):${NUODB_JAR} InsertLoad $*
