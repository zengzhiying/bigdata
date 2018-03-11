#!/usr/bin/env python
# coding=utf-8
from pyspark import SparkContext, SparkConf
'''
./bin/spark-submit --master spark://master:7077 --executor-memory 1G --total-executor-cores 3 word_count_demo.py
'''
def printLine(line):
    print line
if __name__ == '__main__':
    conf = SparkConf().setAppName('word_count_demo')
    sc = SparkContext(conf=conf)
    texts = sc.textFile('hdfs://index:8020/wordcount')
    texts.foreach(lambda line:printLine(line))
    counts = texts.flatMap(lambda line:line.split(" ")).map(lambda word:(word, 1)).reduceByKey(lambda a, b:a + b)
    # 转为driver结果
    count_result = counts.collect()
    for result in count_result:
        print result

    print "save to hdfs."
    counts.saveAsTextFile('hdfs://index:8020/wordcount_output')
