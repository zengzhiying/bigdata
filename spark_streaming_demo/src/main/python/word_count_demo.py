#!/usr/bin/env python
# coding=utf-8
from pyspark import SparkContext, SparkConf
'''
./bin/spark-submit --master spark://master:7077 --executor-memory 1G --deploy-mode cluster --total-executor-cores 3 word_count_demo.py
'''
if __name__ == '__main__':
    conf = SparkConf().setAppName('logfile_character_count')
    sc = SparkContext(conf=conf)
    texts = sc.textFile('hdfs://index:8020/wordcount')
    texts.foreach(lambda line:print line)
    counts = texts.flatMap(lambda line:line.split(" ")).map(lambda word:(word, 1)).reduceByKey(lambda a, b:a + b)
    # 转为driver结果
    count_result = counts.collect()
    for result in count_result:
        print result

    print "save to hdfs."
    counts.saveAsTextFile('hdfs://index:8020/wordcount_output')
