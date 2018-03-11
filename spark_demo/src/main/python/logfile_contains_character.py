#!/usr/bin/env python
# coding=utf-8
from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    conf = SparkConf().setAppName('logfile_character_count')
    sc = SparkContext(conf=conf)
    log_file = 'hdfs://index:8020/test_log.log'
    content = sc.textFile(log_file).cache()

    num_a = content.filter(lambda s: 'a' in s).count()
    num_b = content.filter(lambda s: 'b' in s).count()

    print "content with a: %d, with b: %d" % (num_a, num_b)
