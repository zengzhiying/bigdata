#!/usr/bin/env python3
# coding=utf-8
import time

import numpy

from annoy import AnnoyIndex

def cosine_similarity(multiple_feature, f):
    """余弦相似度计算, 支持批量计算
    Args:
        multiple_feature: 批量待比对矩阵: dim -> x*n, x特征值个数; n向量维度
        f: 1*n的向量, n向量维度
    Returns:
        余弦值结果向量: 取[:, 0]得到结果1*x
    """
    x = multiple_feature.shape[0]
    multiple_norm = numpy.linalg.norm(multiple_feature, axis=1).reshape(x, 1)
    f_norm = numpy.linalg.norm(f, axis=1).reshape(1, 1)
    return multiple_feature.dot(f.T) / multiple_norm.dot(f_norm)

def dot(vectors, v):
    return vectors.dot(v.T)

if __name__ == '__main__':
    dim = 128
    n = 200000
    top_k = 100
    # vectors = numpy.random.uniform(-3, 3, (n, dim))
    vectors = numpy.random.random((n, dim))
    # 归一化
    norms = numpy.linalg.norm(vectors, axis=1)
    vectors = vectors / norms.reshape(n, 1)

    # v = numpy.random.uniform(-3, 3, size=dim)
    # v = numpy.random.random(dim)
    # v /= numpy.linalg.norm(v)
    v = vectors[1003]

    print(numpy.sum(numpy.linalg.norm(vectors, axis=1) == 1))

    t1 = time.time()
    #r = cosine_similarity(vectors, v.reshape(1, dim))[:, 0]
    
    r = vectors.dot(v.reshape(1, dim).T)[:,0]

    ids = numpy.argsort(r)[::-1][:top_k]
    values = r[ids]

    t2 = time.time()
    print(ids, values)
    print("time1: {:.3f}s".format(t2 - t1))

    ann = AnnoyIndex(dim, 'dot')
    for i, vec in enumerate(vectors):
        ann.add_item(i, vec.tolist())

    ann.build(n_trees=100)
    ann.save('vec-index.ann')


    ann = AnnoyIndex(dim, 'dot')
    # prefault 是否提前预加载模型, 默认为mmap读取时加载
    ann.load('vec-index.ann', prefault=False)

    t1 = time.time()
    r1 = ann.get_nns_by_vector(v.tolist(), top_k, include_distances=True)
    t2 = time.time()

    
    print(r1)

    print("time2: {:.3f}s".format(t2 - t1))
    ann.unload()


    s1 = set(ids)
    s2 = set(r1[0])
    print(len(s1 & s2) / top_k)

