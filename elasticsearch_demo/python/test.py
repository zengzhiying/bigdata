#!/usr/bin/env python
# coding=utf-8
from elasticsearch_utils import ElasticsearchUtils
es = ElasticsearchUtils([{'host':'127.0.0.1', 'port':9200}])
print es.init_connect()
result = es.get_search_result('website', 'blog', {
       'query':{
            "match":{"name":"测试"}
        }
    })
#print result
print "条数: %d" % result['hits']['total']
for rs in result['hits']['hits']:
    rs_source = rs['_source']
    print rs_source['name']
    print rs_source['id']
#es.add_index_doc('website', 'blog', '7',{
#    'name':"测试名称",
#    'id':3568
#})
bodys = ('{"index": { "_id":"100"}}\n'
         '{"name":"sss1", "id":2029}\n'
         '{"index": { "_id":"101"}}\n'
         '{"name":"sss2", "id":2030}\n'
        )
#print bodys
es.batch_index("website", "blog", bodys)
