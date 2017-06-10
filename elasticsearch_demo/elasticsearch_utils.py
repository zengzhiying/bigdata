# coding=utf-8
from elasticsearch import Elasticsearch

class ElasticsearchUtils(object):
    def __init__(self, host_ports):
        # host_ports格式 [{'host':'xxx', 'port':9200},{}]
        self.host_ports = host_ports
        self.es = None

    def init_connect(self):
        self.es = Elasticsearch(self.host_ports)
        return self.es.ping()


    def get_search_result(self, index_name, type_name, query_body):
        if self.es:
            return self.es.search(index=index_name, doc_type=type_name, body=query_body)
        return

    def get_id_result(self, index_name, type_name, doc_id):
        if self.es:
            return self.es.get(index=index_name, doc_type=type_name, id=doc_id)['_source']
        return


    # doc_id为None说明让es自动生成id
    def add_index_doc(self, index_name, type_name, doc_id, doc_body):
        if doc_id:
            self.es.index(index=index_name, doc_type=type_name, id=doc_id, body=doc_body)
        else:
            self.es.index(index=index_name, doc_type=type_name, body=doc_body)

    def batch_index(self, index_name, type_name, doc_body_lines):
        self.es.bulk(index=index_name, doc_type=type_name, body=doc_body_lines)
