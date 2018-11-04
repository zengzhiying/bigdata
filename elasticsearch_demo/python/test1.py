# coding=utf-8
import requests
import json

s1 = ('{"index":{"_id":"100"}}\n'
      '{"name":"sss1"}\n')
rs = requests.post('http://localhost:9200/website/blog/_bulk', data=s1)
print rs
