import sys
sys.path.append("./")
from image_matchxx.elasticsearchflat_driver import *
from elasticsearch import Elasticsearch
import time

signature_es = SignatureES(Elasticsearch('http://10.0.2.26:9200'),
                       index='atlasfinger_test_1',
                       distance_cutoff=0.45,
                       k=3,
                       n_grid=5,
                       N=36,
                       size=100)
x = signature_es.add_image_with_data_id('http://static.1sapp.com/lw/img/2019/09/02/145fc21f7975b8c0e12323f356c0c95f.jpeg','4',1)
print(x)
