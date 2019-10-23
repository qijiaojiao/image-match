import sys
sys.path.append("./")
from image_match.elasticsearchflat_driver import *
from elasticsearch import Elasticsearch
import time

signature_es = SignatureES(Elasticsearch('http://10.0.2.26:9200'),
                       index='fingerprint_dedup_qtt_4',
                       distance_cutoff=1.0,
                       k=3,
                       n_grid=4,
                       N=27,
                       size=100)
x = signature_es.search_image('http://static.1sapp.com/lw/img/2019/09/02/145fc21f7975b8c0e12323f356c0c95f.jpeg')
print(x)
