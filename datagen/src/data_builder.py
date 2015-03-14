import random
import json
import uuid
import time
import codecs

class Entity():

  def __init__(self, row_key, range_key, **kwargs):
    self.row_key = row_key
    self.range_key = range_key
    for k in kwargs:
      self.__dict__[k] = kwargs[k]

  def to_jsons(self):
    return json.dumps(self.__dict__)

  def to_json(self):
    return json.dump(self.__dict__)


'''
rk_start:
rk_count:
'''
def build(**kwargs):

  row_key_gen = None
  r = random.Random()
  rk_count = 1000
  rk_start = 1
  string_size = 1024

  if 'rk_count' in kwargs:
    rk_count = kwargs['rk_count']
  if 'random' in kwargs:
    row_key_gen = lambda: r.uniform(1, 2000 * 1000 * 1000)
  if 'rk_start' in kwargs:
    rk_start = kwargs['rk_start']

  f = codecs.open('hadoop_in_action_ch_9.txt', encoding='utf-8')
  txt = f.read()
  f.close()
  start_max = len(txt)/2
  for i in range(rk_start, rk_count):
    rk = i if row_key_gen == None else row_key_gen()
    range_count = kwargs['range_count'] if 'range_count' in kwargs else i
    for j in range(1, range_count):
      guid = uuid.uuid1()
      start = int(r.uniform(0, start_max))
      yield Entity(row_key=rk, range_key=j, the_guid=str(guid),
                   the_int=int(r.uniform(0, 1000000000)), the_date=time.time(),
                   the_string=txt[start:start+string_size])

for f in build():
  print(f.to_jsons())

