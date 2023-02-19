import asyncio
from collections import *
from queue import Queue

class Buffer:
    # __shared_state = ""
    def __init__(self, topic):
        self.topic = topic
        # self.buff = Queue(maxsize=10) # 크기가 10인 버퍼 역할 큐
        self.buff = Queue(maxsize = 10)
        # self.__dict__ == self.__shared_state
    def add_data(self, data): # data 넣기 (data는 json)
        if self.buff.full():
            out = self.buff.get(block=True, timeout=None)
            print("Buf is FULL!! Removed: ", out)
            # self.buff.get(block=True, timeout=None)
        self.buff.put(data, block=True, timeout=None)
        print("Add Data to buffer: ", data)
        
    def get_data(self): # data 꺼내기
        if self.buff.full():
            ret = self.buff.get(block=True, timeout=None)
            print("Get Data from buffer: ", ret)
            return ret
        elif self.buff.empty():
            print("---------------No data in buffer!---------------- in: ", self.topic)
        else:
            ret = self.buff.get(block=True, timeout=None)
            print("Get Data from buffer: ", ret)
            return ret