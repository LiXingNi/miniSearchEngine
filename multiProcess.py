'''
from multiprocessing import Pool,Manager
from mergeTempBtables import MergeTwo
import os

def process_f(args):
    q = args[0]
    lis = args[1]
    try:
        val = q.get(block = False)
        if len(val) == 2:
            merge = MergeTwo(val[0],val[1])
            merge.mergerFun()
            s = "%s_%s"%(val[0],val[1])
            lis.append(s)
        else:
            if len(val) == 1:
                lis.append(val[0])
            else:
                print "error"
    except:
        pass
    
def p_task(q,lis):
    i  = 0
    length = len(lis)
    while i + 1 < length:
        q.put((lis[i],lis[i + 1]),block = False)
        i += 2
    if i < length:
        q.put((lis[i],),block = False)
    del lis[:]
    



if __name__ == "__main__":
    manager = Manager()
    queue = manager.Queue()
    lis = manager.list(range(11))
    pool = Pool(processes = 6)
    
    while len(lis) > 1:
        num = len(lis) / 2 + 1
        p_task(queue,lis)
        args = (queue,lis)
        pool.map(process_f,[args]*num)
        print lis[:]
    
    print "end"
    pool.close()
'''
from multiprocessing import Pool,Manager
from mergeTempBtables import MergeTwo

def process_f(args):
    q = args[0]
    dic = args[1]
    try:
        val = q.get(block = False)
        key_v = val[0].split('_',1)
        key = int(key_v[0])
        if len(val) == 2:
            merge = MergeTwo(val[0],val[1])
            merge.mergerFun()
            s = "%s_%s"%(val[0],val[1])
            dic[key] = s
        else:
            if len(val) == 1:
                dic[key] = val[0]
            else:
                print "error"
    except:
        pass
    
def p_task(q,dic):
    i  = 0
    length = len(dic)
    lis = dic.keys()
    lis.sort()
    while i + 1 < length:
        q.put((dic[lis[i]],dic[lis[i + 1]]),block = False)
        i += 2
    if i < length:
        q.put((dic[lis[i]],),block = False)
    dic.clear()
    



if __name__ == "__main__":
    manager = Manager()
    queue = manager.Queue()
    dic = manager.dict()
    for i in range(11):
        dic[i] = "%s"%i
    pool = Pool(processes = 6)
    
    while len(dic) > 1:
        num = len(dic) / 2 + 1
        p_task(queue,dic)
        args = (queue,dic)
        pool.map(process_f,[args]*num)
        lis = dic.values()
        print lis[:]
    
    print "end"
    pool.close()
