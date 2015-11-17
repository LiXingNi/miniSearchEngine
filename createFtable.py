
import jieba
import sqlite3
#from _tkinter import create
import operator

class ForwardTableObj:
    doc_id = 0
    word_id = 0
    hits= []

    def __init__(self,t_doc_id,t_word_id,t_hits):
        self.doc_id = t_doc_id
        self.word_id = t_word_id
        self.hits = t_hits[:]
        
class PageDic:
    
    page_dic = {}
    
    def __init__(self):
        self.page_dic = {}
    
    def addPageItem(self,t_word,t_beg,t_end):
        
        if not self.page_dic.has_key(t_word):
            self.page_dic[t_word] = [(t_beg,t_end)]
        else:
            self.page_dic[t_word].append((t_beg,t_end))
    
class BackwardItem:
    doc_id = 0
    hit_list = []
    
    def __init__(self,t_doc_id,t_hit_list):
        self.doc_id = t_doc_id
        self.hit_list = t_hit_list[:]
            
    

class ParseControl:
    word_id_count = 1
    word_id_dic = {}
    id_word_dic = {}
    forward_table_lis = []   #list of ForwardTableObj
    backward_table_dic = {}  #list of BackwardTableObj
    
    cmp_func = operator.attrgetter('word_id')
    
    def __init__(self,t_count):
        self.word_id_count = t_count
        self.forward_table_lis = []
        self.backward_table_dic = {}

    def addWordIdDic(self,word): #if word not in dictionary, add it,else return the wordId
        if not self.word_id_dic.has_key(word): #add into wordDic
            self.word_id_dic[word] = self.word_id_count
            self.id_word_dic[self.word_id_count] = word
            self.word_id_count = self.word_id_count + 1
                    

    def addPageDic(self,t_doc_id,t_page_dic):
        for word in t_page_dic:
            self.forward_table_lis.append(ForwardTableObj(t_doc_id,self.word_id_dic[word],t_page_dic[word]))
            
    def sortedForwardTable(self):
        self.forward_table_lis.sort(key = self.cmp_func)

    def createBackwardTable(self):
        for forwardObj in self.forward_table_lis:
            if not self.backward_table_dic.has_key(forwardObj.word_id): #if new element
                self.backward_table_dic[forwardObj.word_id] = [BackwardItem(forwardObj.doc_id,forwardObj.hits)]
            else:
                self.backward_table_dic[forwardObj.word_id].append(BackwardItem(forwardObj.doc_id,forwardObj.hits))
    
    def saveWordIdDic(self,b_cursor):
        b_cursor.execute("CREATE TABLE IF NOT EXISTS wordIdTable (word TEXT PRIMERY KEY,id INTEGER)")
        for obj in self.word_id_dic:
            b_cursor.execute("INSERT INTO wordIdTable (word,id) VALUES (?,?)",(obj,self.word_id_dic[obj]))
     
                
def dealOnePage(control_obj,dataObj):
    title = dataObj[1].lower()
    cur_page_dic = PageDic()
        
    seg_list = jieba.tokenize(title,mode = "search")  #get segment list

    for tk in seg_list:
        ############################################### deal for a word in a page
        control_obj.addWordIdDic(tk[0]) #add to word to wrodId dictionary
        cur_page_dic.addPageItem(tk[0],tk[1],tk[2])
        
    #add current word-hits dictionary to an object
    control_obj.addPageDic(dataObj[0],cur_page_dic.page_dic)   

def saveOneTable(control_obj,b_cursor):
        #create backwardTable

    
    b_cursor.execute('''CREATE TABLE IF NOT EXISTS backwardIndexTable
        (wordId INTEGER PRIMARY KEY,nDocs INTEGER,offset INTEGER)''')
    
    b_cursor.execute('''CREATE TABLE IF NOT EXISTS backwardTable
        (docIdIndex INTEGER PRIMARY KEY AUTOINCREMENT,docId INTEGER,nHits INTEGER,hitList text)''')
    
    current_offset = 0
    for wordObj in control_obj.backward_table_dic:
        ndocs = len(control_obj.backward_table_dic[wordObj])
        b_cursor.execute('INSERT INTO backwardIndexTable (wordId,nDocs,offset) VALUES (?,?,?)',(wordObj,ndocs,current_offset))
        current_offset = current_offset + ndocs
        
        for obj in control_obj.backward_table_dic[wordObj]:
            nhit_str = ''
            nhits = len(obj.hit_list)
            for hit_tuple in obj.hit_list:
                nhit_str = nhit_str + '%d '%hit_tuple[0] + '%d '%hit_tuple[1]
            
            b_cursor.execute('INSERT INTO backwardTable(docId,nHits,hitList) VALUES (?,?,?)',(obj.doc_id,nhits,nhit_str))
    
    b_cursor.execute('INSERT INTO backwardTable(docId,nHits,hitList) VALUES (?,?,?)',(obj.doc_id,nhits,nhit_str))
    

def createFTable():
    #import orgData
    conn = sqlite3.connect("orgDb.db")
    cursor = conn.cursor()
    
    b_conn = sqlite3.connect("backwardTableDb.db")
    b_cursor = b_conn.cursor()
    
    #parse orgData
    lineNum = 10000
    cursor.execute('SELECT * FROM orgTable')
    orgData = cursor.fetchmany(lineNum)  #each Forward Table have lineNum lines
    
    word_id_count = 1
    
    while len(orgData) != 0:
        #define a parse_control for one table
        control_obj = ParseControl(word_id_count)
        ######################################################## deal for a page of backwardTable ##############################
        for dataObj in orgData:
            ################################################### deal for a page in a backwardTable
            dealOnePage(control_obj,dataObj)
    
        control_obj.sortedForwardTable() 
        control_obj.createBackwardTable()    
        saveOneTable(control_obj,b_cursor)    
        print '-----------------------------------------'
    #parse forwardTable     
    #for obj in control_obj.forward_table_lis:
    #    print obj.doc_id,control_obj.id_word_dic[obj.word_id],
    #    print obj.word_id,
    #    for i in obj.hits:
    #        print i,
    #    print "\n------------------------------------"
             
    #parse backwardTable
    #
    #    for word in control_obj.backward_table_dic:
    #        for j in control_obj.backward_table_dic[word]:
    #            print word,j.doc_id,
    #            for s in j.hit_list:
    #                print s,
    #        
        word_id_count = control_obj.word_id_count         
        orgData = cursor.fetchmany(lineNum)
    
    control_obj = ParseControl(word_id_count)
    control_obj.saveWordIdDic(b_cursor)
    
    b_conn.commit()
    b_conn.close()
    conn.close()
    
#if __name__ == "__main__":
   # createFTable()