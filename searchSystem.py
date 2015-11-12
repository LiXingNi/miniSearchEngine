import jieba
import sqlite3

class BackwardObj:
    doc_id = 0
    nhits = 0
    hit_list = ''
    
    def __init__(self,t_doc_id,t_nhits,t_hit_list):
        self.doc_id = t_doc_id
        self.nhits = t_nhits
        self.hit_list = t_hit_list[:]

class CentralControl:
    conn = sqlite3.connect("backwardTableDb.db")
    cursor = conn.cursor()
    
    org_conn = sqlite3.connect("orgDb.db")
    org_cursor = org_conn.cursor()
    
    word_id_dic = {}
    
    def __init__(self):
        self.loadWordIdDic()
    
    def loadWordIdDic(self):
        self.cursor.execute("SELECT * FROM wordIdTable")
        word_id_data = self.cursor.fetchall()
        
        for word_id_obj in word_id_data: 
            self.word_id_dic[word_id_obj[0]] = word_id_obj[1]
    
    def findKeyWordDocId(self,key_word):
        ndocs_offset = []
        if not self.word_id_dic.has_key(key_word):
            ndocs_offset.append(0)
            ndocs_offset.append(0)
            return ndocs_offset
        word_id = self.word_id_dic[key_word]
        self.cursor.execute("SELECT * FROM backwardIndexTable WHERE wordId = (?)",(word_id,))
        info = self.cursor.fetchone()
        ndocs_offset.append(info[1])
        ndocs_offset.append(info[2])
        return ndocs_offset
    
    def getSetFromBT(self,ndocs,offset):
        current_set = []
        doc_id_index_beg = offset + 1
        doc_id_index_end = doc_id_index_beg + ndocs - 1
        self.cursor.execute("SELECT * FROM backwardTable WHERE docIdIndex between (?) and (?)",(doc_id_index_beg,doc_id_index_end))
        total_info = self.cursor.fetchall();
        for info in total_info:
            current_set.append(BackwardObj(info[1],info[2],info[3]))  
        return current_set

    def getDocSet(self,key_word):
        current_set = []
        [ndocs,offset] = self.findKeyWordDocId(key_word)
        if ndocs == 0 and offset == 0 :    #the key word not in dictionary
            return current_set
        current_set = self.getSetFromBT(ndocs,offset)
        return current_set

    def twoIntersect(self,first_set,second_set):
        result_set = []
        first_len = len(first_set)
        second_len = len(second_set)
        first = 0
        second = 0
        
        while first < first_len and second < second_len :
            doc_id_1 = first_set[first].doc_id
            doc_id_2 = second_set[second].doc_id
            
            if doc_id_1 < doc_id_2 :
                first = first + 1
            else :
                if doc_id_1 > doc_id_2 :
                    second = second + 1
                else:
                    result_set.append(first_set[first])
                    first = first + 1
                    second = second + 1
        return result_set

    def getIntersection(self,set_dict):
        intersection_set = []
        if len(set_dict) == 0:
            return intersection_set;
        flag = False
        for key in set_dict:
            if(flag) :
                intersection_set = self.twoIntersect(intersection_set,set_dict[key])
            else:
                flag = True
                intersection_set = set_dict[key]
                
        return intersection_set

    def getDocIdScores(self,set_dic):
        doc_score_dic = {}
        for key in set_dic:
            for obj in set_dic[key]:
                if doc_score_dic.has_key(obj.doc_id):
                    doc_score_dic[obj.doc_id] += 1
                else:
                    doc_score_dic[obj.doc_id] = 1
        doc_score_set = sorted(doc_score_dic.iteritems(),key = lambda d : d[1],reverse = True)
        return doc_score_set
    
    
    def oneSearch(self,query):
        query = query.lower()
        seg_list = jieba.cut_for_search(query)
        seg_list = list(seg_list)
        if ' ' in seg_list:   #deal ' '
            seg_list = [i for i in seg_list if i != ' ']
        set_dic = {}
        for key_word in seg_list:
            current_set = self.getDocSet(key_word) #the key word may be not in the dictionary
            if len(current_set) != 0 :
                set_dic[key_word] = current_set
            
            print key_word    
                #check partition
                #for obj in current_set:
                #    print key_word,obj.doc_id,obj.nhits,obj.hit_list
        doc_set = self.getDocIdScores(set_dic)
        
        if len(doc_set) != 0:
            self.printDocInfo(doc_set)
        else:
            print "not find information"
            
    def printDocInfo(self,doc_set):
        index = 0
        while index < 8:
            doc_id = doc_set[index][0]
            hit_times = doc_set[index][1]
            index += 1
            self.org_cursor.execute("SELECT title, url FROM orgTable WHERE id == (?)",(doc_id,))
            info = self.org_cursor.fetchone()
            
            print "title : ",info[0]
            print "url : ",info[1]
            print "hits : ",hit_times
            print "------------------------------------"
        
if __name__ == "__main__":
    central_control = CentralControl()
    while True:
        query = raw_input("input query:")
        central_control.oneSearch(query)

    central_control.conn.close()
    central_control.org_conn.close()
    