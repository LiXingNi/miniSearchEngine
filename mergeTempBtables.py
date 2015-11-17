import sqlite3

class MergeTwo:
    def __init__(self,db1,db2):
        self.offset = 0
        db_name1 = "backwardTables\\orgDb%s.db"%db1
        db_name2 = "backwardTables\\orgDb%s.db"%db2
        db_name = "backwardTables\\orgDb%s_%s.db"%(db1,db2)
        self.conn1 = sqlite3.connect(db_name1)
        self.conn2 = sqlite3.connect(db_name2)
        self.conn = sqlite3.connect(db_name)
        self.cursor1 = self.conn1.cursor()
        self.cursor2 = self.conn2.cursor()
        self.cursor = self.conn.cursor()
        
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS backwardIndexTable
        (wordId INTEGER PRIMARY KEY,nDocs INTEGER,offset INTEGER)''')
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS backwardTable
        (docIdIndex INTEGER PRIMARY KEY AUTOINCREMENT,docId INTEGER,nHits INTEGER,hitList text)''')
    
    
    def mergerFun(self):
        self.cursor1.execute("SELECT * FROM backwardIndexTable")
        self.cursor2.execute("SELECT * FROM backwardIndexTable")
        line_num = 1000
        
        org_data1 = self.cursor1.fetchmany(line_num)
        org_data2 = self.cursor2.fetchmany(line_num)
        
        org_data1 = list(org_data1)
        org_data2 = list(org_data2)
        
        while len(org_data1) != 0 and len(org_data2) != 0 :
            [org_data1,org_data2] = self.mergeData(org_data1,org_data2)
            tmp_data1 = self.cursor1.fetchmany(line_num - len(org_data1))
            tmp_data2 = self.cursor2.fetchmany(line_num - len(org_data2))
            tmp_data1 = list(tmp_data1)
            tmp_data2 = list(tmp_data2)
            org_data1.extend(tmp_data1)
            org_data2.extend(tmp_data2)
        
        if len(org_data1) == 0 and len(org_data2) == 0:
            return
        else:
            if len(org_data1) == 0:
                while len(org_data2) != 0:
                    self.writeOrgData(2,org_data2)
                    org_data2 = self.cursor2.fetchmany(line_num * 2)
                    org_data2 = list(org_data2)
            else:
                while len(org_data1) != 0:
                    self.writeOrgData(1,org_data1)
                    org_data1 = self.cursor1.fetchmany(line_num * 2)
                    org_data1 = list(org_data1)
                        
        self.conn.commit()
        self.conn.close()
        self.conn1.close()
        self.conn2.close()
        
        
    def mergeData(self,org_list1,org_list2):
        len1 = len(org_list1)
        len2 = len(org_list2)
        i = 0
        j = 0
        
        while i < len1 and j < len2:
            list1 = org_list1[i]
            list2 = org_list2[j]
            [word_id_1,ndocs_1,offset_1] = [list1[0],list1[1],list1[2]]
            [word_id_2,ndocs_2,offset_2] = [list2[0],list2[1],list2[2]]
            if word_id_1 == word_id_2:
                self.writeData(1,ndocs_1,offset_1)
                self.writeData(2,ndocs_2,offset_2)
                self.cursor.execute('''INSERT INTO backwardIndexTable(wordId,nDocs,offset)
                                        VALUES(?,?,?)''',(word_id_1,ndocs_1 + ndocs_2,self.offset))
                self.offset += ndocs_1 + ndocs_2
                i += 1
                j += 1
            else:
                if word_id_1 < word_id_2:
                    self.writeData(1,ndocs_1,offset_1)
                    self.cursor.execute('''INSERT INTO backwardIndexTable (wordId,nDocs,offset)
                                        VALUES(?,?,?)''',(word_id_1,ndocs_1,self.offset))
                    self.offset += ndocs_1
                    i += 1
                else:
                    self.writeData(2,ndocs_2,offset_2)
                    self.cursor.execute('''INSERT INTO backwardIndexTable (wordId,nDocs,offset)
                                        VALUES(?,?,?)''',(word_id_2,ndocs_2,self.offset))
                    self.offset += ndocs_2
                    j += 1
            
        left_list = [org_list1[i:],org_list2[j:]]
        return left_list
                
    
    def writeData(self,num,ndocs,offset):
        beg_index = offset + 1
        end_index = beg_index + ndocs - 1
        
        if num == 1:
            c_cursor = self.conn1.cursor()
        else:
            c_cursor = self.conn2.cursor()
        
        c_cursor.execute("SELECT docId,nHits,hitList FROM backwardTable WHERE docIdIndex between (?) and (?)",(beg_index,end_index))
        total_info = c_cursor.fetchall()
        total_info = list(total_info)
        self.cursor.executemany("INSERT INTO backwardTable (docId,nHits,hitList) VALUES(?,?,?)",total_info)
      
    
    def writeOrgData(self,num,org_data):
        for data in org_data:
            [c_word_id,c_ndocs,c_offset] = [data[0],data[1],data[2]]
            self.writeData(num, c_ndocs, c_offset)
            self.cursor.execute('''INSERT INTO backwardIndexTable(wordId,nDocs,offset)
                                    VALUES(?,?,?)''',(c_word_id,c_ndocs,self.offset))
            self.offset += c_ndocs
                

'''
if __name__ == "__main__":
    lis = [0,2,4,6,8]
    for i in lis:
        merge = MergeTwo(i,i+1)
        merge.mergerFun()
    print "end"
 '''         
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                