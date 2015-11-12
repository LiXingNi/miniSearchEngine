import scrapy 
from scrapy import Request
from itertools import count

#deal ascii wrong
import sys
reload(sys)
sys.setdefaultencoding('gbk')

#deal database
import sqlite3

class rsSpider(scrapy.spiders.Spider):
    name = "zhidao"
    #download_delay = 1  
    allowed_domains = ["zhidao.baidu.com"]
    url_first = 'http://zhidao.baidu.com/question/'
    start_urls = ["http://zhidao.baidu.com/question/647795152324593805.html", #python
                  "http://zhidao.baidu.com/question/23976256.html", #database
                  "http://zhidao.baidu.com/question/336615223.html", #C++
                  "http://zhidao.baidu.com/question/251232779.html", #operator system
                  "http://zhidao.baidu.com/question/137965104.html"  #Unix programing
                  ]
   
   
   #add to file
    outputFile = open('test.txt','w')
    
    #add database
    connDataBase = sqlite3.connect("zhidao.db")
    cDataBase = connDataBase.cursor()
    cDataBase.execute('''CREATE TABLE IF NOT EXISTS infoLib 
        (id INTEGER PRIMARY KEY AUTOINCREMENT,name text,url text,html text)''')
    
    #url dataBase
    cDataBase.execute('''CREATE TABLE IF NOT EXISTS urlLib
        (url text PRIMARY KEY)''')



    def parse(self,response):
        pageName = response.xpath('//title/text()').extract()[0]
        pageUrl = response.xpath("//head/link").re('href="(.*?)"')[0]
        pageHtml = response.xpath("//html").extract()[0]
        
        # judge whether pageUrl in cUrl
        if pageUrl in self.start_urls:
            self.cDataBase.execute('SELECT * FROM urlLib WHERE url = (?)',(pageUrl,))
            lines = self.cDataBase.fetchall()
            if len(lines):
                pass
            else:
                 self.cDataBase.execute('INSERT INTO urlLib (url) VALUES (?)',(pageUrl,))
                 self.cDataBase.execute("INSERT INTO infoLib (name,url,html) VALUES (?,?,?)",(pageName,pageUrl,pageHtml))
        else:
            self.cDataBase.execute('INSERT INTO urlLib (url) VALUES (?)',(pageUrl,))
            self.cDataBase.execute("INSERT INTO infoLib (name,url,html) VALUES (?,?,?)",(pageName,pageUrl,pageHtml))
        
        self.connDataBase.commit()
        
        #write to file
        self.outputFile.write(pageName)
        self.outputFile.write(pageUrl)
        print "-----------------------------------------------"
        

        
        for sel in response.xpath('//ul/li/a').re('href="(/question/.*?.html)'):
            sel = "http://zhidao.baidu.com" + sel
            self.cDataBase.execute('SELECT * FROM urlLib WHERE url = (?)',(sel,))
            lines = self.cDataBase.fetchall()
            if len(lines) == 0:
                yield Request(url = sel, callback=self.parse)