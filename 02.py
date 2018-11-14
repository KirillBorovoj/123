import csv, sys, os, requests, datetime, time
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import urllib.request
import pandas as pd
import sqlite3 as lite
from itertools import groupby
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Pool
from threading import Thread

import selenium
from selenium import webdriver

column = ['link', 'name', 'path_1', 'path_2', 'path_3', 'path_4', 'path_5',
          'price', 'oldprice', 'Product_overview', 'SKU', 'Weight', 'Plu', 'Size&Package', 'Maximum Quantity Allowed per customer',
          'Flavour', 'Country_of_manufacture', 'Chinese',     
          'pict_1', 'pict_2', 'pict_3', 'pict_4', 'pict_5', 'pict_6',
          'pict_7', 'pict_8', 'pict_9', 'pict_10', 'img-alt_1',
          'img-alt_2', 'img-alt_3', 'img-alt_4', 'img-alt_5',
          'img-alt_6', 'img-alt_7', 'img-alt_8', 'img-alt_9',
          'img-alt_10', 'brand', 'stock', 'Meta_Description', 'Meta_Keyword']


class GetBrand:
    def get_html(self, url):
        headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'}
        r = requests.get(url, headers=headers)
        return r.content

    def get_brand(self,):
        print('collecting BRANDS')
        con = lite.connect('InfoBox.db', timeout=1)
        cur = con.cursor()
        
        cur.execute("DELETE FROM Brand")
        cur.execute("DELETE FROM GoodLinks")
        cur.execute("DELETE FROM Data")
        cur.execute("UPDATE control SET brand = 0")
        con.commit()

        url = 'https://www.mrvitamins.com.au/brands'
        
        soup = BeautifulSoup(self.get_html(url), 'html.parser')
        block = soup.find('div',{'id':'center-main'})
        rows = block.find_all('div',{'class':'title'})

        box = []
        for row in rows:
            link = row.find('a').get('href')
            box.append(link)
            
        for link in box:
            soup = BeautifulSoup(self.get_html(link), 'html.parser')
            name = soup.find('h2').text.split('(')[0].strip()
            html = str(soup.find('div', {'class':'desc'}))
            aa = [name, link, html]
            
            cur.execute("INSERT INTO Brand VALUES(?,?,?);", [aa[0],aa[1],aa[2]])
            con.commit()
            
        cur.execute("UPDATE control SET brand = 1")
        con.commit()
        cur.close()

class GetLinks:
    def start(self, links):
        print('collecting LINKS')
        browser = webdriver.PhantomJS('./phantomjs/bin/phantomjs')
        for link in links:
            print(link)
            numb_brand = links.index(link)
            browser.get(link)
            # Get scroll height
            LH = browser.execute_script("return document.body.scrollHeight")
            PH = 0
            while True:
                # Scroll down to bottom
                browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                # Wait to load page
                time.sleep(5)
                # Calculate new scroll height and compare with last scroll height
                NH = browser.execute_script("return document.body.scrollHeight")
                if NH == LH:
                    self.scroll(browser, numb_brand)
                    break
                else:
                    if PH == LH: break
                    PH = LH
                    self.scroll(browser, numb_brand)
        browser.quit()

    def scroll(self, browser, numb_brand):
        con = lite.connect('InfoBox.db', timeout=1)
        cur = con.cursor()
        soup = BeautifulSoup(browser.page_source, 'html.parser')
        block = soup.find('div', {'class':'category-products'})
        try:
            box = block.find('ul')
            grid = box.find_all('li')
            for name in grid:
                names = name.find('h2', {'class':'product-name'})
                for ll in names:
                    links = ll.find('a')
                    if 'href' in ll.attrs:
                        link = ll.attrs['href']
                        cur.execute("INSERT INTO GoodsLink VALUES(?);", [link])
                        con.commit()
        except:pass
        cur.execute("UPDATE control SET brand_numb =" + str(numb_brand))
        con.commit()        
        cur.close()


        
class GetData:
    def pictures(self, pict_link, file):
          img = requests.get(pict_link) 
          out = open(file, "wb") 
          out.write(img.content) 
          out.close()

    def Product_Overview(self, dd, info):
        try: overview = dd.find('div', {'class':'tab-content product-overview'})
        except: overview = ''
        info.update({'Product_overview': str(overview)})
        return info

    def China(self, dd, info):
        chinese = dd.find('td', {'class':'std'})
        info.update({'Chinese': str(chinese)})
        return info

    def Spec(self, dd, info):
        rows = dd.find_all('tr')
        for row in rows:
            lable = row.find('h4').text.strip().replace('\n','').replace(':','').replace(';','')
            val = row.find('td', {'class':'std'}).text.strip().replace('\n','')
            info.update({lable: val})
        return info

    def price_(self, soup):
        try:      
            prices = soup.find('div',{'class':'price-box'})
            if prices != 'None':
                p_price = prices.find('p', {'class':'special-price'})
                price = p_price.find('span', {'class':'price'}).text.split('AU$ ')[1]
            else:
                p_price = prices.find('span', {'class':'regular-price'})
                price = p_price.find('span', {'class':'price'}).text.split('AU$ ')[1]
        except: price = ''
        try:
            p_old_price = prices.find('p', {'class':'old-price'})
            old_price = p_old_price.find('span', {'class':'price'}).text.split('AU$ ')[1]
        except: old_price = ''
        
        return price, old_price

    def path_(self, soup, info):
        try:
            ppp = soup.find('div',{'class':'breadcrumbs'})
            pp_box = ppp.find_all('ul')
            nn = 1
            for kkk in pp_box:
                rows = kkk.find_all('li')[1:]
                line = ''
                for row in rows:
                    line = line + ' / ' + row.text.replace('>','').strip()
                    line = line[1:]
                path = 'path_' + str(nn)
                info.update({path: line})
                nn += 1
        except: path = ''

        return info
        

    def get_data(self, link):
        print(link)
        print('')
        soup = BeautifulSoup(get_html(link), 'html.parser')
        hh = soup.find('div',{'class':'product-name'})                       #-----------NAME-------------------------------
        name = hh.find('span', {'class':'h1'}).text
        meta_disr = soup.find('meta',{'name':'description'}).get('content')  #---------META--------------------------------------
        meta_key = soup.find('meta',{'name':'keywords'}).get('content')
        brand = soup.find('div',{'class':'brand_link'}).a.text               #----------BRAND----------------------------
        available = soup.find('div',{'class':'extra-info'}).p
        stock = available.find('span', {'class':'value'}).text
        prices = price_(soup)
        price = prices[0]
        old_price = prices[1]

        info = {'link':link,
                'oldprice':old_price,
                'price': price,
                'name': name,
                'brand':brand,
                'stock': stock,
                'Meta_Description':meta_disr,
                'Meta_Keyword':meta_key}

        info = path_(soup, info)

        block = soup.find('dl', {'id':'collateral-tabs'})
        dt = len(block.find_all('dt'))
        ii = 0
        while ii < dt:
            lable = block.find_all('dt')[ii].text
            dd = block.find_all('dd')[ii]
            if lable == 'Product Overview': info = Product_Overview(dd, info)
            if lable == '中文说明': info = China(dd, info)
            if lable == 'Specifications': info = Spec(dd, info)
            ii += 1

        try:
            box = soup.find('div',{'class':'pic-info'})
            obj = box.find('div',{'class':'product-pic'})            
            img = obj.find('img').get('src')
            alt_1 = obj.img.get('alt')
            info.update({'img-alt_1': alt_1})
            pict = name + '_1' + '.jpg'
            foto = './Pictures_1/' +  pict
            pictures(img, foto)
            info.update({'pict_1': pict})
        except: pict = ''
        
        con = lite.connect('InfoBox.db', timeout=3)
        cur = con.cursor()
        aaa = []
        for ll in column:  aaa.append(info.get(ll))

        cur.execute("INSERT INTO Data VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",
                   [aaa[0],aaa[1],aaa[2],aaa[3],aaa[4],aaa[5],aaa[6],aaa[7],aaa[8],aaa[9],aaa[10],aaa[11],aaa[12],aaa[13],aaa[14],
                    aaa[15],aaa[16],aaa[17],aaa[18],aaa[19],aaa[20],aaa[21],aaa[22],aaa[23],aaa[24],aaa[25],aaa[26],aaa[27],aaa[28],
                    aaa[29],aaa[30],aaa[31],aaa[32],aaa[33],aaa[34],aaa[35],aaa[36],aaa[37],aaa[38],aaa[39],aaa[40],aaa[41]])
        con.commit()
        cur.close()

gb = GetBrand()
gl = GetLinks()
gd = GetData()
def main():
    global end    
    end = 0 
    con = lite.connect('InfoBox.db', timeout=1)
    cur = con.cursor()
    
    df = pd.read_sql('select * from Brand', con) 
    dfb = df.drop_duplicates(subset=['links'], keep='first')
    brand_links = dfb['links'].tolist()
    
    df = pd.read_sql('select * from control', con)
    ind_brand = list(df['brand'])[0]
    numb_brand = list(df['brand_numb'])[0]
    con.close()
    
    if ind_brand == 0: gb.get_brand()

    if len(brand_links) > numb_brand: gl.start(brand_links[numb_brand:])

    df = pd.read_sql('select * from GoodsLink', con)
    dfd = df.drop_duplicates(subset=['link'], keep='first')
    inp_links = dfd['link'].tolist()

    df = pd.read_sql('select * from Data', con) 
    data_link = df['link'].tolist()

    links = []
    for ii in inp_links: 
        if ii in data_link: continue
        links.append(ii)
    cur.close()

    pool = ThreadPool(3)  ##Колличество потоков
    pool.map(gd.get_data, links[501:1001])
    pool.close()

    print('**********************************************************')
    end = 1

def re_start():
    time.sleep(30)
    period = 30 ## time of period checking (seconds)
    pause = 30 ## pause in record to file "out_data.csv"  (seconds)
    for ii in range(0, 10000, 1):
        time.sleep(period)
        time_last = os.path.getmtime('InfoBox.db', timeout=1)
        time_now = datetime.timestamp(datetime.now())
        delta_time = (time_now - time_last)
        nn = 1
        if delta_time > pause and ii > 0:
            print(nn, '+++++++++++++++++++++++++++++++++++++')
            nn += 1
            if end == 1: break
            Thread(target = main).start()
        else:
            print('---------------------------------------------')
            if end == 1: break
    print('Finish')
       
if __name__ == '__main__':
    Thread(target = main).start()
    Thread(target = re_start).start()





