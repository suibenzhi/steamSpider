# -*- coding: utf-8 -*-
'''
    旨在爬取当前steam优惠游戏信息，包括游戏分类、评分、标签、降价幅度、优惠截止日期
'''

import time
import requests
from bs4 import BeautifulSoup
import csv
import threading

thread_num_gl = 50

lock_gl = threading.Lock()


# 返回对应url的页面
def getHtmlText(url):
    try:
        r = requests.get(url)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        return r.text
    except:
        return ""


# 获取最大页面数,只运行一次
def getMaxPageNum(start_url):
    html = getHtmlText(start_url)
    #    print(html)
    soup = BeautifulSoup(html, 'html.parser')
    max_page_num = soup.find_all('a', onclick="SearchLinkClick( this ); return false;")[-2].text
    max_page_num_i = int(max_page_num)
    print("max_page_num:", max_page_num_i)

    return max_page_num_i


class SteamSpider:

    # 定义构造器
    def __init__(self, baseUrl, csvWriter, max_page_num, thread_num):
        self.baseUrl = baseUrl
        self.csvWriter = csvWriter
        self.max_page_num = max_page_num
        self.thread_num = thread_num
        #        self.No = No
        #        self.lock = threading.Lock()
        self.lock = lock_gl

    def getGameList(self, html):
        try:
            soup = BeautifulSoup(html, 'html.parser')
            Games = soup.find_all('a', class_='search_result_row')
            return Games
        except Exception as e:
            print('error1', str(e))

    # 获得每个游戏的详细信息并存入文件
    def getGameInfo(self, gamelist, filename, No):
        for game in gamelist:
            try:
                name = game.find_all('span', attrs={'class': "title"})[0].text
                link = game['href']
                original_price = game.find_all('strike')[0].text.strip()
                current_price = game.find_all('div', attrs={'class': "discounted"})[0].text.replace(original_price,
                                                                                                    '').strip()
                #            releaseday = game.find_all('div', attrs={'class':"search_released"})[0].text
                discount = game.find_all('div', attrs={'class': "search_discount"})[0].text.strip()

                # 加锁
                #                self.lock.acquire()

                # 写入csv文件
                #            if(discount == '-100%') :
                #                csvWriter.writerow([name, current_price, original_price, discount,
                #                                releaseday, scoreDict.get(score,'unknown'), review, link])
                csvFile = open(filename, 'a', encoding='utf-8', newline='')
                csvWriter = csv.writer(csvFile, delimiter=',', lineterminator='\n')
                csvWriter.writerow([No, name, current_price, original_price, discount, link])
                csvFile.close()
                print(str(No) + ' ' + name + ' ' + current_price + ' ' + original_price + ' ' + discount + ' ' + link)
                # 修改完成，释放锁
            #                self.lock.release()
            except Exception as e:
                #           print('error2', str(e))
                continue

    def run(self, No):
        filename = './tmp/tmp' + str(No) + '.csv'
        csvFile = open(filename, 'w', encoding='utf-8', newline='')
        csvFile.truncate()
        # csvWriter = csv.writer(csvFile, delimiter=',', lineterminator='\n')
        csvFile.close()
        for i in range(self.max_page_num):
            if ((i + 1) % self.thread_num == No):
                pageUrl = self.baseUrl + str(i + 1)
                print(str(No) + ' ' + str(pageUrl))
                self.getGameInfo(self.getGameList(getHtmlText(pageUrl)), filename, No)

        # csvFile.close()
        csvRead = csv.reader(open(filename, 'r'))
        try:
            # 加锁
            self.lock.acquire()
            self.csvWriter.writerows(csvRead)
        except Exception as e:
            print(str(No) + ' ' + 'writerrows error!')
        finally:
            # 修改完成，释放锁
            self.lock.release()

        print(str(No) + ' ' + 'writerrows finished!')
        # csvRead.close()


def spiderMain(baseUrl, csvWriter, max_page_num, thread_num, No):
    ss = SteamSpider(baseUrl, csvWriter, max_page_num, thread_num)
    ss.run(No)


def main():
    startTime = time.time()
    urlFile = open('url.text', 'r')
    urlList = urlFile.readlines()
    startUrl = urlList[0]
    baseUrl = urlList[1]

    filename = time.strftime("%Y%m%d_%H%M%S", time.localtime()) + '.csv'
    csvFile = open(filename, 'w', encoding='utf-8', newline='')
    csvWriter = csv.writer(csvFile, delimiter=',', lineterminator='\n')
    #    csvWriter.writerow(['名称', '当前价格', '初始价格', '折扣',
    #                        '发行日期', '评分', '评测情况', '游戏主页'])
    csvWriter.writerow(['线程编号', '名称', '当前价格', '初始价格', '折扣', '游戏主页'])

    Max_page_Num = getMaxPageNum(startUrl)
    #    Max_page_Num = 100

    thread_num = thread_num_gl
    threads = []

    for i in range(thread_num):
        t = threading.Thread(target=spiderMain, args=(baseUrl, csvWriter, Max_page_Num, thread_num, i))
        t.start()
        #        t.join() # 设置主线程等待子线程结束
        threads.append(t)

    for t in threads:
        t.join()

    #    Max_page_Num = 2881
    '''
    for i in range(Max_page_Num):
        pageUrl = baseUrl + str(i + 1)
        print(pageUrl)
        getGameInfo(getGameList(getHtmlText(pageUrl)), csvWriter)
    '''

    ''' 
    for i in range(thread_num):
        tmp = 'tmp' + str(i) + '.csv'
#        csvTmp = open(tmp, 'r', encoding='utf-8',newline='')
#        csvRead = csv.Read(csvTmp, delimiter=',', lineterminator='\n')
        csvRead = csv.reader(open(tmp,'r'))
        csvWriter.writerows(csvRead)
    '''
    csvFile.close()

    endTime = time.time()
    print('总耗时为%f秒' % round(endTime - startTime, 2))


if __name__ == '__main__':
    main()
