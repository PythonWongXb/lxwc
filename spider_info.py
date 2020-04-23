#!/usr/bin/env python
# coding: utf-8

import requests
from bs4 import BeautifulSoup
import time
import random
import pandas as pd
from sqlalchemy import create_engine
import re
import json
from concurrent.futures import ThreadPoolExecutor

def f(page, num):
    '''获取page下的num，页数下的具体url，打印的就是'''
    url = 'http://www.lxwc.com.cn/topic-{}-{}.html'.format(page, num)
    print(url)
    cls = 's xst'
    ty = 'a'
    headers1 = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 QIHU 360SE'
    }
    response_1 = requests.get(url, headers=headers1)
    res = response_1.text
    soup = BeautifulSoup(res, 'lxml')  # 推荐使用 lxml, 容错性好
    c = soup.find_all(ty, class_=cls)
    return c


def save_info(f, db):
    '''
    :param: f --> list of 2 level
    db --> str the name of database
    '''

    global DATABASE
    global USERNAME
    global PASSWORD

    HOSTNAME = "127.0.0.1"
    PORT = "3306"
    DATABASE = DATABASE
    USERNAME = USERNAME
    PASSWORD = PASSWORD

    DB_URI = "mysql+pymysql://{username}:{password}@{host}:{port}/{db}?charset=utf8".format(username=USERNAME,
                                                                                            password=PASSWORD,
                                                                                            host=HOSTNAME, port=PORT,
                                                                                            db=DATABASE)
    engining = create_engine(DB_URI)
    f.to_sql(db, con=engining, if_exists='append', index=False)


def main_func(two_params):
    '''主程序 获取信息到数据库
    获取文章的百度网盘地址
    page信息 等
    获取图片
    获取所有的评论页数
    遍历页数，获取评论
    '''
    page = two_params[0]
    num = two_params[1]

    time.sleep(random.random() * 3)
    d = []
    for i in f(num, page):
        message = []
        p = []
        #         message = {}
        time.sleep(random.random() * 3)
        url = 'http://www.lxwc.com.cn/' + i.get('href')
        title = i.get_text()
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 QIHU 360SE',
        }

        # 及时替换cookie http://www.lxwc.com.cn/home.php?mod=spacecp&ac=pm&op=checknewpm&rand=1586356123

        #         cookie = 'w2hd_8acf_saltkey=suV8H1nU; w2hd_8acf_lastvisit=1586342456; w2hd_8acf_nofavfid=1; w2hd_8acf_visitedfid=67D70; w2hd_8acf_ulastactivity=6830vJdFsEZfUPG3rWOscTGahAwEFtG6JBxClHnAipGjnNxbr%2FvB; w2hd_8acf_lip=111.193.90.27%2C1586512460; w2hd_8acf_sendmail=1; w2hd_8acf_auth=248cDXxaoN3H%2FLCgDrKmJI6GlGQOm%2FMRJeV0VlfFZho4055UDyiz4pFtLIeyDg21SaUFla1OQl7aeOcUsBut%2BUWCJqd8; w2hd_8acf_wxauth=ed8397955a81d8c4d0094e23f81a274d; w2hd_8acf_st_t=1557649%7C1586516540%7Cf2e05d220292df6e133d0fa66c5e41bf; w2hd_8acf_forum_lastvisit=D_70_1586431048D_67_1586516540; w2hd_8acf_checkpm=1; Hm_lvt_696f676523c9176b65220027136d9a6f=1586511345,1586511692,1586512000,1586515234; Hm_lpvt_696f676523c9176b65220027136d9a6f=1586516541; w2hd_8acf_lastact=1586516550%09forum.php%09viewthread; w2hd_8acf_st_p=1557649%7C1586516550%7Cfd0507574f74a93cd130d2dbb718e33b; w2hd_8acf_viewid=tid_106749; w2hd_8acf_sid=fi82uP'
        global cookie
        headers['cookie'] = cookie
        headers['Host'] = 'www.lxwc.com.cn'

        response_1 = requests.get(url, headers=headers)
        res = response_1.text

        soup = BeautifulSoup(res, 'lxml')  # 推荐使用 lxml, 容错性好
        if len(soup.find_all('a', target='_blank', rel="nofollow")) == 1:
            baidu = soup.find('a', target='_blank', rel="nofollow").get('href')
        else:
            x = soup.find_all('a', target='_blank', rel="nofollow")
            for y in x:
                if 'baidu' in y.get('href'):
                    baidu = y.get('href')
        al = soup.find('p', class_='xg1')
        author_id = al.find_all('a')[1].get('href').rsplit('-', 1)[-1][:-5]
        # .rsplit('-',1)[-1][:-5]
        t = al.text
        info = t.split()
        author = info[3]
        text_time = info[4][1:] + '-' + info[5][:-1]
        see = info[-3].split('/')[0]
        mark = info[-3].split('/')[-1][:-1]
        real_mark = info[-1]
        # 评论的页数
        #         num = int(real_mark) % 9 + 1
        try:
            num = soup.find('a', class_='last').text.split()[-1]
        except Exception as e:
            print(e)
            num = int(real_mark) % 9 + 1

        psw = 'fail'
        try:
            if identify(baidu):
                psw = identify(baidu)
        except Exception as e:
            print(e)
        article_id = i.get('href').split('-', 2)[1]
        message.append(title)
        message.append(article_id)
        message.append(author)
        message.append(author_id)
        message.append(text_time)
        message.append(see)
        message.append(mark)
        message.append(real_mark)
        message.append(num)
        message.append(baidu)
        message.append(psw)
        message.append(page)

        d.append(message)

        imgs = soup.find_all('img')
        if imgs:
            # print('有图片')
            for i in imgs:
                pic = []
                try:
                    file = i.get('zoomfile')
                except Exception as e:
                    print(e)
                if file:
                    pic.append(article_id)
                    pic.append(file)
                    p.append(pic)
                    pics = pd.DataFrame(p, columns=['article_id', 'pic'])
                    save_info(pics, 'pictures')
        try:
            get_comments(int(num), url)
        except Exception as e:
            print(e)
    d = pd.DataFrame(d, columns=['title', 'article_id', 'author', 'author_id', 'text_time', 'see', 'mark', 'real_mark',
                                 'num', 'baidu', 'psw', 'page'])
    save_info(d, 'wxb')


# In[9]:


def get_urls(url):
    '''得到top_url中的page'''
    dic = {}
    global headers1
    res = requests.get(url, headers=headers1).text
    soup = BeautifulSoup(res, "lxml")
    f = soup.find_all('dt')
    for i in f:

        if '失效' not in i.a.text:
            num = i.a.get('href').split('-', 2)[1]
            types = i.a.text
            dic[num] = types
    return dic


def get_last_number(num):
    '''得到最后的num'''
    global headers1
    url = 'http://www.lxwc.com.cn/topic-{}-1.html'.format(num)
    res = requests.get(url, headers=headers1).text
    soup = BeautifulSoup(res, "lxml")
    n = soup.find_all('div', class_='pg')[-1].text.rsplit(' ', 2)[1]

    return n


def get_num_and_page(url):
    '''获取num和page
    page是每个主题对应的号码
    num是每个page共有多页
    '''
    ls = []
    d = get_urls(url)
    for num in d:
        page = get_last_number(num)
        ls.append([num, page])
    return ls


def get_comments(num_c, url_c):
    '''param:
    num_c: 评论的页数
    url_c: 评论第一页
    '''
    for n in range(1, num_c + 1):
        pattern = pattern = re.compile(r'-\d-')
        url = re.sub(pattern, '-' + str(n) + '-', url_c)
        print(url)
        res = parse_c(url)


def parse_c(url):
    '''解析url，获取内容'''
    res = requests.get(url).text
    soup = BeautifulSoup(res, "lxml")
    comments = soup.find_all('div', class_="xld xlda mbm")[:-1]
    info = []

    post_author_id = soup.find_all('a')[1].get('href').rsplit('-', 1)[-1][:-5]

    for c in comments:
        message = []
        post_id = c.get('id')
        user_id = c.find('a').get('href')
        user_img = c.find('a').img.get('src')
        user_name = c.find('a', class_='xi2').text
        post_time = c.find('span', class_='xg1 xw0').text
        comment = c.find('td', class_='t_f').text.strip()
        author_id = c.find('em').get('id')
        # 在表中增加作者的url
        message.append(post_id)
        message.append(user_id)
        message.append(user_img)
        message.append(user_name)
        message.append(post_time)
        message.append(comment)
        message.append(author_id)
        info.append(message)
    info = pd.DataFrame(info,
                        columns=['post_id', 'user_id', 'user_img', 'user_name', 'post_time', 'comment', 'author_id'])
    save_info(info, 'comments')


import pymysql.cursors


def read():
    '''读取数据库'''
    conn = pymysql.connect(host='localhost',
                           user='root',
                           password='123456',
                           db='wxb',
                           charset='utf8')

    # 创建一个游标
    cursor = conn.cursor()

    # 查询数据
    sql = "select * from wxb"
    cursor.execute(sql)  # 执行sql

    # 查询所有数据，返回结果默认以元组形式，所以可以进行迭代处理
    # for i in cursor.fetchall():
    #     print(i)

    result_1 = cursor.fetchall()
    for i in result_1:
        try:
            if identify(i[-2]):
                update(identify(i[-2]), i[-2])
        except Exception as e:
            print(e)
            continue

    cursor.close()  # 关闭游标
    conn.close()  # 关闭连接


def update(x, y):
    '''更新数据库'''
    conn = pymysql.connect(host='localhost',
                           user='root',
                           password='123456',
                           db='wxb',
                           charset='utf8')

    # 创建一个游标
    cursor = conn.cursor()
    # 修改数据
    sql = "update wxb set psw = %s where baidu = %s"  # 注意%s什么时候加引号，什么时候不加
    data = (x, y)
    cursor.execute(sql, data)
    conn.commit()  # 提交，不然无法保存插入或者修改的数据
    cursor.close()  # 关闭游标
    conn.close()  # 关闭连接
    print('over')


def identify(baidu):
    '''获取百度网盘的url'''
    header = {
        'User-Agent': get_user_agent()
    }
    header['Host'] = 'node.pnote.net'
    preflex = 'https://node.pnote.net/public/pan?url='
    url = preflex + baidu
    time.sleep(random.random() * 3 + 0.1)
    res = requests.get(url, timeout=8, headers=header, ).text.encode('utf-8')
    #     print(res)
    if 'access_code' in str(res):
        return json.loads(res)['access_code']


def get_pictures(url):
    '''获取图片的url'''
    res = requests.get(url).text
    soup = BeautifulSoup(res, "lxml")
    return soup


# In[ ]:


def get_user_agent():
    '''
    随机获取一个用户代理
    '''
    user_agents = [
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
        "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
        "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
        "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
        "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
        "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
        "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
        "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
        "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 LBBROWSER",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; 360SE)",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
        "Mozilla/5.0 (iPad; U; CPU OS 4_2_1 like Mac OS X; zh-cn) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148 Safari/6533.18.5",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b13pre) Gecko/20110307 Firefox/4.0b13pre",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:16.0) Gecko/20100101 Firefox/16.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11",
        "Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10"]
    #  random.choice返回列表的随机项
    user_agent = random.choice(user_agents)
    return user_agent

def new_database(database_name):
    '''建立数据库'''
    import pymysql
    db = pymysql.connect(host='localhost', user=USERNAME, password=PASSWORD, port=3306)
    cursor = db.cursor()
    cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET utf8".format(DATABASE))
    db.close()

if __name__ == '__main__':



    DATABASE = "lxwc"  # 数据库名字，需要自己先建立，编码为utf8，排序general ci
    USERNAME = "root"  # 数据库用户名 默认为root
    PASSWORD = "123456"  # 数据库密码 填写自己的
    # cookie 登录账号后获取 格式和我下面的一样
    cookie = 'w2hd_8acf_saltkey=suV8H1nU; w2hd_8acf_lastvisit=1586342456; w2hd_8acf_nofavfid=1; w2hd_8acf_visitedfid=67D55D385D99D91D95D96D77D70D342; w2hd_8acf_ulastactivity=7de5Ze%2BZsYfY0ggGNxLBT5CaLJJGTdZeMECt%2Fj1p91xflgpbCtla; w2hd_8acf_auth=9081Y%2BVnV1MWTyvL9FEswBVL1WoUgPXn8Ek4pRLvLosQ1oQqS6c6oJrtslXXNjpBvXmzEHOLRiD4fnKOguqnJm3rW0mw; w2hd_8acf_wxauth=ed8397955a81d8c4d0094e23f81a274d; w2hd_8acf_mpscanckn=yes; w2hd_8acf_st_t=1557649%7C1586863642%7C967ce6d6d1f8688ab3c4d2336e244947; w2hd_8acf_forum_lastvisit=D_42_1586522429D_218_1586522788D_75_1586525124D_342_1586526373D_70_1586556854D_77_1586556880D_96_1586556925D_91_1586568411D_99_1586568673D_385_1586568782D_55_1586568790D_67_1586863642; w2hd_8acf_checkpm=1; w2hd_8acf_sendmail=1; w2hd_8acf_lip=111.193.88.53%2C1586863649; w2hd_8acf_lastact=1586863652%09forum.php%09viewthread; w2hd_8acf_st_p=1557649%7C1586863652%7Cf556de31a6f4b047729776b44d01b86d; w2hd_8acf_viewid=tid_67436; w2hd_8acf_sid=b8oyW6; Hm_lvt_696f676523c9176b65220027136d9a6f=1586568158,1586568195,1586862693,1586863643; Hm_lpvt_696f676523c9176b65220027136d9a6f=1586863653'
    top_url = 'http://www.lxwc.com.cn/forum.php'
    # top_url = 'http://www.lxwc.com.cn/forum0.html'
    # 以上 需要根据自己的本地的条件填写
    headers1 = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 QIHU 360SE'
    }
    headers1['Host'] = 'www.lxwc.com.cn'

    # 建立数据库，第一次运行就行了。第二次就注释掉建立数据库的
    # 否则会报错：pymysql.err.ProgrammingError: (1007, "Can't create database 'wxb1'; database exists")
    new_database(DATABASE)

    # 这个url是英文社区的url，如果趴中文社区就取消下面网址的注释
    num_and_page = get_num_and_page(top_url)

    for i in num_and_page:
        ls = []
        for j in range(1, int(i[1]) + 1):
            n_p = (j, i[0])
            ls.append(n_p)
        with ThreadPoolExecutor(30) as executor:
            executor.map(main_func, ls)
