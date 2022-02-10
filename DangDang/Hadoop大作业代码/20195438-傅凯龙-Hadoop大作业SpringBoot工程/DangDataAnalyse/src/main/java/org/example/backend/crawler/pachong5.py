# -*- coding: utf-8 -*-
"""
Created on Mon Aug 23 12:14:43 2021

@author: fkl18
"""

import requests
import re
import json
import xlwt
X=0
work_book=xlwt.Workbook(encoding='utf-8')
sheet=work_book.add_sheet('书籍表')
TITLE_LABEL = ['range', 'image',  'title', 'recommand', 'author','times','price']
#请求当当网 获取源代码
def request_dangdang(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.text
    except requests.RequestException:
        return None

#解析源代碼
def parse_response(html):
    pattern = re.compile('<li>.*?list_num.*?(\d+).</div>.*?<img src="(.*?)".*?class="name".*?title="(.*?)">.*?class="star">.*?target="_blank">(.*?)条评论.*?class="tuijian">(.*?)%推荐</span>.*?class="publisher_info">.*?target="_blank">(.*?)</a>.*?class="publisher_info">.*?target="_blank">(.*?)</a>.*?<span\sclass="price_n">&yen;(.*?)</span>',re.S)
#    .*?class="tuijian">(.*?)%推荐</span>.*?class="publisher_info">.*?target="_blank">(.*?)</a>.*?  class="publisher_info">.*?target="_blank">(.*?)</a>.*?<p><span\sclass="price_n">&yen;(.*?)</span>.*?</li>
    items = re.findall(pattern,html)
    for item in items:
        #打印一下 查看結果
        global X
        sheet.write(X, 0,item[0])
        sheet.write(X, 1,item[1])
        sheet.write(X, 2,item[2])
        sheet.write(X, 3,item[3])
        sheet.write(X, 4,item[4])
        sheet.write(X, 5,item[5])
        sheet.write(X, 6,item[6])
        sheet.write(X, 7,item[7])
        X = X+1
        yield {
            'ranking': item[0],
            'image': item[1],
            'title': item[2],
            'comment': item[3],
            'recommand': item[4],
            'author': item[5],
            'publisher': item[6],
            'price': item[7]
        }


#写入文件
def write_item_to_file(item):
#    with open('books.txt', 'a', encoding='UTF-8') as f:
#        f.write(json.dumps(item, ensure_ascii=False) + '\n')
#        f.close()
    T=1


#主函数
def main(page):
    url = 'http://bang.dangdang.com/books/bestsellers/01.00.00.00.00.00-month-2021-5-1-'+str(page)
    html = request_dangdang(url)
    items = parse_response(html)

    for item in items:
        write_item_to_file(item)
    work_book.save('book5.xls')

#实现翻页
if __name__=="__main__":
    for i in range(1,26):
        main(i)
