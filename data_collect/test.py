from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
import json
import os
import multiprocessing
import random



def open_browser():
    options = ChromeOptions()
    user_agent = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"
    options.add_argument('user-agent=' + user_agent)
    options.add_argument("lang=ko_KR")
    #options.add_argument('headless')
    options.add_argument('window-size=1920x1080')
    options.add_argument("disable-gpu")
    options.add_argument("--no-sandbox")

    # 크롬 드라이버 최신 버전 설정
    service = ChromeService(executable_path=ChromeDriverManager().install())

    # chrome driver
    driver = webdriver.Chrome(service=service, options=options) # <- options로 변경
    return driver

def crawling(driver, n):
    html = driver.page_source
    prgmName = driver.current_url.split('/')[-2]
    soup = bs(html, 'html.parser')
    # print(soup)

    result_list = []
    for row in soup.select('.u_cbox_comment_box'):
        try:
            nickname = row.select_one('.u_cbox_nick').text
            timestamp = row.select_one('.u_cbox_date').text
            comment = row.select_one('.u_cbox_contents').text
        except Exception as e:
            # print('에러 발생:', row, ';', e)
            pass

        result_list.append({
                    'nickname': nickname,
                    'timestamp': timestamp,
                    'comment': comment
                    })

    # driver.page_source는 처음부터 아래 데이터를 다 긁어오기 때문에 스크롤 내려서 추가된 데이터만 저장
    result_list = result_list[-20*n:]
    # print(result_list[-20*n])
    # print(result_list[-1])
    # print(len(result_list))
    result_df = pd.DataFrame(result_list)
    # print(result_df.tail(3))

    filename = f'raw_data_{prgmName}.txt'
    if not os.path.exists(filename):
        result_df.to_csv(filename, index=None, encoding='utf-8')
    else:
        result_df.to_csv(filename, mode='a', header=None, index=None, encoding='utf-8')

    return driver

def main(url):
    # Chrome Browser 열기
    driver = open_browser()
    driver.get(url)
    time.sleep(2)
    # 필요에 따라 cnt 조정
    cnt = 1
    n = 10
    last_cnt = 5

    while True:
        # 대기시간 랜덤으로 주기
        t = random.randint(2,5)
        time.sleep(t)
        # print(cnt)

        # n번 count마다 크롤링 후 파일로 저장
        if cnt%n == 0:
            driver.execute_script('window.scrollTo(0,0);')
            driver = crawling(driver, n)
            time.sleep(t)

        # 마지막 페이지까지 남은 페이지 모두 크롤링 후 파일로 저장
        if cnt > last_cnt:
            # while 문이 break될때 맨위로 스크롤 올리기
            driver.execute_script('window.scrollTo(0,0);')
            driver = crawling(driver, n)
            time.sleep(2)
            driver.quit()
            break

        xpath = '''//*[@id="cbox_module"]/div/div[7]/a/span/span/span[2]'''
        driver.find_element(By.XPATH, xpath).click()
        cnt += 1


if __name__ == '__main__':
    urls = ['https://program.naver.com/p/8675789/talk',   # SKY캐슬
            'https://program.naver.com/p/3619390/talk']   # 힘쎈여자도봉순
    # multiprocessing으로 browser 2개 동시에 열기 (CPU개수 많이 지정하면 크롤링봇 막힐 수 있음)
    with multiprocessing.Pool(processes=2) as pool:
        pool.map(main, urls)
        pool.terminate()
        pool.join()