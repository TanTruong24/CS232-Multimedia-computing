import random
from selenium import webdriver
import pandas as pd
from time import sleep
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup

# khai bao bien browser
browser = webdriver.Chrome(executable_path="./chromedriver.exe")

# 2. mở thử một trang web
browser.get("https://m.facebook.com/ConfessionUIT")


def writeCSV(lstLinkPost, lstText):
    df = pd.DataFrame({'LinkPost': lstLinkPost, 'DatePost': lstText})
    df.to_csv("./linkPost.csv", header=True, index=False)


def login():
    try:
        txtUser = browser.find_element_by_id("m_login_email")
        # <---  Điền username thật của các bạn vào đây
        txtUser.send_keys("mail-1")

        txtPass = browser.find_element_by_id("m_login_password")
        txtPass.send_keys("pass-1")

        # 2b. Submit form
        txtPass.send_keys(Keys.ENTER)

        sleep(7)

        return print('login success')
    except:
        return print("not login")


def scrollPage():
    # scroll page
    scroll_pause_time = random.randint(3, 16)
    i = 0
    while i < 2:
        if (i == 1):
            browser.execute_script("window.scrollTo(0, 0);")
            sleep(random.randint(1, 3))
            browser.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);")
            # Wait to load page
            sleep(scroll_pause_time)
        else:
            browser.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);")
            sleep(scroll_pause_time)
        i += 1
        print(i)


def getLink():
    soup = BeautifulSoup(browser.page_source, 'html')
    browser.close()

    lstLinks = []   # link bài post
    lstDate = []    # ngày post bài
    links = soup.findAll(
        'div', attrs={'class': "_52jc _5qc4 _78cz _24u0 _36xo"})
    for cp in links:
        lstLinks.append("https://m.facebook.com" + cp.a['href'])
        lstDate.append(cp.find('a').getText())

    print(len(lstLinks))
    writeCSV(lstLinks, lstDate)

if __name__ == "__main__":
    login()
    scrollPage()
    getLink()