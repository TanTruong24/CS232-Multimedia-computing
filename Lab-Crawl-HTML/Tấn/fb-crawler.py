import random
from selenium import webdriver
import pandas as pd
import numpy as np
from time import sleep
import os
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
from csv import writer


def getValuecsv():
    df_links = pd.read_csv("./linkPost.csv")
    return df_links["LinkPost"].tolist(), df_links["DatePost"].tolist()


def appendRowCsv(lstData):
    with open('./Post-Comment.csv', 'a', encoding="utf-8", newline='') as f_object:
        # Pass the CSV  file object to the writer() function
        writer_object = writer(f_object)
        # Result - a writer object
        # Pass the data in the list as an argument into the writerow() function
        writer_object.writerow(lstData)
        # Close the file object
        f_object.close()


def login(browser, mail, password):
    try:
        txtUser = browser.find_element_by_id("m_login_email")
        txtUser.send_keys(mail)

        txtPass = browser.find_element_by_id("m_login_password")
        txtPass.send_keys(password)

        # 2b. Submit form
        txtPass.send_keys(Keys.ENTER)
        sleep(7)

        return True
    except:
        return False


def getAllPost():
    lstLinks, lstDate = getValuecsv()
    lstData = []
    #len_links = len(lstLinks)
    Acc0 = ["mail-1", "pass-1"]
    Acc1 = ["mail-2", "pass-2"]
    Acc2 = ["mail-3", "pass-3"]
    lstAcc = [Acc0, Acc1, Acc2]
    for i in range(0, len(lstLinks)):
        n = random.randint(0, 2)    # random chọn tài khoản ngẫu nhiên
        print("Acc-{}".format(n))

        lstData.append(lstDate[i])
        lstData.append(lstLinks[i])

        browser = webdriver.Chrome(executable_path="./chromedriver.exe")
        browser.get(lstLinks[i])
        if (login(browser, lstAcc[n][0], lstAcc[n][1]) == False):
            soup = BeautifulSoup(browser.page_source, 'html')
            #sleep(random.randint(3, 17))
            browser.close()

            # ---------------content---------------------
            s = soup.find('div', attrs={'class': "_5rgt _5nk5"})
            if (s == None):
                continue
            lstData.append(s.getText())

            # --------------commnets-------------------
            Comments = soup.findAll('div', attrs={'class': "_333v _45kb"})
            if not Comments:
                continue
            temp = Comments[0].findAll(
                'div', attrs={'data-sigil': "comment-body"})
            cmt = ""
            for cp in temp:
                cmt += cp.getText() + "\n"
            lstData.append(cmt)
        else:
            soup = BeautifulSoup(browser.page_source, 'html')
            sleep(random.randint(3, 17))
            browser.close()

            # ---------------content---------------------
            s = soup.find_all('div', attrs={'class': "_5rgt _5nk5"})
            if not s:
                continue
            if (s[0].find('p') == None):
                print("s[0] rỗng")
                continue
            lstData.append(s[0].find('p').getText())

            # --------------commnets-------------------
            Comments = soup.findAll('div', attrs={'class': "_333v _45kb"})
            if not Comments:
                continue

            temp = Comments[0].findAll(
                'div', attrs={'data-sigil': "comment-body"})
            cmt = ""
            for cp in temp:
                cmt += cp.getText() + "\n"
            lstData.append(cmt)

        # ------------record csv----------------
        appendRowCsv(lstData)
        lstData.clear()

        print(i)    #check
        sleep(random.randint(5, 10))

if __name__ == "__main__":
    getAllPost()
