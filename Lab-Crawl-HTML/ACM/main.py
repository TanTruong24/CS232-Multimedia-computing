from selenium import  webdriver
from time import  sleep
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import pandas as pd
s = Service("C:\\Users\\HH\\Desktop\\chromedriver.exe")
driver = webdriver.Chrome(service = s)
driver.maximize_window()
driver.get("https://dl.acm.org/")
sleep(2)
search = driver.find_element(By.XPATH,"/html/body/div[1]/div/main/div[1]/div/div[1]/div/div[1]/div/form/div/div/input")
search.send_keys("prem kumar kalra")
click_search = driver.find_element(By.XPATH,"/html/body/div[1]/div/main/div[1]/div/div[1]/div/div[1]/div/form/div/div/button")
click_search.click()
sleep(5)
parent_title = driver.find_elements(By.CLASS_NAME,"hlFld-Title")
title_list = []
for element in parent_title:
    title = element.find_element(By.TAG_NAME,"a")
    title_list.append(title.text)
parent_abstract = driver.find_elements(By.CLASS_NAME,"issue-item__abstract")
abstract_list = []
for element in parent_abstract:
    abstract_list.append(element.text)
data = {
    'title': title_list,
    'abstract': abstract_list
}
df = pd.DataFrame(data = data)
df.to_csv('data.csv')
sleep(10)
driver.close()