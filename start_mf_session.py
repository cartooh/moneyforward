#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import base64
import os
import re
import hashlib
import json
import pickle
from urllib.parse import urlparse, parse_qs, urlunsplit
from pprint import pprint
import keyring
import requests
import chromedriver_binary
import time
from selenium import webdriver
from selenium.webdriver.common.by import By

parser = argparse.ArgumentParser()
parser.add_argument('-s', '--service', default='moneyforward')
parser.add_argument('username')
parser.add_argument('-p', '--password')
parser.add_argument('-c', '--mf_cookies', default='mf_cookies.pkl')
args = parser.parse_args()

username = args.username
password = args.password
if not password:
    password = keyring.get_password(args.service, username)


options = webdriver.ChromeOptions()
# options.add_argument('--headless')
driver = webdriver.Chrome(options=options)

driver.get('https://moneyforward.com/sign_in')

text_box = driver.find_element(by=By.NAME, value="mfid_user[email]")
text_box.send_keys(username)
text_box.submit()

text_box = driver.find_element(by=By.NAME, value="mfid_user[password]")
text_box.send_keys(password)
text_box.submit()

while True:
    current_url = urlparse(driver.current_url)
    if current_url.path != '/email_otp':
        print(f"Unkown URL: {current_url}")
        input("Press Enter to continue...")
        break

    driver.minimize_window()
    email_otp = input("Enter OTP: ")
    text_box = driver.find_element(by=By.NAME, value="email_otp")
    text_box.send_keys(email_otp)
    text_box.submit()
    time.sleep(5)

    if driver.current_url == 'https://moneyforward.com/':
        print("Login successful")
        break

with requests.session() as s:
    for cookie in driver.get_cookies():
        s.cookies.set(cookie["name"], cookie["value"])

    json_data = s.get('https://moneyforward.com/sp/category').json()
    if int(json_data['result']) != 0:
        print("Failed to get category:", json_data)
        exit(1)

    with open(args.mf_cookies, 'wb') as f:
        pickle.dump(s.cookies, f)
    print("Save session:", args.mf_cookies)

driver.quit()

