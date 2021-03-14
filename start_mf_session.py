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
from selenium import webdriver


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
options.add_argument('--headless')
driver = webdriver.Chrome(options=options)

driver.get('https://id.moneyforward.com/sign_in/email')

text_box = driver.find_element_by_name("mfid_user[email]")
text_box.send_keys(username)
text_box.submit()

text_box = driver.find_element_by_name("mfid_user[password]")
text_box.send_keys(password)
text_box.submit()


code_verifier = base64.urlsafe_b64encode(os.urandom(128)).decode('utf-8')
code_verifier = re.sub('[^a-zA-Z0-9]+', '', code_verifier)

code_challenge = hashlib.sha256(code_verifier.encode('utf-8')).digest()
code_challenge = base64.urlsafe_b64encode(code_challenge).decode('utf-8')
code_challenge = code_challenge.replace('=', '')

authorize_url = "https://id.moneyforward.com/oauth/authorize"
authorize_params = dict(
    client_id="2WND7CAYV1NsJDBzk13JRtjuk5g9Jtz-4gkAoVzuS_k", 
    code_challenge=code_challenge,
    code_challenge_method="S256",
    redirect_uri="moneyfwd://moneyforward.com/mfid/login",
    response_type="code",
    scope="openid",
    state="bWgZLVSOWbjmYtbp5oY3e0vYTScqotj2P1zvDcBwWBw",
)

sessions_url = "https://moneyforward.com//sp2/oauth/mfid/sessions"

with requests.session() as s:
    for cookie in driver.get_cookies():
        s.cookies.set(cookie["name"], cookie["value"])
    
    authorize_response = s.get(authorize_url, params=authorize_params, allow_redirects=False)
    loc = urlparse(authorize_response.headers["Location"])
    qs = parse_qs(loc.query)
    
    sessions_params = dict(
        code_verifier=code_verifier,
        redirect_uri=urlunsplit((loc.scheme, loc.netloc, loc.path, '', '')),
        code=qs["code"][0],
    )
    sessions_response = s.post(sessions_url, json.dumps(sessions_params), headers={'Content-Type': 'application/json'})
    if sessions_response.status_code != requests.codes.ok:
        pprint(sessions_response.json())
    
    with open(args.mf_cookies, 'wb') as f:
        pickle.dump(s.cookies, f)
    print("Save session:", args.mf_cookies)
    
driver.quit()

