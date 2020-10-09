from  geopy.geocoders import Nominatim
from selenium.webdriver.support import ui
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import ast
import re
from bs4 import BeautifulSoup as bssoup
from urllib.request import Request, urlopen
from fuzzywuzzy import fuzz, process
from random_proxies import random_proxy
import json
import requests
from random import choice
from random import randint
from time import sleep
from pathlib import Path
import os
import csv
import time
import pandas as pd
from selenium.webdriver import Chrome
import psycopg2
from datetime import datetime as dt
import sys

CURRENT_WORKING_DIR = Path.cwd()
ofile = open('scrape_output_drugprice.csv', 'w', 1,newline='')
scrape_list = []

def get_ped_drug_master_data():
    connection = ''
    rows = None
    network = None
    PharmData = None

    try:
        # read database configuration
        conf = open('config.json', 'r')
        conf_json = json.load(conf)
        # connect to the PostgreSQL database
        connection = psycopg2.connect(user=conf_json['DEV']['user'],
                                      password=conf_json['DEV']['password'],
                                      host=conf_json['DEV']['host'],
                                      port=conf_json['DEV']['port'],
                                      database=conf_json['DEV']['database'])
        cursor = connection.cursor()
        postgreSQL_select_Query = "SELECT DISTINCT ndc, quantity, dosage_strength, drug_type, name, zip_code, id, goodrx_id FROM ped.drug_master" \
                                  " ORDER BY DRUG_TYPE ASC"

        # print(postgreSQL_select_Query)
        cursor.execute(postgreSQL_select_Query)
        rows = cursor.fetchall()
        print('rows : ', rows)
        print('No of Rows:' + str(len(rows)))
        cursor = connection.cursor()
        select1_Query = "select program_id, name from ped.program_info"
        cursor.execute(select1_Query)
        network = cursor.fetchall()
        nw_df = pd.DataFrame(network, columns=['program_id', 'network'])
        cursor = connection.cursor()
        Select2_Query = "Select pharmacy_id, upper(name) from ped.pharmacy_info"
        cursor.execute(Select2_Query)
        PharmData = cursor.fetchall()
        Pharm_df = pd.DataFrame(PharmData, columns=['pharmacy_id','Pharmacy'])

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            # print("GRX PostgreSQL connection is closed")
    format_data_for_url(rows, Pharm_df)
    #return rows, nw_df, Pharm_df

def format_data_for_url(data, pharm_df):
    row_list = []
    for row in data:
        Drug_Name = str(row[4]).split(',')[0]
        Drug_Type = str(row[3]).strip()
        Drug_Strength = str(row[2]).strip()
        NDC = str(row[0]).strip().zfill(11)
        Quantity = str(int(row[1]))
        Days_Supply = 2
        Zip_Code = str(row[5]).strip().zfill(5)
        Drug_ID = row[6]
        Distance = 20
        goodrx_id = row[7]
        temp = [Drug_Name, Drug_Type, Drug_Strength, NDC, Quantity, Days_Supply, Zip_Code, Distance, Drug_ID, goodrx_id]
        row_list.append(temp)
    #return row_list
    for i in range(len(row_list)):
        create_url(row_list[i][0], row_list[i][1], row_list[i][2], row_list[i][3], row_list[i][4],
                   row_list[i][5], row_list[i][6], row_list[i][7], row_list[i][8], row_list[i][9])
    scrape_df = pd.DataFrame(scrape_list, columns=["createdat", "drug_id", "program_id", "price", "network", "distance", "Pharmacy"])
    pharm_id_list = create_id_match_list(scrape_df, pharm_df)
    scrape_df.insert(3, "pharmacy_id", pharm_id_list, True)
    scrape_df.pop("Pharmacy")
    print(scrape_df.head(5))
    insert_competitor_pricing(scrape_df)

def create_url(Drug_Name, Drug_Type, Drug_Strength, NDC, Quantity, Days_Supply, Zip_Code, Distance, Drug_ID, good_rx_id):
    geolocator = Nominatim(user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36')
    loc = geolocator.geocode(Zip_Code)
    latitude = loc.latitude
    longitude = loc.longitude

    url_1 = 'https://rxsaver.retailmenot.com/api/v2/priceListItems?pda={}++++{}+{}+++++&ndc=' \
            '{}&quantity={}&daysSupply={}&zipCode={}&distance={}'.format(Drug_Name, Drug_Type, Drug_Strength, NDC, Quantity, Days_Supply, Zip_Code, Distance)
    url_1b = 'https://rxsaver.retailmenot.com'
    #get_drug_price_rxsaver(url_1, url_1b, Drug_ID, Distance)

    url_2 = 'https://webapi.singlecare.com/api/pbm/tiered-pricing/{}?qty={}&zipCode={}'.format(NDC, Quantity, Zip_Code)
    url_2b = 'https://webapi.singlecare.com'
    #get_drug_price_single_care(url_2, url_2b, Drug_ID, Distance)

    url_3 = 'https://rxsavings.Medimpact.com/web/rxcard/home?p_p_id=com_cashcard_portal_portlet_CashCardPortlet_INSTANCE_wVwgc3hAI7xv&p_p_' \
          'lifecycle=2&p_p_state=normal&p_p_mode=view&p_p_cacheability=cacheLevelPage&_com_cashcard_portal_portlet_CashCardPortlet_' \
          'INSTANCE_wVwgc3hAI7xv_cmd=get_drug_detail&_com_cashcard_portal_portlet_CashCardPortlet_INSTANCE_wVwgc3hAI7xv_quantity={}&' \
          '_com_cashcard_portal_portlet_CashCardPortlet_INSTANCE_wVwgc3hAI7xv_drugName={}&_com_cashcard_portal_portlet_CashCardPortlet' \
          '_INSTANCE_wVwgc3hAI7xv_brandGenericFlag=G&_com_cashcard_portal_portlet_CashCardPortlet_INSTANCE_wVwgc3hAI7xv_lat={}&_com_cashcard' \
          '_portal_portlet_CashCardPortlet_INSTANCE_wVwgc3hAI7xv_lng={}&_com_cashcard_portal_portlet' \
          '_CashCardPortlet_INSTANCE_wVwgc3hAI7xv_numdrugs={}'.format(Quantity, Drug_Name, latitude, longitude,Quantity)
    url_3b = "https://rxsavings.Medimpact.com"
    #get_drug_price_rxsavings_medimpact(url_3, url_3b, Drug_ID, Distance)

    if good_rx_id:
        url_4 = 'https://www.goodrx.com/api/v4/drugs/{}/prices?location={},{}&location_type=LAT_LNG_GEO_IP&dosage{}' \
                '&quantity={}'.format(good_rx_id, longitude, latitude, Drug_Strength, Quantity)
        url_4b = 'https://www.goodrx.com'
        get_drug_price_goodrx(url_4, url_4b, Drug_ID, Distance)

        url_5 = 'https://www.blinkhealth.com/api/v2/user/drugs/detail/{}/dosage/{}/quantity/{}'.format(Drug_Name, good_rx_id, Quantity)
        url_5b = 'https://www.blinkhealth.com'
        get_drug_price_blink_health(url_5, url_5b, Drug_ID, Distance)
    else:
        print("Couldn't access the networks GoodRX and Blinkhealth!!")

    url_6 = "https://services.insiderx.com/pricing/v1/npi/people"

    url_7 = "https://www.wellrx.com/prescriptions/get-specific-drug"


def get_drug_price_rxsavings_medimpact(url, urlb, Drug_ID, Distance):
    try:
        driver = Chrome('C:\\Users\\pgiliyar\\.wdm\\drivers\\chromedriver\\win32\\84.0.4147.30\\chromedriver.exe')
        executor_url = driver.command_executor._url
        session_id = driver.session_id
        print("Session ID: {}; Executor URL: {}".format(session_id, executor_url))
        driver.get(urlb)
        cookie = driver.get_cookies()
        for n in cookie:
            driver.add_cookie({"name": n['name'], "value": n['value']})
        executor_url = driver.command_executor._url
        session_id = driver.session_id
        sleep(randint(10, 20))
        stamp = dt.now()
        timestamp = "'{}'".format(stamp)
        driver.get(url)
        time.sleep(randint(10, 15))
        out1 = driver.page_source
        driver.delete_all_cookies()
        sub = str(out1[25:-14])
        data = json.loads(sub)
        for n in data["drugs"]["locatedDrug"]:
            pharm_name = n["pharmacy"]["name"]
            price = n["pricing"]["price"]
            network = ""
            program_id = 3
            olist = [timestamp, Drug_ID, program_id, price, network, Distance, pharm_name]
            scrape_list.append(olist)
        driver.close()
        return scrape_list
    except:
        print("Error while scraping RxSavings Medimpact!!")
        return scrape_list

def get_drug_price_blink_health(url, urlb, Drug_ID, Distance):
    try:
        driver = Chrome('C:\\Users\\pgiliyar\\.wdm\\drivers\\chromedriver\\win32\\84.0.4147.30\\chromedriver.exe')
        executor_url = driver.command_executor._url
        session_id = driver.session_id
        print("Session ID: {}; Executor URL: {}".format(session_id, executor_url))
        driver.get(urlb)
        cookie = driver.get_cookies()
        for n in cookie:
            driver.add_cookie({"name": n['name'], "value": n['value']})
        executor_url = driver.command_executor._url
        session_id = driver.session_id
        sleep(randint(10, 20))
        stamp = dt.now()
        timestamp = "'{}'".format(stamp)
        driver.get(url)
        time.sleep(randint(10, 15))
        out1 = driver.page_source
        driver.delete_all_cookies()
        sub = str(out1[84:-20])
        data = json.loads(sub)
        pharm_name = ""
        price = data["result"]["price"]["delivery"]["raw_value"]
        network = ""
        program_id = 5
        olist = [timestamp, Drug_ID, program_id, price, network, Distance, pharm_name]
        scrape_list.append(olist)
        driver.close()
        return scrape_list
    except:
        print("Error scraping Blink Health!!")
        return scrape_list

def get_drug_price_rxsaver(url, urlb, Drug_ID, Distance):
    try:
        driver = Chrome('C:\\Users\\pgiliyar\\.wdm\\drivers\\chromedriver\\win32\\84.0.4147.30\\chromedriver.exe')
        executor_url = driver.command_executor._url
        session_id = driver.session_id
        print("Session ID: {}; Executor URL: {}".format(session_id, executor_url))
        driver.get(urlb)
        cookie = driver.get_cookies()
        for n in cookie:
            driver.add_cookie({"name": n['name'], "value": n['value']})
        executor_url = driver.command_executor._url
        session_id = driver.session_id
        sleep(randint(10, 20))
        stamp = dt.now()
        timestamp = "'{}'".format(stamp)
        driver.get(url)
        out1 = driver.page_source
        driver.delete_all_cookies()
        sub = str(out1[84:-20])
        data = json.loads(sub)
        for n in data["priceListItems"]:
            pharm_name = n["name"]
            price = n["price"]["discounted"]
            network = ""
            program_id = 7
            olist = [timestamp, Drug_ID, program_id, price, network, Distance, pharm_name]
            scrape_list.append(olist)
        driver.close()
        return scrape_list
    except:
        print("Error while scraping RxSaver!!!")
        return scrape_list

def get_drug_price_goodrx(url, urlb, Drug_ID, Distance):
    try:
        driver = Chrome('C:\\Users\\pgiliyar\\.wdm\\drivers\\chromedriver\\win32\\84.0.4147.30\\chromedriver.exe')
        executor_url = driver.command_executor._url
        session_id = driver.session_id
        print("Session ID: {}; Executor URL: {}".format(session_id, executor_url))
        driver.get(urlb)
        cookie = driver.get_cookies()
        for n in cookie:
            driver.add_cookie({"name": n['name'], "value": n['value']})
        executor_url = driver.command_executor._url
        session_id = driver.session_id
        sleep(randint(10, 20))
        stamp = dt.now()
        timestamp = "'{}'".format(stamp)
        driver.get(url)
        time.sleep(randint(10, 15))
        out1 = driver.page_source
        driver.delete_all_cookies()
        sub = str(out1[84:-20])
        data = json.loads(sub)
        for n in data["results"]:
            pharm_name = n["pharmacy"]["name"]
            program_id = 6
            temp = n["prices"]
            for m in temp:
                temp2 = list(m.items())
                for i in range(len(temp2)):
                    if 'COUPON' in temp2[i]:
                        for j in range(0, 9):
                            if 'price' in temp2[i + j]:
                                price = temp2[i + j][1]
                        for k in range(0, 9):
                            if '_network' in temp2[i + k]:
                                network = temp2[i + k][1]
                        olist = [timestamp, Drug_ID, program_id, price, network, Distance, pharm_name]
                        scrape_list.append(olist)
        driver.close()
        return scrape_list
    except:
        print("Error while scraping GoodRx!!")
        return scrape_list

def get_drug_price_single_care(url, urlb, Drug_ID, Distance):
    try:
        driver = Chrome('C:\\Users\\pgiliyar\\.wdm\\drivers\\chromedriver\\win32\\84.0.4147.30\\chromedriver.exe')
        executor_url = driver.command_executor._url
        session_id = driver.session_id
        print("Session ID: {}; Executor URL: {}".format(session_id, executor_url))
        driver.get(urlb)
        cookie = driver.get_cookies()
        for n in cookie:
            driver.add_cookie({"name": n['name'], "value": n['value']})
        executor_url = driver.command_executor._url
        session_id = driver.session_id
        sleep(randint(10, 20))
        stamp = dt.now()
        timestamp = "'{}'".format(stamp)
        driver.get(url)
        out1 = driver.page_source
        driver.delete_all_cookies()
        sub = str(out1[25:-14])
        data = json.loads(sub)
        for n in (data["Result"]["PharmacyPricings"]):
            pharm_name = n["Pharmacy"]["Name"]
            price = n["Prices"][0]["Price"]
            network = ""
            program_id = 4
            olist = [timestamp, Drug_ID, program_id, price, network, Distance, pharm_name]
            scrape_list.append(olist)
        driver.close()
        return scrape_list
    except:
        print("Error while scraping SingleCare!!!")
        return scrape_list

def create_id_match_list(df1, df2):
    plist = []
    ilist = []
    count = len(df1.index)
    print("String matching started for {} items".format(count))
    for i in df1.index:
        a = df1.loc[i, "Pharmacy"]
        print("Matching {}th item".format(i))
        for j in df2.index:
            b = df2.loc[j, "Pharmacy"]
            plist.append(b)
        c = list(process.extractOne(a, plist))
        if c[1] >= 90:
            for k in range(len(df2.index)):
                if df2.loc[k, "Pharmacy"] == c[0]:
                    id = df2.loc[k, "pharmacy_id"]
                    ilist.append(id)
        else:
            for l in range(len(df2.index)):
                if df2.loc[l, "Pharmacy"] == "OTHERS":
                    id = df2.loc[l, "pharmacy_id"]
                    ilist.append(id)
        print("Matching complete for {} items".format(i))
    return ilist


def insert_competitor_pricing(data):
    connection = None
    try:
        # read database configuration
        conf = open('config.json', 'r')
        conf_json = json.load(conf)
        # connect to the PostgreSQL database
        connection = psycopg2.connect(user=conf_json['DEV']['user'],
                                      password=conf_json['DEV']['password'],
                                      host=conf_json['DEV']['host'],
                                      port=conf_json['DEV']['port'],
                                      database=conf_json['DEV']['database'])

        for i in range(data.shape[0]):
            cursor = connection.cursor()
            postgreSQL_Insert_Query = "INSERT INTO PED.COMPETITOR_PRICING (createdat, drug_id, program_id, pharmacy_id, price, network, distance)" \
                                      " VALUES (%s, %s, %s, %s, %s, '%s', %s);" \
                                      % (data.loc[i, 'createdat'], data.loc[i, 'drug_id'], data.loc[i, 'program_id'], data.loc[i, 'pharmacy_id'], data.loc[i, 'price'], data.loc[i, 'network'], data.loc[i, 'distance']);

            # " ON CONFLICT (drug_id, program_id, pharmacy_id)" \
            # " DO NOTHING "

            cursor.execute(postgreSQL_Insert_Query)
            connection.commit()
            print("Added query {}".format(i))
            print(postgreSQL_Insert_Query)
        print("Query added successfully")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error : %s" % error)
        cursor.close()
    cursor.close()

get_ped_drug_master_data()