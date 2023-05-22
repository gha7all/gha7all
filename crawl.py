# !pip install requests
# !pip install bs4
# !d:/git/postgres/venv/Scripts/python.exe -m pip install ipykernel -U --force-reinstall
# !pip install selenium==3.141
# !pip install msedge-selenium-tools
# !pip install pandas 
# !pip install emoji

import requests
from bs4 import BeautifulSoup
from time import sleep
import re
import pandas as pd
from emoji import replace_emoji 
import numpy as np

link_home=[]
break_=0
number_of_pages=10
for i in range(number_of_pages):
        # sleep(1)
        if break_>5:break
        html=requests.get("https://kilid.com/buy/tehran-region5?listingTypeId=1&page=%d"%i)
        if html.status_code == 200:
            soup=BeautifulSoup(html.content,'html.parser')
            print("page :" , i)
        
            
            for a in soup.find_all('a',class_="kilid-listing-card flex-col al-start ng-star-inserted", href=True):
                sleep(0.2)
                if a["href"] in link_home:
                    print("duplicate in page :",i+1)
                    break_ +=1
                    continue
                link_home.append(a["href"])
                # print("Found the URL:", a['href'])
                
                
   

with open("link.txt","w") as f:
    f.write(" ".join(link_home))
f.close()
   


merge_dic=lambda x,y:x|y

def replace_chars(text):
   for ch in ['/','`','*','_','{','}','[',']','(',')','>','#','+','-','!','$','\'',"//"]:
        if ch in text:
            text = text.replace(ch," ")
   return text

def try_css_selector(soup,css):
    
    try:
        return replace_chars(soup.select_one(css).text.strip())
        
    except AttributeError:
        return None
def extract_location(soup,css):
   location=dict()
   
   try:
      complit_loc=[]
      complit_loc = try_css_selector(soup,".single-data__location span").split("،")
      
      
      region=[]
      for i in complit_loc[1].strip():
         if i.isdigit():
            region.append(i)
      location["city"]=complit_loc[0].strip()
      location["region"]="".join(region)
      location["address"]=complit_loc[2].strip()
      
      return location

   except :
      
       location["city"]=None
       location["region"]=None
       location["address"]=None
      
       return location 

def extract_features(soup,css):
   features=dict()
   features2=dict()
   name=[]
   nums=[]
   try:
      for i in try_css_selector(soup,".single-data__container--attributes").split():
         if i.isalpha():
            name.append(i)
         else:nums.append(i)

      features={k:v for k,v in zip(name,nums)}

      features2["parking"] = features.get("پارکینگ",None)    
      features2["Meterage"] = features.get("متر",None)    
      features2["bedroom "] = features.get("خوابه",None)    
      features2["age_year"] = features.get("ساله",None)    
         
   except:
      features2["parking"] = None 
      features2["meterage"] = None
      features2["bedroom "] = None    
      features2["age_year"] = None    
   
   return features2


def extract_persian_text(soup, css):
   try:
      string_=str(soup.select_one(css))
      
      facilities=[]
      pattern =r'.*>([\u0600-\u06FF]+\s*[\u0600-\u06FF]*)<.*'
      while True:
          try:
              y=re.search(pattern,string_)
              if y.groups():
                  facilities.append(y.groups()[0])
                  string_=string_.replace(facilities[-1],"")          
              else:break
              
          except  : break
          

      return " , ".join(facilities)
   
   except: return None
  
def extract_number(soup,css):
   try:
      for i in try_css_selector(soup=soup,css=css).split():
         if i.isdigit():
            return int(i)
   except:
      return None
   
   
def details(href):
   html = requests.get(href)
   
   if html.status_code == 200:
      soup = BeautifulSoup(html.content , "html.parser")
      
      home=dict() 
      home["title"]= try_css_selector(soup,".single-data__info")  
      try:
         home["total_price"]=replace_chars(try_css_selector(soup,".single-data__container.ng-star-inserted").split()[2])
      except:
         home["total_price"]=None
      
      home["price_per_meter"]=extract_number(soup=soup,css=".ng-star-inserted+ .single-data__container")
      
      home=merge_dic(home,extract_location(soup,".single-data__location span"))
      home=merge_dic(home,extract_features(soup,".single-data__location span"))
      home["facilities"] = extract_persian_text(soup,".ng-trigger-slideDown")
      home["adviser"] = try_css_selector(soup=soup , css= ".single-sticky__department__user-name")
      home["real_estate"] =try_css_selector(soup=soup,css=".single-sticky__department__name")
      home["ad_code"] = extract_number(soup=soup,css=".single-sticky__info__item:nth-child(1)")
      
      home["description"] = extract_persian_text(soup,".single-description")
      
      try:
         home["description"] = replace_emoji(soup.select_one(".single-description").text.strip())
      except:
         home["description"] = None
      
      return home
   
with open("link.txt","r") as f:
  
      link_home=f.read().split()
      
      
      
from msedge.selenium_tools import Edge,EdgeOptions  
from selenium.webdriver.common.keys import Keys  
from time import sleep
from selenium.webdriver.common.action_chains import ActionChains

from selenium.webdriver.edge.service import Service
from selenium import webdriver


df=pd.DataFrame()
# with open("your_file_name") as in_file:
   
# with open("link.txt","r") as f:
  
#       link_home=f.read().split()
   
for url in range(len(link_home)): #range(3):
   
   edge_options = EdgeOptions()  
   edge_options.use_chromium = True  
   # edge_options.add_argument("start-maximized")  
   edge_options.add_argument('headless')  
   edge_options.add_argument("inprivate")   
   edge_options.add_argument("--ignore-coretificate-errors")
   edge_options.add_argument("--ignore-ssl-errors")
   
   driver_path = r"D:\git\postgres\edgedriver_win64_2\msedgedriver.exe"
   
   href = r"https://kilid.com"+link_home[url]
   # url = "https://kilid.com/buy/detail/3124988"
   # sleep(5)
   
   
   driver = Edge(executable_path = driver_path, options=edge_options)  
   driver.get(href)
   
   output=pd.DataFrame(details(href),index=[0])
   try:
      sleep(.3)
      element=driver.find_element_by_css_selector(".single-sticky__button--call")
      element.click()
   
      sleep(0.5)
   
      output["phone"]=try_css_selector(BeautifulSoup(driver.page_source),".single-sticky__button--call p")
   except:
      output["phone"]=None
   print(url)
   df=pd.concat([df,output],axis=0,ignore_index=True)
   # sleep(1)
   driver.close()
   driver.quit()

   
# fill na && data manipulate

df.price_per_meter=df[df.total_price.notna()].price_per_meter.fillna((df[df.total_price.notna()].total_price.astype(float) * 1000 / df[df.total_price.notna()].Meterage.astype(float)).astype(int))

df.loc[df.total_price.notna(),"price_per_meter"]= df[df.total_price.notna()].price_per_meter.astype(int)
df.loc[df.total_price.notna(),"total_price"]= df[df.total_price.notna()].total_price.astype(float)
df.region= df.region.astype(int)
df.parking=df.parking.fillna(0)
df.parking= df.parking.astype(int)
df.Meterage= df.Meterage.astype(int)
df["bedroom "]=df["bedroom "].fillna(0)
df["bedroom "]= df["bedroom "].astype(int)

df.phone= df.phone.astype("Int64")

df.address.replace("بلوار","",regex=True,inplace=True)



import numpy as np
x=[]
for i in df.facilities[df.facilities.notna()].str.split(" , "):
  for j in i:

    x.append(j)
x=list(set(x))
len(x)
y ="""'sauna',
  'proportionate shares',
  'roof garden',
  'balcony',
  'sports hall',
  'exchange',
  'guardian',
  'agreed price',
  'Elevator',
  'Jacuzzi',
  'have loan',
  'conference hall',
  'newly built',
  'mall',
  'lobby',
  'remote door',
  'air conditioning',
  'pool',
  'Central antenna',
  'Warehouse'
  """
dict_unique_features=dict(zip(x,[i.strip().replace(" ","_") for i in y.replace("\n","").replace("'","").split(", ")]))
dict_unique_features

temp=dict()
for i in range(df.shape[0]):
  list_features=[]
  for j in dict_unique_features.keys():
    if df.facilities[i] is np.nan: 
        list_features.append(list(np.full(len(dict_unique_features),np.nan)))
        continue
    
    if j in df.facilities[i].split(" , "):
      list_features.append(1)
    else :
      list_features.append(0)
  temp[i]=list_features
facilities=pd.DataFrame(temp.values(),columns=dict_unique_features.values(),index=temp.keys())

list_features.clear()
temp.clear()

df2=pd.concat([df,facilities],axis=1).drop("facilities",axis=1)

df2.to_excel("homes.xlsx",index=None)





