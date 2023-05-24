
import psycopg2 as pg2
import pandas as pd
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys
# کانکشن به پایگاه داده
# conn = psycopg2.connect(
#     host="localhost",
#     database="database_name",
#     user="username",
#     password="password"
# )

# کلاس CRUD
class Database:
     
            
    def __init__(self, config,database):
           self.config = config
           self.dbname = database
           self.con = None
           self.cur = None
           
           
    def connect(self):
               
        
            try:
                self.con = pg2.connect(**config)
                
            except :
                print ("Unable to connect!")
                sys.exit(1)
            else:
                
                print ("Connected!")
                self.con.autocommit = True
                self.con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                self.cur = self.con.cursor()
                self.cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '%s' ;" %self.dbname)
                exists = self.cur.fetchone()
                if not exists:
                    self.cur.execute(f'CREATE DATABASE {self.dbname} ;')
                    
                
                # self.cur.execute(f'DROP DATABASE IF EXISTS {self.dbname}')
                # print(f"!! Database{self.dbname} has dropped :) ")
                # self.cur.execute('CREATE DATABASE ' + self.dbname)   
                    self.config["database"] = self.dbname
                    self.con = pg2.connect(**config)
                    self.cur = self.con.cursor()
                self.cur.execute("SELECT current_database()")
                print('current database')
                for r in self.cur.fetchall():
                    print(r)
                print('\n')   
                 
    def disconnect(self):
        self.con.commit() 
           
        self.cur.close()
        self.con.close()
                    

    def create_table(self, table_name,data_types ,columns):
            
            try:
                columns = ', '.join([f"{k} {v}" for k,v in data_types.items()])
            
                create_table_query = f"CREATE TABLE {table_name} ({columns});"
        
                self.cur.execute(create_table_query)
                self.con.commit()
            except:
                print("There may be a table with this name, you can use << drop_table(table_name)>> function to delete it and create it again !")
            
    def drop_table(self,table_name):
           
            
            drop_table_query = f"DROP TABLE {table_name} ;"
            self.cur.execute(drop_table_query)
            print("Table has be deleted :) ")
            
    def execute_query(self, query):
           
            self.cur.execute(query)    
                    
    def get_data(self, query):
        
        self.cur.execute(query)        
        data = self.cur.fetchall()
        cols = [col[0] for col in self.cur.description]
        df = pd.DataFrame(data, columns=cols)
  
        return df
   
   
class CRUD:
       
       def __init__(self,db): #  , table_name=None):
           
           self.db = db
           
       def insert_into_table(self ,table_name, df):    
            
            for i in range(df.shape[0]):
                row=df.iloc[i,:].to_dict()
                
                insert_query = f"INSERT INTO {table_name} ({','.join(row.keys())}) values ({','.join(['%s']*len(row.values()))})"
                db.cur.execute(insert_query, tuple(row.values()))
                print(f"{i} Insert in table <<{table_name} >> successfully :) ")
            
            self.db.con.commit()
            
       def get_all(self,table_name):
           query = f"SELECT * FROM {table_name}"
           return self.db.get_data(query)
       
       def get_by_ad_code(self, table_name ,ad_code):
           params = (ad_code,)
           query = f"SELECT * FROM {self.table_name} WHERE ad_cod=%s"
           return self.db.get_data(query, params)
       
       def add_data(self,table_name, values):
           fields = ', '.join(values.keys())
           values2 = ', '.join(['%s'] * len(values))
           query = f"INSERT INTO {self.table_name} ({fields}) VALUES ({values2})"
           params = tuple(values.values())
           self.db.execute_query(query, params)
           
           self.db.con.commit()
           
       def update(self, table_name,ad_code, values):
           values2 = ', '.join([f"{key}=%s" for key in values.keys()])
           query = f"UPDATE {self.table_name} SET {values2} WHERE ad_code=%s"
           params = tuple(values.values()) + (id,)
           self.db.execute_query(query, params)
           self.db.con.commit()
       def delete(self,table_name, ad_code):
           query = f"DELETE FROM {self.table_name} WHERE ad_code=%s"
           params = (ad_code,)
           self.db.execute_query(query, params)   
           self.db.con.commit()


# دیتا تایپ ها برای ساخت جدول
data_types = {
    "id" : "serial",
    "title":"varchar(150)",
    "total_price":"numeric",
    "price_per_meter":"numeric",
    "city":"varchar(50)",
    'region':"int",
    "address":"varchar(200)",
    'parking':"bigint",
    'Meterage':"bigint",
    "bedroom":"int",
    "age_year":'varchar(50)',
    "adviser":"varchar(100)",
    "real_estate":"varchar(100)",
    "ad_code":"bigint PRIMARY KEY",
    "phone":"varchar(100)",
    "description":"varchar(1000)",
    'sauna':"varchar(50)", 
    'proportionate_shares':"varchar(50)",
    'roof_garden':"varchar(50)",
    'balcony':"varchar(50)",
    'sports_hall':"varchar(50)", 
    'exchange':"varchar(50)",
    'guardian':"varchar(50)",
    'agreed_price':"varchar(50)",
    'Elevator':"varchar(50)",
    'Jacuzzi':"varchar(50)",
    'have_loan':"varchar(50)", 
    'conference_hall':"varchar(50)",
    'newly_built':"varchar(50)",
    'mall':"varchar(50)",
    'lobby':"varchar(50)", 
    'remote_door':"varchar(50)",
    'air_conditioning':"varchar(50)",
    'pool':"varchar(50)",
    'Central_antenna':"varchar(50)", 
    'Warehouse':"varchar(50)"
}



config={"database":"template1",
                        "host":"localhost",
                        "user":"postgres",
                        "password":"1234",
                        "port":"1234"}

       
table_name =  "estate_region5"


# db = Database(config)

db = Database(config,config.get("database","template0"))  
   
db.connect()

db.drop_table(table_name=table_name)

db.create_table(table_name,data_types,list(data_types.keys()))
   # ایجاد شی از کلاس Model برای جدول 'users'
estate = CRUD(db)

# insert data by dataframe

df=pd.read_excel("homes.xlsx")
df.age_year=df.age_year.astype(object).fillna("null")
df["phone"]=df["phone"].astype(object).fillna("null")

estate.insert_into_table(df=df,table_name=table_name)


# add data by dict or you can convert data frame with this >>> new_home = df.to_dict(); db.add_data( )



all_homes= estate.get_all(table_name)

# tehran5.update( table_name,ad_code, values)


# delete by ad_code
# tehran5.delete(ad_code)



db.disconnect()

print(all_homes.head())