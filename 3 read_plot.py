from sys import exit
import psycopg2 as pg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# connect and read data from database
class Database:
     
            
    def __init__(self, config):
           self.config = config
           self.con = None
           self.cur = None
           
    def connect(self):
            try:
                self.con = pg2.connect(**config)    
            except:
                print ("Unable to connect!")
                exit(1)
            else: 
                print ("Connected!")
                
                self.cur = self.con.cursor()
               
                self.cur.execute("SELECT current_database()")
                print('current database')
                for r in self.cur.fetchall():
                    print(r)
                print('\n')   
                 
    def disconnect(self):
           self.cur.close()
           self.con.close()
           
           
    def get_all(self,table_name):
           query = f"SELECT * FROM {table_name}"
           self.cur.execute(query)
           data = self.cur.fetchall()
           cols = [col[0] for col in self.cur.description]
           df = pd.DataFrame(data, columns=cols)

           return df
                       
config={"database":"template1",
                        "host":"localhost",
                        "user":"postgres",
                        "password":"1234",
                        "port":"1234"}


table_name =  "estate_region5"

db = Database(config)
   
db.connect()

df=db.get_all(table_name)

db.disconnect()

# delete redundant words in neighborhoods
redundant = ["جنوبی","مرکزی","غربی","بلوار", 'شرقی','شمالی']
for i in redundant:
    df["address"]=df["address"].str.replace(i,"",regex=True)
    df["address"]=df["address"].str.strip()


# Selecting and extracting the required data

neighborhoods=df[df.total_price.notna()].loc[:,["total_price","address"]].copy()
farsi_neighborhood=list(neighborhoods.address.unique())
eng_neighborhoods="""
'Ferdos', 'Janat', 'Plan', 'Mehran', 'Water',
'Pyambar', 'Faiz', 'ziba', 'Punak', 'Shahran'
""".replace("\n","").replace("'","").split(",")
eng_neighborhoods = list(map(lambda i: i.strip() , eng_neighborhoods))
replace_neighborhoods = {k:v for k,v in zip(farsi_neighborhood,eng_neighborhoods)}
replace_neighborhoods
neighborhoods["address"]= neighborhoods["address"].replace(replace_neighborhoods ,regex=True)

neighborhoods["total_price"]=neighborhoods["total_price"].astype(float)
neighborhoods=neighborhoods.groupby('address').agg(['count','mean'])
neighborhoods

#  ploting 
fig, (ax1, ax2) = plt.subplots(1,2, figsize = (24, 6))
sns.barplot(x=neighborhoods.iloc[:,0].index, y=neighborhoods.iloc[:,0],ax=ax1)
ax1.grid()
ax1.axhline(y=5, color='r', linewidth=1) 
ax1.axhline(y=15, color='b', linewidth=1) 
ax1.set_xlabel("different neighborhoods", fontsize = 13);
ax1.set_ylabel("Number of listings");
ax1.set_title("The number of listings per each neighborhoods", fontsize = 20);
sns.barplot(x=neighborhoods.iloc[:,1].index, y=neighborhoods.iloc[:,1],ax=ax2)
ax2.grid()
ax2.axhline(y=5, color='b', linewidth=1) 

ax2.set_xlabel("different neighborhoods", fontsize = 13);
ax2.set_ylabel("Average price of each neighborhoods");
ax2.set_ylabel("Average at the rate of billion toman");
ax2.set_title("Average price in different neighborhoods", fontsize = 20);

plt.show()