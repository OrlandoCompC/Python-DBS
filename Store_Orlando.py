'''
Name: Orlando Companioni
date: 2023/03/28

This program will read from a csv and a txt file and then insert the data into a database
then it will use the database to preform some queries and then it will print the results
'''
import mysql.connector
import csv


class Item(): #class for the items
    def __init__(self,iid,name,category,price):
        self.iid=iid
        self.name=name
        self.category=category
        self.price=price

    #getters
    def get_iid(self):
        return self.iid
    
    def get_name(self):
        return self.name
    
    def get_category(self):
        return self.category
    
    def get_price(self):
        return self.price
    
    def getdict(self): #this will return a dictionary with all the data
        return {self.iid:[self.name,self.category,self.price]}
    

class Transaction(): #class for the transactions
    def __init__(self,transaction_id,iid,quantity):
        self.transaction_id=transaction_id
        self.iid=iid
        self.quantity=quantity

    #getters
    def get_transaction_id(self):
        return self.transaction_id
    
    def get_iid(self):
        return self.iid
    
    def get_quantity(self):
        return self.quantity
    
    def getdict(self): #this will return a dictionary with all the data
        return {self.transaction_id:[self.iid,self.quantity]}



def main(): #this is the controller
    process()
    #closing the connection
    try:
        conn.close()
        prepared_cursor.close()
    except Exception as err:
        print(f"\033[1;44mConnection has been closed")
        print(f"\033[1;44mThank you for using the database by Orlando Companioni")

def process(): #this is the process function that will call all the other functions
    create_creation()
    prepared_cursor()
    read_csv()
    insertRecord_items()
    read_txt()
    insertRecord_Transactions()
    create_table()
    read_tableItems()
    item_object() #this will create the objects for the items
    object_item_dict() #this will create a dictionary with all the objects

    read_tabletransactions()
    transaction_object() #this will create the objects for the transactions
    object_transaction_dict() #this will create a dictionary with all the objects
    compute_table_values()
    user_choose()
    transaction_total()
    more_than_3()
    custom_select()


def read_txt(): # this will read the contents of the text file
    #I will put all the contents into a list and then into a dictionary organized by transaction ID
    global transactionDict # making it global so that all other functions can use it
    txtlist=[]
    transactionDict={}
    with open('Transactions.txt','r') as f:
        data=csv.reader(f) # im using the csv reader because it will read the file line by line and its easy to control
        next(data)
        next(data)#skips the first 2 lines
        for line in data:
            for item in line:
                txtlist.append(item.split(' '))
    for item in txtlist:
        transactionDict[item[0]]=item[1:3] # this will put the data into a dictionary organized by transaction ID
    print(f"\033[1;44m\nDATA READ FROM THE TRANSACTION FILE............\033[0;0m")
    

def read_csv(): # Going to read the csv file
    global itemDict #making it global so that all other functions can use it
    while True:
        try:
            name=input("Please enter your name: ").capitalize()
            with open(f'Items_{name}.csv','r')as f:
                reader=csv.DictReader(f)
                itemDict=[row for row in reader]
            print(f"\033[1;44m\nDATA READ FROM THE ITEM FILE............\033[0;0m")
            break
        except Exception as err:
            print("Invalid name, there is no file associated with that name")
            print("Please try again")
            continue


    
def create_creation():
    global conn # its made so that all other functions can use it
    while True:
        try:
            username = input("Please enter your username: ")
            pword=input("Please enter your password: ")
            dbase=input("Please enter the database name: ")
            try:
                conn = mysql.connector.connect(host="localhost", user=username, password=pword, database=dbase)
            except Exception as err:
                print(err)
            else:
                print(conn)
                print(f'\033[1;34mCONNECTION TO THE DATABASE ESTABLISHED...\033[0;0m')
                break
        except Exception as err:
            continue



def prepared_cursor(): #Using this for a prepared cursor
    global prepared_cursor
    prepared_cursor=conn.cursor(prepared=True) #have to add the prepared = True
    print(f'\033[1;34mPREPARED CURSOR CREATED ............\033[0;0m')
    print(f"\033[1;44m\n%%%%%%%%%%%%%%% WELCOME TO THE DATABASE %%%%%%%%%%%%%%%%%%%%\033[0;0m")



def insertRecord_items():# inserts the data into the items table
    sql = 'insert ignore into Items_Orlando values(?,?,?,?)' #prepared statement
    values = [(item['iid'],item['name'],item['category'],item['price']) for item in itemDict] #making it a tuple
    prepared_cursor.executemany(sql,values)
    conn.commit()
    print(f'\033[1;34mDATA INSERTED INTO Items_Orlando TABLE............\033[0;0m')



def insertRecord_Transactions(): #inserts the data into the transactions table
    sql = 'insert ignore into Transactions values(?,?,?)' #prepared statement
    values = [(k,v[0],v[1]) for k,v in transactionDict.items()] #making it a tuple
    prepared_cursor.executemany(sql,values)
    conn.commit()
    print(f'\033[1;34mDATA INSERTED INTO Transactions TABLE............\n\033[0;0m')




#Create a table in the database using prepared cursor
def create_table():
    tables=("CategoryTotal_Dairy","CategoryTotal_Fruit", "CategoryTotal_Meat", "CategoryTotal_Snacks","CategoryTotal_Vegetables")
    for table in tables:
        sql = f'create table if not exists {table}(ItemID INT primary key, Item varchar(200),Amount DECIMAL(10,2))'
        try:
            prepared_cursor.execute(sql)
        except Exception as err:
            print(err)
        else:
            print('\033[1;34mTABLE CREATED............\033[0;0m')



def read_tableItems(): #will read from the table items
    global dbsitemDict #making it global so that all other functions can use it
    dbsitemDict={}
    sql = 'select * from Items_Orlando'
    prepared_cursor.execute(sql)
    for row in prepared_cursor:
        dbsitemDict[row[0]] = [row[1],row[2],float(row[3])]
    print(f"\033[1;44m\nDATA READ FROM TABLE Items_Orlando COMPLETED............\033[0;0m")
    

def item_object():#creating the objects on the global scope so that all other functions can use them
#read from the tableitemDict and create 10  objects
    global item1,item2,item3,item4,item5,item6,item7,item8,item9,item10
    counter=1
    for k in dbsitemDict.keys(): # this will create the objects
        item=Item(k,dbsitemDict[k][0],dbsitemDict[k][1],dbsitemDict[k][2])
        if counter==1:
            item1=item
        elif counter==2:
            item2=item
        elif counter==3:
            item3=item
        elif counter==4:
            item4=item
        elif counter==5:
            item5=item
        elif counter==6:
            item6=item
        elif counter==7:
            item7=item
        elif counter==8:
            item8=item
        elif counter==9:
            item9=item
        elif counter==10:
            item10=item
        counter+=1
 
    print(f"\033[1;44mITEM OBJECTS CREATED............\033[0;0m")

def object_item_dict(): #this will create a dictionary with all the item objects
    global tableitemDict
    tableitemDict={}
    items=[item1,item2,item3,item4,item5,item6,item7,item8,item9,item10]
    for item in items:
        tableitemDict.update(item.getdict())


def read_tabletransactions(): #will read from the table transactions
    global dbstransactionDict #making it global so that all other functions can use it
    dbstransactionDict={}
    sql = 'select * from transactions'
    prepared_cursor.execute(sql)
    for row in prepared_cursor:
        dbstransactionDict[row[0]] = [row[1],row[2]]
    print(f"\033[1;44mDATA READ FROM Transactions COMPLETED............\033[0;0m")

def transaction_object():#creating the objects on the global scope so that all other functions can use them
    global transac1,transac2,transac3,transac4,transac5,transac6,transac7,transac8,transac9,transac10
    counter=1
    for k in dbstransactionDict.keys(): # this will create the objects
        transaction=Transaction(k,dbstransactionDict[k][0],dbstransactionDict[k][1])

        #this checks and it matches each object with its corresponding name
        if counter==1:
            transac1=transaction
        elif counter==2:
            transac2=transaction
        elif counter==3:
            transac3=transaction
        elif counter==4:
            transac4=transaction
        elif counter==5:
            transac5=transaction
        elif counter==6:
            transac6=transaction
        elif counter==7:
            transac7=transaction
        elif counter==8:
            transac8=transaction
        elif counter==9:
            transac9=transaction
        counter+=1

    print(f"\033[1;44mTRANSACTION OBJECTS CREATED............\033[0;0m\n")

def object_transaction_dict(): #this will create a dictionary with all the item objects
    global tabletransactionDict
    tabletransactionDict={}
    transactions=[transac1,transac2,transac3,transac4,transac5,transac6,transac7,transac8,transac9]
    for transaction in transactions:
        tabletransactionDict.update(transaction.getdict())

    
def insert_record(itemID,itemName,total): #This is will insert the data into the respective tables
    for key,value in tableitemDict.items():
        if key==itemID:
            if value[1]=="Fruit":
                sql = 'insert ignore into CategoryTotal_Fruit values(?,?,?)'
                values = (itemID,itemName,total)
                prepared_cursor.execute(sql,values)
                conn.commit()
            elif value[1]=="Dairy":
                sql = 'insert ignore into CategoryTotal_Dairy values(?,?,?)'
                values = (itemID,itemName,total)
                prepared_cursor.execute(sql,values)
                conn.commit()
            elif value[1]=="Meat":
                sql = 'insert ignore into CategoryTotal_Meat values(?,?,?)'
                values = (itemID,itemName,total)
                prepared_cursor.execute(sql,values)
                conn.commit()
            elif value[1]=="Snacks":
                sql = 'insert ignore into CategoryTotal_Snacks values(?,?,?)'
                values = (itemID,itemName,total)
                prepared_cursor.execute(sql,values)
                conn.commit()
            elif value[1]=="Vegetables":
                sql = 'insert ignore into CategoryTotal_Vegetables values(?,?,?)'
                values = (itemID,itemName,total)
                prepared_cursor.execute(sql,values)
                conn.commit()
    print(f"\033[1;34mRECORD INSERTED\033[0;0m")


def total_price(value0,value1): #this finds the total price of each item 
    #it calculates the price * quantity and inserts it into the respective tables
    totalprice=0
    for k,v in tableitemDict.items():
        if k==value0:
            totalprice=totalprice+v[2]*value1
            insert_record(k,v[0],totalprice)
            totalprice=0

def compute_table_values(): #this will compute the total price of each item and insert it into the respective tables
    totalprice=0
    counter=0
    for k,v in tabletransactionDict.items():
        if v[0]==1:
            total_price(v[0],v[1])
        elif v[0]==2:
            total_price(v[0],v[1])
        elif v[0]==3:
            total_price(v[0],v[1])
        elif v[0]==4: #there are two cases where this item is bought in different amounts.
            for key,value in tableitemDict.items():
                if key==v[0]:
                    totalprice=totalprice+value[2]*v[1]
                    counter+=1 #this counter allows it to fully add the values before inserting it into the table
                    if counter==2:
                        insert_record(key,value[0],totalprice)
                        totalprice=0
        elif v[0]==5:
            total_price(v[0],v[1])
        elif v[0]==6:
            total_price(v[0],v[1])
        elif v[0]==7:
            total_price(v[0],v[1])
        elif v[0]==8:
            total_price(v[0],v[1])
        elif v[0]==9:
            total_price(v[0],v[1])
        elif v[0]==10:
            total_price(v[0],v[1])



def user_choose(): #this will allow the user to choose a category and it will display the table
    while True:
        print(f"\033[1;44mValid Categories: Fruit, Dairy, Meat, Snacks, Vegetables\033[0;0m")
        category=["Fruit","Dairy","Meat","Snacks","Vegetables"]
        choice=input("Please choose a category: ").capitalize()
        if choice in category:
            sql=f'select * from CategoryTotal_{choice}'
            prepared_cursor.execute(sql)
            table=prepared_cursor.fetchall()
            print(f"\033[1;44m{choice} Table\033[0;0m")
            print("ItemID\tItem\tAmount")
            for row in table:
                print(f"{row[0]}\t{row[1]}\t{row[2]}")
            break
        else:
            print("Please choose a valid category")
            continue



def transaction_total():
    #this will check the transactions and multiply the quantity of the item by the price of the item and add all of them together
    total = 0 
    for k,v in tabletransactionDict.items():
        for key,value in tableitemDict.items():
            if v[0]==key:
                total=total+value[2]*v[1]
                
    print(f"\033[1;32mThe Total Amount Of Money Spent On All Transactions Is: ${total}\033[0;0m\n")
        
        
def more_than_3():#this will calculate and display items that have a quantity greater than 3
    for k,v in tabletransactionDict.items():
        if v[1]>3:
            for key,value in tableitemDict.items():
                if v[0]==key:
                    print(f"\033[1;32mThe Item {value[0]} was bought {v[1]} times and its total price is: ${value[2]*v[1]}\033[0;0m")



def custom_select(): #allows the user to enter a custom select statement
    print('Please Remember The Statement Must Start With "select"')
    try:
        sql=input("Please enter a custom select statement: ").strip().lower()
        command=sql.split(" ")
        if command[0] != 'select':
            print("Invalid statement")
        else:
            if command[-1] != 'items_orlando':
                print("Invalid table used Must use items_orlando")
            else:
                prepared_cursor.execute(sql)
                table=prepared_cursor.fetchall()
                for row in table: # this will check if its a number then print it depending on the command
                    for i in range(len(row)):
                        if i==len(row)-1:
                            #check if its a float if it is then format it if it isnt then just print it
                            if isinstance(row[i],float):
                                print(f"{row[i]:.2f}\n")
                            else:
                                print(f"{row[i]}\n")
                        else:
                            print(f"{row[i]}",end=' ')
    except Exception as e:
        print(e)
        print("Please enter a valid statement")
   


if __name__=='__main__':
    main()
