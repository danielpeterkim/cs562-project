
import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv

# DO NOT EDIT THIS FILE, IT IS GENERATED BY generator.py

class H:

    def __init__(mysillyobject, cust, sum_1_quant, sum_2_quant):
        
        
        mysillyobject.cust = cust
        
        

        mysillyobject.sum_1_quant = sum_1_quant
        

        mysillyobject.sum_2_quant = sum_2_quant
        
        
    def printAllClassAttr(abc):
        attrs = vars(abc)
        for attr, value in attrs.items():
            print(f'{attr}: {value}')
    
    
def query():
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect(host = 'localhost', dbname = dbname, user = user, password = password, port = 5432)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SELECT * FROM sales")
    
    instances = {}
    
    for row in cur:
        # Create a unique key
        attributesFormattedForKey = ""
        hInstan = {}
        for x in ['cust']:
            attributesFormattedForKey += f"{x}-{row[x]}@"
            hInstan[x] = row[x]
        attributesFormattedForKey = attributesFormattedForKey[:-1]
        #adds placeholder values in H class for aggregate functions
        for y in ['sum_1_quant', 'sum_2_quant']:
            hInstan[y] = None
        key = attributesFormattedForKey
        if key not in instances:
            instances[key] = H(**hInstan)
    cur.scroll(0, mode='absolute')
    
    for key, h_row in instances.items():
        agg_instance = []
        split_key = key.split('@')
        split_key = [pair.split('-') for pair in split_key]
        for row in cur:
            isUsed = True
            for i in split_key:
                if row[i[0]] != i[1]:
                    isUsed = False
            if isUsed:
                print("1.state=='NJ'")
                print(row['state']=='NJ')
                if not(eval("row['state']=='NJ'")):
                   isUsed = False
                if isUsed:
                    agg_instance.append(row)            
        print(agg_instance)
        cur.scroll(0, mode='absolute')
    
    
    table_data = [vars(inst) for inst in instances.values()]
    return tabulate.tabulate(table_data, headers="keys", tablefmt="psql")

def main():
    print(query())
    
if "__main__" == __name__:
    main()
    