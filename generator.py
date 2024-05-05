import subprocess
import re

def get_arguments_manually():
    s = input("Enter the list of projected attributes (comma-separated): ")
    n = int(input("Enter the number of grouping variables(comma-separated): "))
    v = input("Enter the list of grouping attributes (comma-separated): ")
    f = input(f"Enter list of aggregate functions for the grouping variables (comma-separated): ")
    sigma = [input(f"Enter predicate for grouping variable {i+1}: ") for i in range(n)]
    g = input("Enter predicate for the 'having' clause: ")
    return s, n, v, f, sigma, g

def get_arguments_from_file(filename):
    with open(filename, 'r') as file:
        lines = file.readlines()
        s = lines[0].strip()
        n = int(lines[1].strip())
        v = lines[2].strip()
        f = lines[3].strip()
        sigma = [line.strip() for line in lines[4:4+n]]
        g = lines[4+n].strip()
    return s, n, v, f, sigma, g


def transform_condition_string(input_string):
    pattern = r"(\d+)\.([a-zA-Z_]+)"
    transformed_string = re.sub(pattern, r"row['\2']", input_string)
    return transformed_string

def having_to_condition(input_string):
    pattern = r'\b(\w+_\d+_\w+)\b'
    def replacer(match):
        variable = match.group(1)
        return f"row['{variable}']"
    having_condition = re.sub(pattern, replacer, input_string)
    return having_condition
def main(s, n, v, f, sigma, g):
    """
    This is the generator code. It should take in the MF structure and generate the code
    needed to run the query. That generated code should be saved to a 
    file (e.g. _generated.py) and then run.
    """
    slt = [item.strip() for item in s.split(',')]
    grouping_attributes = [item.strip() for item in v.split(',')]
    aggregate_functions = [item.strip() for item in f.split(',')]
    predicates = sigma
    having_clause = g
    body = f"""
    instances = {{}}
    
    for row in cur:
        # Create a unique key
        attributesFormattedForKey = ""
        hInstan = {{}}
        for x in {grouping_attributes}:
            attributesFormattedForKey += f"{{x}}-{{row[x]}}@"
            hInstan[x] = row[x]
        attributesFormattedForKey = attributesFormattedForKey[:-1]
        #adds placeholder values in H class for aggregate functions
        for y in {aggregate_functions}:
            hInstan[y] = None
        key = attributesFormattedForKey
        if key not in instances:
            instances[key] = H(**hInstan)
    cur.scroll(0, mode='absolute')
    """
    aggInstanceCode = """"""
    for z in range(n):
        aggInstanceCode += f"""
    print({z})
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
                    if not(eval("{transform_condition_string(predicates[z])}")):
                        isUsed = False
                    if isUsed:
                        agg_instance.append(row)  
            for x in {aggregate_functions}: # for calculating the aggregate functions for the H-class table
                split_x = x.split("_")
                if split_x[0] == "sum" and split_x[1] == str({z + 1}) :
                    sum = 0
                    for l in agg_instance: 
                        sum += l[split_x[2]]
                    setattr(instances[key], x, sum)
                    
                if split_x[0] == "count" and split_x[1] == str({z + 1}) :
                    count = len(agg_instance)
                    setattr(instances[key], x, count)

                if split_x[0] == "min" and split_x[1] == str({z + 1}) :
                    first = True
                    for l in agg_instance:
                        if first:
                            min = l[split_x[2]]
                            first = False
                        else:
                            if l[split_x[2]] < min:
                                min = l[split_x[2]]
                    setattr(instances[key], x, min)

                if split_x[0] == "max" and split_x[1] == str({z + 1}) :
                    first = True
                    for l in agg_instance:
                        if first:
                            max = l[split_x[2]]
                            first = False
                        else:
                            if (l[split_x[2]] > max):
                                max = l[split_x[2]]
                    setattr(instances[key], x, max)
                
                if split_x[0] == "avg" and split_x[1] == str({z + 1}) :
                    sum = 0
                    count = len(agg_instance)
                    for l in agg_instance: 
                        sum += l[split_x[2]]
                    avg = sum/count
                    setattr(instances[key], x, avg)
                               
            cur.scroll(0, mode='absolute')
    """
    
    groupv =""""""
    select = v + ", " + f
    aggv =""""""
    for x in grouping_attributes:
        groupv += f"""
        
        mysillyobject.{x} = {x}
        """
    for x in aggregate_functions:
        aggv += f"""

        mysillyobject.{x} = {x}
        """
     
    hclass = f"""
    def __init__(mysillyobject, {select}):
        {groupv}
        {aggv}
        
    def printAllClassAttr(abc):
        attrs = vars(abc)
        for attr, value in attrs.items():
            print(f'{{attr}}: {{value}}')
    
    """

    # Note: The f allows formatting with variables.
    #       Also, note the indentation is preserved.
    tmp = f"""
import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv

# DO NOT EDIT THIS FILE, IT IS GENERATED BY generator.py

class H:
{hclass}
def query():
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect(host = 'localhost', dbname = dbname, user = user, password = password, port = 5432)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SELECT * FROM sales")  
    {body}
    {aggInstanceCode}
    table_data = [vars(inst) for inst in instances.values()]
    return tabulate.tabulate(table_data, headers="keys", tablefmt="psql")

def main():
    print(query())
    
if "__main__" == __name__:
    main()
    """

    # Write the generated code to a filep
    open("_generated.py", "w").write(tmp)
    # Execute the generated code
    subprocess.run(["python", "_generated.py"])


if "__main__" == __name__:
    method = input("Type 'manual' for manual input, or 'file' for file input: ")
    if method.lower() == 'manual':
        args = get_arguments_manually()
    elif method.lower() == 'file':
        filename = input("Enter the filename: ")
        args = get_arguments_from_file(filename)
    else:
        raise ValueError("Invalid input method specified. Use 'manual' or 'file'.")
    main(*args)