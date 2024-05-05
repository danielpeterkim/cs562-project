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

def process_conditions(conditions):
    processed_conditions = []
    for condition in conditions:
        condition = re.sub(r'\d+\.', '', condition)  # remove numbers followed by "."
        parts = condition.split('=')
        if len(parts) == 2 and parts[0].strip() != parts[1].strip():
            processed_conditions.append(condition)
    
    final_condition = ' OR '.join(f"({cond})" for cond in processed_conditions)
    return final_condition

def check_query_keywords(query):
    query_lower = query.lower()
    
    found_keywords = [keyword for keyword in ['cust', 'prod', 'day', 'month', 'year', 'state', 'quant', 'date'] if keyword in query_lower]
    return found_keywords

def transform_condition_string(input_string):
    pattern = r"(\d+)\.([a-zA-Z_]+)"
    transformed_string = re.sub(pattern, r"row['\2']", input_string)
    return add_h_row_prefix(transformed_string)

def add_h_row_prefix(input_string):
    keywords = ['cust', 'prod', 'day', 'month', 'year', 'state', 'quant', 'date']
    keyword_pattern = re.compile(r'\b(' + '|'.join(re.escape(k) for k in keywords) + r')\b(?! *\])')
    combined_pattern = re.compile(r"row\['[^']+'\]|" + keyword_pattern.pattern)
    final_string = []
    last_end = 0
    for match in combined_pattern.finditer(input_string):
        final_string.append(input_string[last_end:match.start()])
        if match.group(0).startswith("row["):
            final_string.append(match.group(0))
        else:
            # It's a keyword, add h_row prefix
            final_string.append(f"h_row.{match.group(0)}")
        last_end = match.end()
    final_string.append(input_string[last_end:])
    
    return ''.join(final_string)


def having_to_condition(input_string):
    pattern = r'\b(\w+_\d+_\w+)\b'
    def replacer(match):
        variable = match.group(1)
        return f"h_row.{variable}"
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
    combined_input = f"{s} {v} {f} {' '.join(sigma)} {g}"
    select_clause = ', '.join(check_query_keywords(combined_input))
    where_clause = process_conditions(sigma) if sigma else ""

    final_query = f"SELECT {select_clause} FROM sales"
    if where_clause:
        final_query += f" WHERE {where_clause}"

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
    h_table_grouping_attr_time = time.time()
    h_table_grouping_attr_time_total = h_table_grouping_attr_time - start_time
    print(f"H Table Grouping Atrr executed in {{h_table_grouping_attr_time_total:.2f}} seconds.")
    """
    aggInstanceCode = """"""
    for z in range(n):
        aggInstanceCode += f"""
    predicate_string = "{transform_condition_string(predicates[z])}"
    h_table_aggrefunc_time_start = time.time()

    for key, h_row in instances.items():
            agg_instance = []
            split_key = key.split('@')
            split_key = [pair.split('-') for pair in split_key]
            such_that_time_start = time.time()
            for row in cur:
                isUsed = True
                if isUsed:
                    such_that_time_start = time.time()
                    if not(eval(predicate_string)):
                        isUsed = False  
                    if isUsed:
                        agg_instance.append(row)  
                    such_that_time_end = time.time()
                    such_that_time_total =  such_that_time_end -  such_that_time_start
                    print(f" Such That Mini Table {z} Time executed in {{such_that_time_total:.2f}} seconds.")
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
    h_table_aggrefunc_time_end = time.time()
    h_table_aggrefunc_time_total = h_table_aggrefunc_time_end - h_table_aggrefunc_time_start
    print(f" H Table AggreFunc {z} Time executed in {{h_table_aggrefunc_time_total:.2f}} seconds.")
    """
    
    # so having will go through each "row" of instance. if it doesn't fufill predicate's logic, then delte the row. else nothing
    having = f""" 
    keys_to_remove = []
    if {having_clause != ""}:
        having_time_start = time.time()
        having_string = "{having_to_condition(having_clause)}"
        for key, h_row in instances.items():
            if not(eval(having_string)):
                keys_to_remove.append(key)
        for key in keys_to_remove:
            del instances[key]
        having_time_end = time.time()
        having_time_total = having_time_end - having_time_start
        print(f" Having Time executed in {{having_time_total:.2f}} seconds.")
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
    
    """
    

    # Note: The f allows formatting with variables.
    #       Also, note the indentation is preserved.
    tmp = f"""
import os
import psycopg2
import psycopg2.extras
import tabulate
import time
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
    start_time = time.time()
    cur.execute(f"{final_query}")  
    {body}
    {aggInstanceCode}
    
    {having}
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Query executed in {{elapsed_time:.2f}} seconds.")
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