import subprocess


def get_arguments_manually():
    s = input("Enter the list of projected attributes (comma-separated): ")
    n = int(input("Enter the number of grouping variables(comma-separated): "))
    v = input("Enter the list of grouping attributes (comma-separated): ")
    f = input(f"Enter list of aggregate functions for the grouping variables (comma-separated): ")
    sigma = [input(f"Enter predicate for grouping variable {i+1}: ") for i in range(n)]
    g = input("Enter predicate for the 'having' clause: ")
    return s, n, v, f, sigma, g


def main(s, n, v, f, sigma, g):
    """
    This is the generator code. It should take in the MF structure and generate the code
    needed to run the query. That generated code should be saved to a 
    file (e.g. _generated.py) and then run.
    """
    attributes = s.split(',')
    grouping_attributes = v.split(',')
    aggregate_functions = f.split(',')
    predicates = sigma
    having_clause = g


    body = """
    for row in cur:
        if row['quant'] > 10:
            _global.append(row)
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

def query():
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                            cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    cur.execute("SELECT * FROM sales")
    
    _global = []
    {body}
    
    return tabulate.tabulate(_global,
                        headers="keys", tablefmt="psql")

def main():
    print(query())
    
if "__main__" == __name__:
    main()
    """

    # Write the generated code to a file
    open("_generated.py", "w").write(tmp)
    # Execute the generated code
    subprocess.run(["python", "_generated.py"])


if "__main__" == __name__:
    args = get_arguments_manually()
    main(*args)
