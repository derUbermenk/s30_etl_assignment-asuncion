import sqlite3
import csv

def sql_solution():
    with sqlite3.connect("./Data_Engineer_Assignment/S30_ETL_Assignment.db") as con:
        cur = con.cursor()
        res = cur.execute("""
            WITH filtered_orders AS (
                SELECT
                    *,
                    FLOOR(quantity) 'floored_quantity'
                FROM orders
                WHERE quantity >= 1
            )
            SELECT
                s.customer_id 'Customer',
                c.age 'Age',
                i.item_name 'Item',
                SUM(fo.floored_quantity) 'Quantity'
            FROM filtered_orders fo 
            LEFT JOIN sales s
            ON fo.sales_id = s.sales_id 
            LEFT JOIN items i
            ON fo.item_id = i.item_id
            LEFT JOIN customers c
            ON s.customer_id = c.customer_id
            GROUP BY s.customer_id, c.age, i.item_id, i.item_name
            ORDER BY c.age ASC
        """)
        results = res.fetchall()

        with open('./output/sql_solution_results.csv', 'w', newline='') as f:
            fieldnames = ['Customer', 'Age', 'Item', 'Quantity']
            writer = csv.writer(f, delimiter=';')
            writer.writerow(fieldnames)
            writer.writerows(results)
    
if __name__ == "__main__":
    sql_solution()
