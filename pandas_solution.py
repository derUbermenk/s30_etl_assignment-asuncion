import sqlite3
import pandas as pd

def pandas_solution_1():
    with sqlite3.connect("./Data_Engineer_Assignment/S30_ETL_Assignment.db") as con:
        query = """
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
        """
        result = pd.read_sql(query, con=con)
        result.to_csv('./output/pandas_solution_1_results.csv', sep=";", index=False)

# in case looking for solution with minimal sql use
def pandas_solution_2():
    with sqlite3.connect("./Data_Engineer_Assignment/S30_ETL_Assignment.db") as con:
        sales = pd.read_sql('SELECT * FROM sales', con=con)        
        orders = pd.read_sql('SELECT * FROM orders', con=con)        
        items = pd.read_sql('SELECT * FROM items', con=con)        
        customers = pd.read_sql('SELECT * FROM customers', con=con)        

        # filter orders
        filtered_orders = orders[orders.quantity >= 1]
        filtered_orders['floored_quantity'] = filtered_orders.quantity.astype(int)

        # merge tables
        filtered_orders_items = filtered_orders.merge(items, how='left', on='item_id')
        filtered_orders_items_sales = filtered_orders_items.merge(sales, how='left', on='sales_id') 
        all_merged = filtered_orders_items_sales.merge(customers, how='left', on='customer_id') 

        # aggregate quantities 
        result = all_merged.groupby(by=['customer_id', 'age', 'item_id', 'item_name'])['floored_quantity'].sum().reset_index()

        # format result
        result.rename(columns={'customer_id': 'Customer', 'age': 'Age', 'item_name': 'Item', 'floored_quantity': 'Quantity'}, inplace=True)
        result = result[['Customer', 'Age', 'Item', 'Quantity']].sort_values(by=["Age", "Customer"])

        result.to_csv('./output/pandas_solution_2_results.csv', sep=";", index=False)

if __name__ == "__main__":
    pandas_solution_1()