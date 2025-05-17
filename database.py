#!/usr/bin/env python3
import psycopg2

#####################################################
##  Database Connection
#####################################################

'''
Connect to the database using the connection string
'''
def openConnection():
    # connection parameters - ENTER YOUR LOGIN AND PASSWORD HERE

    myHost = "awsprddbs4836.shared.sydney.edu.au"
    userid = "y25s1c9120_sliu0188"
    passwd = "ehUYr8JZ"  #test
    
    # Create a connection to the database
    conn = None
    try:
        # Parses the config file and connects using the connect string
        conn = psycopg2.connect(database=userid,
                                    user=userid,
                                    password=passwd,
                                    host=myHost)

    except psycopg2.Error as sqle:
        print("psycopg2.Error : " + sqle.pgerror)
    
    # return the connection to use
    return conn

'''
Validate salesperson based on username and password
'''
def checkLogin(login, password):
    conn = openConnection()
    if conn is None:
        return None

    try:
        cursor = conn.cursor()
        # Case-insensitive match for username
        query = """
            SELECT Username, FirstName, LastName
            FROM Salesperson
            WHERE LOWER(Username) = LOWER(%s) AND Password = %s
        """
        cursor.execute(query, (login, password))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result:
            return list(result)  # ['jdoe', 'John', 'Doe']
        else:
            return None

    except Exception as e:
        print(f"Error during checkLogin: {e}")
        if conn:
            conn.close()
        return None


"""
    Retrieves the summary of car sales.

    This method fetches the summary of car sales from the database and returns it 
    as a collection of summary objects. Each summary contains key information 
    about a particular car sale.

    :return: A list of car sale summaries.
"""
def getCarSalesSummary():
    conn = openConnection()
    if conn is None:
        return []

    try:
        cursor = conn.cursor()
        query = """
            SELECT
                mk.MakeName,
                mo.ModelName,
                COUNT(*) FILTER (WHERE cs.IsSold = FALSE) AS AvailableUnits,
                COUNT(*) FILTER (WHERE cs.IsSold = TRUE) AS SoldUnits,
                COALESCE(SUM(cs.Price) FILTER (WHERE cs.IsSold = TRUE), 0) AS TotalSales,
                TO_CHAR(MAX(cs.SaleDate) FILTER (WHERE cs.IsSold = TRUE), 'DD-MM-YYYY') AS LastPurchasedAt
            FROM Model mo
            JOIN Make mk on mo.MakeCode = mk.MakeCode
            LEFT JOIN CarSales cs on mk.MakeCode = cs.MakeCode and mo.ModelCode = cs.ModelCode
            GROUP BY mk.MakeName, mo.ModelName
            ORDER BY mk.MakeName, mo.ModelName
        """
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        conn.close()


        summary = [
            {
                'make': row[0],
                'model': row[1],
                'availableUnits': row[2],
                'soldUnits': row[3],
                'soldTotalPrices': f"{row[4]:.2f}",
                'lastPurchaseAt': row[5] if row[5] else ''
            }
            for row in results
        ]
        return summary

    except Exception as e:
        print(f"Error during getCarSalesSummary: {e}")
        if conn:
            conn.close()
        return []

"""
    Finds car sales based on the provided search string.

    This method searches the database for car sales that match the provided search 
    string. See assignment description for search specification

    :param search_string: The search string to use for finding car sales in the database.
    :return: A list of car sales matching the search string.
"""
def findCarSales(searchString):
    print(searchString)
    conn = openConnection()
    if conn is None:
        return []

    try:
        cursor = conn.cursor()
        searchString = searchString.strip()
        query = """
                SELECT
                    CarSaleID,
                    mk.MakeName,
                    mo.ModelName,
                    BuiltYear,
                    Odometer,
                    Price,
                    IsSold,
                    SaleDate,
                    (c.FirstName || ' ' || c.LastName) AS BuyerName,
                    (s.FirstName || ' ' || s.LastName) AS SalespersonName
                FROM CarSales cs 
                LEFT JOIN Make mk on cs.MakeCode = mk.MakeCode 
                LEFT JOIN Model mo on cs.ModelCode = mo.ModelCode
                LEFT JOIN Customer c on cs.BuyerID = c.CustomerID 
                LEFT JOIN Salesperson s on cs.SalespersonID = s.UserName 
                WHERE 
                    (IsSold = FALSE OR (IsSold = TRUE AND SaleDate >= CURRENT_DATE - INTERVAL '3 years'))
                    AND (
                        LOWER(mk.MakeName) LIKE LOWER('%%' || %s || '%%')
                        OR LOWER(mo.ModelName) LIKE LOWER('%%' || %s || '%%')
                        OR LOWER((c.FirstName || ' ' || c.LastName)) LIKE LOWER('%%' || %s || '%%')
                        OR LOWER((s.FirstName || ' ' || s.LastName)) LIKE LOWER('%%' || %s || '%%')
                    )
                ORDER BY 
                    IsSold, 
                    CASE WHEN IsSold = TRUE THEN SaleDate ELSE NULL END ASC,
                    mk.MakeName ASC,
                    mo.ModelName ASC
            """
        params = (searchString, searchString, searchString, searchString)
        cursor.execute(query, params)  
        print(2)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        print(results)

        car_sales = [
            {
                'carsale_id': row[0],
                'make': row[1],
                'model': row[2],
                'builtYear': row[3],
                'odometer': row[4],
                'price': f"{row[5]:.2f}",
                'isSold': row[6],
                'sale_date': row[7] if row[7] else '',
                'buyer': row[8] if row[8] else '',
                'salesperson': row[9] if row[9] else ''
            }
            for row in results
        ]
        return car_sales

    except Exception as e:
        print(f"Error during findCarSales: {e}")
        if conn:
            conn.close()
        return []

"""
    Adds a new car sale to the database.

    This method accepts a CarSale object, which contains all the necessary details 
    for a new car sale. It inserts the data into the database and returns a confirmation 
    of the operation.

    :param car_sale: The CarSale object to be added to the database.
    :return: A boolean indicating if the operation was successful or not.
"""
def addCarSale(make, model, builtYear, odometer, price):
    conn = openConnection()
    if conn is None:
        return False
    
    try:
        cur = conn.cursor()
        
        try:
            builtYear = int(builtYear)
            odometer = int(odometer)
            price = float(price)
        except (ValueError, TypeError):
            cur.close()
            conn.close()
            return False
        
        cur.execute("SELECT add_car_sale(%s, %s, %s, %s, %s)", 
                    (make, model, builtYear, odometer, price))
        
        conn.commit()
        
        cur.close()
        conn.close()
        
        return True
    
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
            conn.close()
        print("Database error:", e)
        return False

"""
    Updates an existing car sale in the database.

    This method updates the details of a specific car sale in the database, ensuring
    that all fields of the CarSale object are modified correctly. It assumes that 
    the car sale to be updated already exists.

    :param car_sale: The CarSale object containing updated details for the car sale.
    :return: A boolean indicating whether the update was successful or not.
"""
def updateCarSale(carsaleid, customer, salesperson, saledate):
    conn = openConnection()
    if conn is None:
        return False
    
    try:
        cur = conn.cursor()
        
        try:
            carsaleid = int(carsaleid)
        except (ValueError, TypeError):
            cur.close()
            conn.close()
            return False
        
        
        cur.execute("SELECT update_car_sale(%s, %s, %s, %s)", 
                    (carsaleid, customer, salesperson, saledate))
        result = cur.fetchone()[0]
        
        conn.commit()
        
        cur.close()
        conn.close()
        
        return result
    
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
            conn.close()
        print("Database error:", e)
        return False
