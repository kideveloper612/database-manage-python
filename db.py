import mysql.connector
import os
import csv
import glob
import zipfile
import rarfile
import pandas as pd

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="search"
)

mycursor = mydb.cursor()


def db_create(db_name):
    mycursor.execute("CREATE TABLE {} ("
                     "id INT NOT NULL AUTO_INCREMENT,"
                     "email VARCHAR(70), "
                     "family VARCHAR(50), "
                     "given VARCHAR(50), "
                     "address VARCHAR(100), "
                     "city VARCHAR(30), "
                     "state VARCHAR(20), "
                     "zip VARCHAR(10), "
                     "phone VARCHAR(15) UNIQUE, "
                     "ip VARCHAR(20), "
                     "gender VARCHAR(20), "
                     "DOB VARCHAR(20), "
                     "PRIMARY KEY(id) "
                     ")".format(db_name))


def main():
    # db_create('customers')
    directory = 'F:\\working\\python\\scrapping\\Location_Marco\\Newly\\Individuals\\fastpeoplesearch\\Database\\440_files\\'
    for name in glob.glob('{}*.zip'.format(directory)):
        base = os.path.basename(name)
        filename = os.path.splitext(base)[0]
        data = filename
        archive = '.'.join([data, 'zip'])
        full_path = ''.join([directory, archive])
        csv_file = '.'.join([data, 'csv'])
        zf = zipfile.ZipFile(full_path)
        df = pd.read_csv(zf.open(csv_file))
        df = df.astype(object).where(pd.notnull(df), None)
        sql = "INSERT INTO customers (email, family, given, address, city, state, zip, phone, ip, DOB, gender) " \
              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        print(csv_file, full_path)
        try:
            mycursor.executemany(sql, df.values.tolist())
            mydb.commit()
            print(csv_file, "was inserted.")
        except mysql.connector.errors as e:
            print(e)
            continue


if __name__ == '__main__':
    main()
