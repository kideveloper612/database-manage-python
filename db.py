import mysql.connector
import os
import csv
import glob
import zipfile
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
                     "phone VARCHAR(15), "
                     "ip VARCHAR(20), "
                     "DOB VARCHAR(20), "
                     "gender VARCHAR(3), "
                     "PRIMARY KEY(id)"
                     ")".format(db_name))


def read_file(name):
    with open(file=name, encoding='utf-8', mode='r') as csv_file:
        rows = list(csv.reader(csv_file))
    return rows


def get_file_paths():
    directory = 'F:\\working\\python\\scrapping\\Location_Marco\\Newly\\Individuals\\fastpeoplesearch\\Database\\'
    files = []
    for name in glob.glob('{}*.zip'.format(directory)):
        base = os.path.basename(name)
        filename = os.path.splitext(base)[0]
        data = filename
        archive = '.'.join([data, 'zip'])
        full_path = ''.join([directory, archive])
        csv_file = '.'.join([data, 'csv'])
        files.append(csv_file)
    return files, full_path


def main():
    full_path, file_names = get_file_paths()
    for file_name in file_names:
        zf = zipfile.ZipFile(full_path)
        df = pd.read_csv(zf.open(csv_file))
        print(df.values.tolist()[2])
    db_create('customers')
    path = '166.csv'
    lines = read_file(name=path)
    sql = "INSERT INTO customers (email, family, given, address, city, state, zip, phone, ip, DOB, gender) VALUES (%s, " \
          "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mycursor.executemany(sql, lines)
    print(mycursor.rowcount, "was inserted.")


if __name__ == '__main__':
    main()
