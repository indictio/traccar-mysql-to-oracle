import mysql.connector
import cx_Oracle as oracle
import config

mydb = mysql.connector.connect(
  host=config.mysql_host,
  user=config.mysql_user,
  password=config.mysql_password,
  database=config.mysql_database
)

mycursor = mydb.cursor()
mycursor.execute("select * from tc_devices")
myrows = mycursor.fetchall()

for myrow in myrows:
  print(myrow)

mycursor.close()
mydb.close()

oradb = oracle.connect(
  user=config.ora_user,
  password=config.ora_password,
  dsn=config.ora_dns,
  encoding=config.ora_encoding
)

oracursor = oradb.cursor()
oracursor.execute("select * from gps_vehicule")
orarows = oracursor.fetchall()
for orarow in orarows:
  print(orarow)


oracursor.close()
oradb.close()