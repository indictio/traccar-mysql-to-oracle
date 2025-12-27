import mysql.connector as mysql
import oracledb as oracle
import config
import sys
import os

"""
1. open mysql
2. open oracle
3. sync devices
4. sync positions


98. close mysql
99. close oracle
"""

# open db mysql
mys_db = mysql.connect(
  host=config.mysql_host,
  user=config.mysql_user,
  password=config.mysql_password,
  database=config.mysql_database
)

mys_cursor = mys_db.cursor()

# open db oracle
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"
ora_db = oracle.connect(
  dsn=config.ora_dns
)

ora_cursor = ora_db.cursor()

# sync table tc_devices

ora_cursor.execute("delete tc_devices")

mys_cursor.execute("select * from tc_devices")
mys_rows = mys_cursor.fetchall()

sql = """
  insert into tc_devices(
    id, 
    name, 
    uniqueid, 
    lastupdate, 
    positionid, 
    groupid, 
    attributes, 
    phone, 
    model, 
    contact, 
    category, 
    disabled, 
    status, 
    expirationdate, 
    motionstate, 
    motiontime, 
    motiondistance, 
    overspeedstate, 
    overspeedtime, 
    overspeedgeofenceid, 
    motionsteak)
  values (
    :id, 
    :name, 
    :uniqueid, 
    :lastupdate, 
    :positionid, 
    :groupid, 
    :attributes, 
    :phone, 
    :model, 
    :contact, 
    :category, 
    :disabled, 
    :status, 
    :expirationdate, 
    :motionstate, 
    :motiontime, 
    :motiondistance, 
    :overspeedstate, 
    :overspeedtime, 
    :overspeedgeofenceid, 
    :motionsteak)
"""
for mys_row in mys_rows:
  print(mys_row[0])
  ora_cursor.execute(sql, [mys_row[0], mys_row[1], mys_row[2], mys_row[3], mys_row[4], mys_row[5], mys_row[6], mys_row[7], mys_row[8], mys_row[9], mys_row[10], mys_row[11], mys_row[12], mys_row[13], mys_row[14], mys_row[15], mys_row[16], mys_row[17], mys_row[18], mys_row[19], mys_row[20]])

# ora_db.commit()

# sys.exit(0)



# sync table tc_positions

ora_cursor.execute("delete tc_positions")

# select records from mysql
mys_cursor.execute("select * from tc_positions")
mys_rows = mys_cursor.fetchall()

# prepare sql for oracle insert
sql = """
  insert into tc_positions(
    id, 
    protocol, 
    deviceid, 
    servertime, 
    devicetime, 
    fixtime, 
    valid, 
    latitude, 
    longitude, 
    altitude, 
    speed, 
    course, 
    address, 
    attributes, 
    accuracy, 
    network, 
    geofenceids) 
  values(
    :id, 
    :protocol, 
    :deviceid, 
    :servertime, 
    :devicetime, 
    :fixtime, 
    :valid, 
    :latitude, 
    :longitude, 
    :altitude, 
    :speed, 
    :course, 
    :address, 
    :attributes, 
    :accuracy, 
    :network, 
    :geofenceids)
"""
# loop to insert into oracle and delete from mysql
for mys_row in mys_rows:
  print(mys_row[0])
  row_valid = 0
  row_network = None
  row_gefenceids = None
  if mys_row[6]==True:
    row_valid = 1
  if mys_row[15] != "null":
    row_network = mys_row[15]
  if mys_row[16] != "null":
    row_gefenceids = mys_row[16]

  # insert the row into oracle
  ora_cursor.execute(sql, [mys_row[0], mys_row[1], mys_row[2], mys_row[3], mys_row[4], mys_row[5], row_valid, mys_row[7], mys_row[8], mys_row[9], mys_row[10], mys_row[11], mys_row[12], mys_row[13], mys_row[14], row_network, row_gefenceids])

  # delete the same row from mysql
  mys_cursor.execute("delete from tc_positions where id = %s", [mys_row[0]])


# commit changes in oracle
# ora_db.commit()

# commit changes in mysql
# mys_db.commit()



# close db mysql
mys_cursor.close()
mys_db.close()

# close db oracle
ora_cursor.close()
ora_db.close()
