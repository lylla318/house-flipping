import re
import json
import io
import csv
import sys
from pprint import pprint
from collections import defaultdict
import mysql.connector
import MySQLdb
import time
import datetime
from ast import literal_eval
from collections import MutableMapping




class Query:

  def __init__(self, years, password):


    self.years = years
    self.password = password
    self.query_output = self.query_acris()
    #self.test_query()
  

  # Just a test function. Messing around with different query syntax, testing MySQLDB capabilities.
  def test_query(self):

    print "Testing query function..."

    cnx = mysql.connector.connect(user='acrisuser', password=self.password,
                                  host='acris-dev.c1e2qqhlvvst.us-east-1.rds.amazonaws.com',
                                  database='acris')

    cursor = cnx.cursor(dictionary=True)

    query = "SELECT * FROM real_property_parties as parties JOIN real_property_legals as lots ON parties.documentid = lots.documentid AND lots.borough = 3 AND lots.block = 2317 AND lots.lot = 1 JOIN real_property_master as docs ON docs.documentid = parties.documentid AND lots.documentid = docs.documentid WHERE docs.docdate = '2010-11-3' "

    cursor.execute(query)

    result_set = cursor.fetchall()

    print result_set  


  # Organize parcel data into data structure
  # BBL stands for borough-block-lot of a given parcel
  # Data structure format: { "BBL":[sale1, sale2, ...], "BBL2": {...} }
  def query_acris(self):

    print("Reading data... ")
    OUTPUT = []
    ctr = 0

    for year in self.years:

      print("Reading..."+str(year))

      csv_read = "output_data/all-boroughs/flips_" + year + ".csv"
      reader = csv.reader(open(csv_read, 'rU'), delimiter=",", dialect=csv.excel_tab)
      ctr = 0

      for row in reader:

        if(ctr > 0 and ctr < 2):

          print("Processing flip number " + str(ctr))
       
          borough = row[0]
          block   = row[4]
          lot     = row[5]
          docdate = row[-1]

          print "DOCDATE: " + str(docdate)

          cnx = mysql.connector.connect(user='acrisuser', password=self.password,
                                    host='acris-dev.c1e2qqhlvvst.us-east-1.rds.amazonaws.com',
                                    database='acris')

          cursor = cnx.cursor(dictionary=True)

          query = "SELECT * FROM real_property_parties as parties JOIN real_property_legals as lots ON parties.documentid = lots.documentid AND lots.borough = " + borough + " AND lots.block = " + block + " AND lots.lot = " + lot + " JOIN real_property_master as docs ON docs.documentid = parties.documentid AND lots.documentid = docs.documentid AND lots.documentid = parties.documentid WHERE docs.docdate = '" + docdate + "' "

          cursor.execute(query)

          result_set = cursor.fetchall()

          OUTPUT.append(result_set)

        ctr += 1

    cursor.close()
    cnx.close()

    self.write_output(OUTPUT)
      
    return 0


  def write_output(self, output):

    print("Writing output...")
    output = output[0]

    csv_write = "test-output.csv"
    with open(csv_write,'wb') as out:
      csv_out=csv.writer(out)
      csv_out.writerow(output[0].keys())
      for row in output:
          csv_out.writerow(row.values())

    print("Output complete.")




if __name__ == '__main__':

 
  #years = ["2010","2011","2012","2013","2014","2015","2016","2017"]
  password = "9]i%adBAwy8%CFRqnnDfahETiTQ=9Q"
  years = ["2011"]
  p = Query(years, password);






