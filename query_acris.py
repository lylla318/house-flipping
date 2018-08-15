import re
import json
import io
import csv
import sys
import operator
from pprint import pprint
from collections import defaultdict
import mysql.connector
#import MySQLdb
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

    print("Testing query function...")

    cnx = mysql.connector.connect(user='acrisuser', password=self.password,
                                  host='acris-dev.c1e2qqhlvvst.us-east-1.rds.amazonaws.com',
                                  database='acris')

    cursor = cnx.cursor(dictionary=True)

    query = "SELECT * FROM real_property_parties as parties JOIN real_property_legals as lots ON parties.documentid = lots.documentid AND lots.borough = 3 AND lots.block = 2317 AND lots.lot = 1 JOIN real_property_master as docs ON docs.documentid = parties.documentid AND lots.documentid = docs.documentid WHERE docs.docdate = '2010-11-3' "

    cursor.execute(query)

    result_set = cursor.fetchall()

    print(result_set)  


  # Organize parcel data into data structure
  # BBL stands for borough-block-lot of a given parcel
  # Data structure format: { "BBL":[sale1, sale2, ...], "BBL2": {...} }
  def query_acris(self):

    print("Reading data... ")
    OUTPUT = []
    ctr = 0

    party_type1 = defaultdict(lambda: defaultdict(int))
    party_type2 = defaultdict(lambda: defaultdict(int))

    for year in self.years:

      print("Reading..."+str(year))

      csv_read = "output_data/all-boroughs/flips_" + year + ".csv"
      reader = csv.reader(open(csv_read, 'rU'), delimiter=",", dialect=csv.excel_tab)
      ctr = 0

      for row in reader:

        #if(ctr > 0 and ctr < 12):

          
     
        borough = row[0]
        block   = row[4]
        lot     = row[5]
        docdate = row[-1]

        

        cnx = mysql.connector.connect(user='acrisuser', password=self.password,
                                  host='acris-dev.c1e2qqhlvvst.us-east-1.rds.amazonaws.com',
                                  database='acris')

        cursor = cnx.cursor(dictionary=True)

        query = "SELECT * FROM real_property_parties as parties JOIN real_property_legals as lots ON parties.documentid = lots.documentid AND lots.borough = " + borough + " AND lots.block = " + block + " AND lots.lot = " + lot + " JOIN real_property_master as docs ON docs.documentid = parties.documentid AND lots.documentid = docs.documentid AND lots.documentid = parties.documentid WHERE docs.docdate = '" + docdate + "' "

        cursor.execute(query)

        result_set = cursor.fetchall()

        # print("*****************************************************")
        # print("Processing flip number " + str(ctr))
        # print("DOCDATE: " + str(docdate))
        pt1 = []
        pt2 = []
        for result in result_set:
          # print(result["documentid"] + "     " + result["doctype"] + "     " + str(result["partytype"]) + "     " + result["name"])
          if(result["doctype"] == "DEED"):
            if(result["partytype"] == 1):
              if(result["name"] not in pt1):
                party_type1[docdate[0:4]][result["name"]] += 1
                pt1.append(result["name"])
            elif (result["partytype"] == 2):
              if(result["name"] not in pt2):
                party_type2[docdate[0:4]][result["name"]] += 1
                pt2.append(result["name"])

        #print(party_type1)

        OUTPUT.append(result_set)

        ctr += 1

    print("*************************************************")
    print("PARTY TYPE 1:")
    print("*************************************************")

    for year in party_type1.keys():
      x = party_type1[year] 
      x_sort = sorted(x.items(), key=operator.itemgetter(1)) 
      print(x_sort[len(x_sort)-15:])

    print("*************************************************")
    print("PARTY TYPE 2:")
    print("*************************************************")

    for year in party_type2.keys():
      x = party_type2[year] 
      x_sort = sorted(x.items(), key=operator.itemgetter(1)) 
      print(x_sort[len(x_sort)-15:])

    cursor.close()
    cnx.close()

    #self.write_output(OUTPUT)
      
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
  years = ["2017"]
  p = Query(years, password);

*************************************************
PARTY TYPE 1:
*************************************************
[
('MESSI EQUITIES LLC', 6), 
('FANNIE MAE', 6), 
('BALI DEVELOPERS INC', 7), 
('DEUTSCHE BANK NATIONAL TRUST COMPANY', 7), 
('NACO HOME BUYERS INC', 7), 
('P.B. 100 INC.', 8), 
('1908 HUNT AVENUE LLC', 8), 
('BARCA DEVELOPMENT LLC', 8), 
('ZMIROT LLC', 10), 
('ELITE PROPERTIES NYC 101 LLC', 10), 
('ETROG LLC', 10), 
('SIGNATURE ROSE LLC', 12), 
('UNITED FRANKLIN LLC', 17), 
('PEREK 92 LLC', 18), 
('NEHEMIAH HOUSING DEVELOPMENT FUND COMPANY, INC.', 50)]
*************************************************
PARTY TYPE 2:
*************************************************
[('HR BOLLER AVE CORP', 2), 
('NISSAN GROUP LLC', 2), 
('SADYKOV, IGOR', 2), 
('ZELMANOVITCH, LILIA', 2), 
('B & S HOLDINGS GROUP LLC', 2), 
('KIM, JULIE', 2), 
('ALLISON, CHRISTOPHER', 3), 
('NEYMAR EQUITIES LLC', 3), 
('ETROG LLC', 3), 
('PLAZA GARDENS REAL ESTATE CORP', 3), 
('ZMIROT LLC', 3), 
('COMMUNITY OPTIONS NEW YORK, INC.', 3), 
('TIGER GATE LLC', 3), 
('RRP REALTY GROUP LLC', 3), 
('ELITE PROPERTIES NYC PARTNERS LLC', 4)]






