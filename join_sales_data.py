import re
import json
import io
import csv
import sys
from pprint import pprint
from collections import defaultdict
# import mysql.connector
# import MySQLdb
import time
import datetime
import numpy as np




class Parser:

  def __init__(self, boroughs, years):

    self.boroughs = boroughs
    self.years = years
    self.joined_sales_data = self.join_sales_data()
    self.col_headers = ["borough", "neighborhood", "building_class_category", "tax_class", "block", "lot", "easement", "building_class", "address", "apt_number", "zipcode", "residential_units", "commercial_units", "total_units", "land_sq_ft", "gross_sq_ft", "year_built", "tax_class_at_sale", "building_class_at_sale", "sale_price", "sale_date"]

    self.write_to_csv()


  # Join all the sales data the city has on record (2003 - 2017)
  def join_sales_data(self):

    print("Reading data... ")
    joined_sales_data = []

    for year in self.years:

      print("Reading..."+str(year))

      for borough in self.boroughs:

        csv_read = "raw_data/" + borough + "/" + year + "_" + borough + ".csv"
        reader = csv.reader(open(csv_read, 'rU'), delimiter=",", dialect=csv.excel_tab)
        row_num = 0

        for row in reader:

          if(row_num != 0): 
            joined_sales_data.append(row)


          row_num += 1

    return joined_sales_data


  #Writes data from each year into a separate csv file.
  def write_to_csv(self):

    print("Writing output...")
    print(self.col_headers)
    output_list = [self.col_headers]
    data = self.joined_sales_data
    for row in data:
      output_list.append(row)

    csv_write = "output_data/joined_sales.csv"
    with open(csv_write,'w') as out:
      csv_out=csv.writer(out)
      for row in output_list:
        csv_out.writerow(row)

    print("Output complete.")





if __name__ == '__main__':

  #borough = "brooklyn" 
  boroughs = ["manhattan", "brooklyn", "queens", "bronx", "staten_island"]
  years = ["2003","2004","2005","2006","2007","2008","2009","2010","2011","2012","2013","2014","2015","2016","2017"]
  p = Parser(boroughs, years);






