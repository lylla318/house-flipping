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

    #self.borough = borough
    self.boroughs = boroughs
    self.years = years

    self.flip_profits_by_year = defaultdict(list)
    self.flips_counts_by_year = defaultdict(int)
    self.properties = self.get_properties()
    self.flips_by_zip = self.get_flips_by_zip()

    self.get_profit_trends()

    # self.output_count_csvs()
    # self.output_data_csvs()


  # Organize parcel data into data structure
  # BBL stands for borough-block-lot of a given parcel
  # Data structure format: { "BBL":[sale1, sale2, ...], "BBL2": {...} }
  def get_properties(self):

    print("Reading data... ")
    properties = defaultdict(list)

    for year in self.years:

      print("Reading..."+str(year))

      for borough in self.boroughs:

        csv_read = "raw_data/" + borough + "/" + year + "_" + borough + ".csv"
        reader = csv.reader(open(csv_read, 'rU'), delimiter=",", dialect=csv.excel_tab)
        row_num = 0

        for row in reader:

          if(row_num != 0): 

            for i in range(len(row)):
              row[i] = row[i].strip()

            bbl = row[0] + "-" + row[4] + "-" + row[5]
            properties[bbl].append(row)
          row_num += 1

    return properties



  # Find flips and organize them data into data structure
  # Data structure format: { year1: {zip1: flip_count, zip2: ..., ...}, "year2": {...} }
  # Criteria for a flip: 2 or more sales in 12 months.
  # Critieria to exclude non-arms length transactions:
  # --- days between sales is greater or equal to 5 and less than or equal to 365
  # --- sales price is greater than or equal to $100k
  # --- difference in sales prices is not equal to 0
  def get_flips_by_zip(self):

    print("Getting flip counts by zip code...")
    flips = defaultdict(lambda: defaultdict(int))
    flip_metadata = defaultdict(list)
    
    flip_count = 0
    potential_flips = 0

    for key in self.properties.keys():
      property_sales = self.properties[key]
      # Get properties with > 1 sale
      if(len(property_sales) > 1):

        #Determine which sales qualify as flips, for each year
        i = len(property_sales) - 1
        while(i > 0):
          current_sale  = property_sales[i]
          previous_sale = property_sales[i-1]
          potential_flips += 1;
          sale_amount = self.get_sale_amount(current_sale)
          prev_amount = self.get_sale_amount(previous_sale)
          try:
            time_delta = self.get_time_diff(current_sale, previous_sale);
            criteria1 = time_delta[1] >= 5 and time_delta[1] <= 365 
            criteria2 = sale_amount >= 100000
            criteria3 = sale_amount - prev_amount != 0
            criteria4 = current_sale[3] == "1"
            if(criteria1 and criteria2 and criteria3 and criteria4):
              # print("*******************************************")
              # print(current_sale)
              # print(previous_sale)
              # print("*******************************************")
              profit = sale_amount - prev_amount
              flips[time_delta[0].year][current_sale[10]] += 1
              current_sale = self.format_parcel_data(current_sale)
              self.flip_profits_by_year[time_delta[0].year].append(profit)
              self.flips_counts_by_year[str(time_delta[0].year)] += 1
              flip_metadata[time_delta[0].year].append(current_sale)
          except Exception as e:
            print(e)

          i-=1

    #print(flip_metadata)
    self.flip_metadata = flip_metadata
    print("--------------------------------------")
    print(self.flips_counts_by_year)
      
    return flips


  def get_profit_trends(self):
    #print(self.flip_profits_by_year)
    average_profits = defaultdict(list)
    summary_data = []
    for year in self.flip_profits_by_year.keys():
      summary_data.append([str(year),np.mean(self.flip_profits_by_year[year]), self.flips_counts_by_year[str(year)]])

    csv_write = "profit-trends.csv"
    with open(csv_write,'w') as out:
      csv_out=csv.writer(out)
      for row in summary_data:
        csv_out.writerow(row)
    print(summary_data)


  def format_parcel_data(self, arr):
    
    parcel_data = arr
    current_date  = arr[-1].split("/")
    mm = current_date[0]
    dd = current_date[1]
    yyyy = current_date[2]

    if(len(dd) < 2):
      dd = "0"+dd
    if(len(mm) < 2):
      mm = "0"+mm
    if(len(yyyy) < 4):
      yyyy = "20"+yyyy

    date = yyyy+"-"+mm+"-"+dd
    parcel_data[-1] = date
    return parcel_data


  # Get the sale amount from the raw data by reformatting & converting the string.
  def get_sale_amount(self, sale):

    try:
      raw_amount = (((sale[-2]).replace("$","")).replace(",","")).strip()
      return float(raw_amount)
    except:
      return 0



  # Get the difference between two dates
  # Input is entire raw date from current sale, raw date from previous sale
  # Returns difference and reformatted date of current sale
  def get_time_diff(self, current_sale, previous_sale):
    # print(current_sale)
    # print(previous_sale)
    current_date  = current_sale[-1].split("/")
    previous_date = previous_sale[-1].split("/")
    current_year  = ("20" + current_date[2]) if len(current_date[2]) == 2 else current_date[2]
    previous_year = ("20" + previous_date[2]) if len(previous_date[2]) == 2 else previous_date[2]
    current_date  = datetime.date(int(current_year),int(current_date[0]),int(current_date[1]))
    previous_date = datetime.date(int(previous_year), int(previous_date[0]), int(previous_date[1]))
    
    return [current_date, abs((current_date - previous_date).days)]


  #Writes data from each year into a separate csv file.
  def output_count_csvs(self):

    print("Writing output...")
    for year in self.flips_by_zip.keys():

      output_list = [["zipcode","flip_count"]]
      data = self.flips_by_zip[year]
      for zipcode in data.keys():
        output_list.append([zipcode,data[zipcode]])

      csv_write = "output_data/all-boroughs/flip-counts/flip_counts_" + str(year) + ".csv"
      with open(csv_write,'w') as out:
        csv_out=csv.writer(out)
        for row in output_list:
          csv_out.writerow(row)

    print("Output complete.")


  #Writes data from each year into a separate csv file.
  def output_data_csvs(self):

    print("Writing output...")
    for year in self.flip_metadata.keys():

      output_list = [["borough", "neighborhood", "building_class_category", "tax_class", "block", "lot", "easement", "building_class", "address", "apt_number", "zipcode", "residential_units", "commercial_units", "total_units", "land_sq_ft", "gross_sq_ft", "year_built", "tax_class_at_sale", "building_class_at_sale",  "sale_price", "sale_date"]]
      data = self.flip_metadata[year]
      for row in data:

        output_list.append(row)

      csv_write = "output_data/all-boroughs/flips_" + str(year) + ".csv"
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






