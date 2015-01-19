# -*- coding: utf-8 -*-
#--- requires Python 3

import datetime
import os
import glob
import shutil
import zipfile
import xlrd
import pandas as pd
from urllib.request import urlopen
from urllib.error import HTTPError
from dateutil.parser import parse
from operator import add
from functools import reduce
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from IPython.display import clear_output

#----------------------Auxiliary-----------------------------------------------

def datetime_range(start, finish, dayfirst=True):
    """Both start and finish are date strings
       Example: datetime_range('1/12/2013', '30/9/2014')
    """
    start = parse(start, dayfirst=dayfirst)
    finish = parse(finish, dayfirst=dayfirst)
    
    return [start + datetime.timedelta(days=x) for x in range(0, (finish-start).days+1)]
    

def flatten(destination, depth=''):
    local = os.path.join(destination, depth)
    for file_or_dir in os.listdir(local):
        if os.path.isfile(os.path.join(local, file_or_dir)):
            if depth == '':
                continue
            else:
                shutil.move(os.path.join(local, file_or_dir), destination)
        else:
            flatten(destination, os.path.join(depth, file_or_dir))


def batch_extract(path, exclude=None):
    for file_or_dir in os.listdir(path):
        if file_or_dir == exclude:
            continue
        if zipfile.is_zipfile(os.path.join(path, file_or_dir)):
            fzip = zipfile.ZipFile(os.path.join(path, file_or_dir))
            fzip.extractall(path)
            fzip.close()
   
         
def single_extract(path, filename):
    fzip = zipfile.ZipFile(os.path.join(path, filename))
    fzip.extractall(path)
    fzip.close()


def cleanup(path):
    for file_or_dir in os.listdir(path):
        local = os.path.join(path, file_or_dir)
        if os.path.isdir(local):
            shutil.rmtree(local)
        elif zipfile.is_zipfile(local):
            os.remove(local)
   
    
def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))
    
    
#------------------Download data-----------------------------------------------
        
#------------------------------------------------------------------------------
#  Download unit availability data:
#------------------------------------------------------------------------------	
def fetch_availabilities(period, verbose=True):
    """ Downloads unit availability data from the Hellenic Independent Power 
        Transmission Operator S.A. (ADMIE) 
        
        Inputs
            period: list of datetime objects
            verbose: bool
        Outputs
            list of date objects corresponding to the dates for which data was 
            not found
    """
    path = os.path.join(os.path.dirname("__file__"), "raw/availability")

    if not os.path.exists(path):
        os.makedirs(path)
 
    missing_days = []
       
    #data before 2009 is not available
    if period[0].year < 2009:
        period = [i for i in period if i.year >=2009]
        if len(period) == 0:
            return
    
    while period[0].year < 2012:
        url = ['http://www.admie.gr/fileadmin/user_upload/reports/DayAheadSchedulingUnitAvailabilities/',
                    '_DayAheadSchedulingUnitAvailabilities_01.ZIP']
                    
        f = urlopen(url[0]+datetime.date(period[0].year, 12, 31).strftime('%Y%m%d')+url[1])
        with open(os.path.join(path, 'availabilities.zip'), "wb") as local_file:
            local_file.write(f.read())
        
        single_extract(path, 'availabilities.zip')
        flatten(path)
        batch_extract(path, exclude='availabilities.zip')
        cleanup(path)
        
        period = [i for i in period if i.year > period[0].year]
        if len(period) == 0:
            return
    
    url = ['http://www.admie.gr/fileadmin/user_upload/reports/DayAheadSchedulingUnitAvailabilities/', 
                    '_DayAheadSchedulingUnitAvailabilities_']
    
    file_suffix = 'UnitAvailabilities.xls' 

    for day in period:   
        sday = day.strftime('%Y%m%d')
        
        # there are duplicate files due to different versions of the same file
        for i in range(3, 0, -1):
            try:
                f = urlopen(url[0]+sday+url[1]+('0%d.xls' %i))
                break
            except HTTPError:
                f = None
                continue
                
        if f is None:
            missing_days.append(day)
            continue
       
        with open(os.path.join(path, sday + file_suffix), "wb") as stream:
            stream.write(f.read())
        
        if verbose:
            print('Day ' + day.strftime('%d/%m/%Y') + ' downloaded', end='\r')
            clear_output(wait=True)
    
    print('\n')
    
    return missing_days      
    
    
#------------------------------------------------------------------------------
#  Download Day-ahead Scheduling (DAS) results data:
#------------------------------------------------------------------------------    
def fetch_results(period, verbose=True):
    """ Downloads Day-ahead Scheduling (DAS) results data from the Hellenic Electricity 
        Market Operator S.A. (LAGIE)  
    
        Inputs
            period: list of datetime objects
            verbose: bool
        Outputs
            list of date objects corresponding to the dates for which data was not found
    """
    path = os.path.join(os.path.dirname("__file__"), "raw/results")

    if not os.path.exists(path):
        os.makedirs(path)
 
    missing_days = []       
    
    #data before 2007 is not available
    if period[0].year < 2007:
        period = [i for i in period if i.year >=2007]
        if len(period) == 0:
            return
 
    while period[0].year < 2011:
        url = ['http://www.lagie.gr/fileadmin/user_upload/reports/DayAheadSchedulingResults/',
                    '_DayAheadSchedulingResults_01.ZIP']
                    
        f = urlopen(url[0]+datetime.date(period[0].year, 12, 31).strftime('%Y%m%d')+url[1])        
        with open(os.path.join(path, 'results.zip'), "wb") as local_file:
            local_file.write(f.read())
        
        single_extract(path, 'results.zip')
        flatten(path)
        batch_extract(path, exclude='results.zip')
        cleanup(path)
        
        period = [i for i in period if i.year > period[0].year]
        if len(period) == 0:
            return
            
    url = ['http://www.lagie.gr/fileadmin/user_upload/reports/DayAheadSchedulingResults/', 
                    '_DayAheadSchedulingResults_']
    
    file_suffix = 'results.xls' 

    for day in period:   
        sday = day.strftime('%Y%m%d')
        
        # there are duplicate files due to different versions of the same file
        for i in range(3, 0, -1):
            try:
                f = urlopen(url[0]+sday+url[1]+('0%d.xls' %i))
                break
            except HTTPError:
                f = None
                continue
                    
        if f is None:
            missing_days.append(day)
            continue
       
        with open(os.path.join(path, sday + file_suffix), "wb") as stream:
            stream.write(f.read())
        
        if verbose:
            print('Day ' + day.strftime('%d/%m/%Y') + ' downloaded', end='\r')
            clear_output(wait=True)
    
    print('\n')

    return missing_days
       

#--------------------Update data-----------------------------------------------

def update_availabilities(period, batch_size, verbose=True):
    
    failed_days = []
    reason = []
    
    path = os.path.join(os.path.dirname("__file__"), "auxiliary/plant_fleet.csv")    
    fleet = pd.read_csv(path)
    lignite_units = fleet[fleet['Fuel'] == 'Lignite']['Name']
    ngas_units = fleet[fleet['Fuel'] == 'NG']['Name']
    foil_units = fleet[fleet['Fuel'] == 'FO']['Name']
    hydro_units = fleet[fleet['Fuel'] == 'Hydro']['Name']
    
    path = os.path.join(os.path.dirname("__file__"), "raw/availability")
    
    client = MongoClient()
    db = client['Greek_Power_Market']
        
    for batch in chunker(period, batch_size):
        lignite_list = []
        ngas_list = []
        foil_list = []
        hydro_list = []
        
        for day in batch:
            sday = day.strftime('%Y%m%d')
            xlfile = glob.glob(os.path.join(path, sday) + '*.xls')
            if len(xlfile) == 0:
                failed_days.append(day)
                reason.append('no file found')
                continue 
            else:
                try:
                    book = xlrd.open_workbook(max(xlfile))
                except xlrd.XLRDError:
                    failed_days.append(day)
                    reason.append('file found but could not open')
                    continue
            
            lignite = {'day':day, 'values':{}}
            ngas = {'day':day, 'values':{}}
            foil = {'day':day, 'values':{}}
            hydro = {'day':day, 'values':{}}
            
            try:
                sheet = book.sheet_by_name('Unit_MaxAvail_Publish')
            except xlrd.XLRDError:
                sheet = book.sheet_by_index(0)
                    
            units = sheet.col_values(1, start_rowx=4)
            capacities = sheet.col_values(3, start_rowx=4)
            
            for unit, capacity in zip(units, capacities):
                if unit == 'MOTOROIL':
                    continue
                elif unit in lignite_units.values:
                    lignite['values'][unit] = capacity
                elif unit in ngas_units.values:
                    ngas['values'][unit] = capacity
                elif unit in foil_units.values:
                    foil['values'][unit] = capacity
                elif unit in hydro_units.values:
                    hydro['values'][unit] = capacity
                else:
                    print('%s not recognized' %unit)
            
            for plant, fuel in zip([lignite_list, ngas_list, foil_list, hydro_list], 
                                   [lignite, ngas, foil, hydro]):
                fuel['total'] = reduce(add, list(fuel['values'].values()))        
                plant.append(fuel)
                
            if verbose:
                print('Day ' + day.strftime('%d/%m/%Y') + ' was put in document', end='\r')
                clear_output(wait=True)
            
        for plant, fuel in zip([lignite_list, ngas_list, foil_list, hydro_list], 
             ['availabilities.%s' %fuel for fuel in ['lignite', 'ngas', 'foil', 'hydro']]):  
            collection = db[fuel]
            collection.ensure_index("day", unique=True)
            try:        
                collection.insert(plant, continue_on_error=True)
            except DuplicateKeyError:
                continue
            
        if verbose:
            print('Up to day ' + day.strftime('%d/%m/%Y') + ' was put in database', end='\r')
            clear_output(wait=True)
        
    print('\n')
    return list(zip(failed_days, reason))
        

def update_results(period, batch_size, verbose=True):
    
    failed_days = []
    reason = []
        
    path = os.path.join(os.path.dirname("__file__"), "auxiliary/plant_fleet.csv")    
    fleet = pd.read_csv(path)
    lignite_units = fleet[fleet['Fuel'] == 'Lignite']['Name']
    ngas_units = fleet[fleet['Fuel'] == 'NG']['Name']
    foil_units = fleet[fleet['Fuel'] == 'FO']['Name']

    path = os.path.join(os.path.dirname("__file__"), "raw/results")
    
    client = MongoClient()
    db = client['Greek_Power_Market']
        
    for batch in chunker(period, batch_size):   
        load_list = []
        smp_list = []
        mandatory_waters_list = []
        res_list = []
        hydro_list = []
        border_list = []
        lignite_list = []
        ngas_list = []
        foil_list = []
    
        for day in batch:
            sday = day.strftime('%Y%m%d')
            xlfile = glob.glob(os.path.join(path, sday) + '*.xls')
            if len(xlfile) == 0:
                failed_days.append(day)
                reason.append('no file found')
                continue 
            else:
                try:
                    book = xlrd.open_workbook(max(xlfile))
                except xlrd.XLRDError:
                    failed_days.append(day)
                    reason.append('file found but could not open')
                    continue
            
            smp = {'day':day, 'values':{}}
            load = {'day':day, 'values':{}}
            mandatory_waters = {'day':day, 'values':{}}
            res = {'day':day, 'values':{}}
            hydro = {'day':day, 'values':{}}
            border = {'day':day, 'values':{}}
            lignite = {'day':day, 'values':{}}
            ngas = {'day':day, 'values':{}}
            foil = {'day':day, 'values':{}}
            
            try:
                sheet = book.sheet_by_name('%s_DAS' %sday)
            except xlrd.XLRDError:
                sheet = book.sheet_by_index(0)   
        
            first_col = sheet.col_values(0)
            if sday=='20090101' or sday=='20090102':
                first_row = sheet.row_values(0)
            else:
                first_row = sheet.row_values(1)
            
            last_col_index = [i for i, label in enumerate(first_row) if str(label) in 'TOTALSUM'][0]
            
            for factor, name in zip([load, mandatory_waters, res, hydro, border], 
                    ['LOAD', 'MANDATORY', 'RENEWABLES', 'HYDRO PRODUCTION', 'BORDER']):            
                row_number = [i for i, label in enumerate(first_col) if name in str(label)][0]          
                factor['values'] = dict(zip([str(i) for i in range(1, last_col_index)], 
                     sheet.row_values(row_number, start_colx=1, end_colx=last_col_index)[:]))
                factor['total'] = sheet.cell(row_number, last_col_index).value 
            
            row_number = first_col.index('SMP')
            smp['values'] = dict(zip([str(i) for i in range(1, last_col_index)], 
                 sheet.row_values(row_number, start_colx=1, end_colx=last_col_index)[:]))
            smp['average'] = sheet.cell(row_number, last_col_index).value 
            
            for row_number, maybe_unit in enumerate(first_col):
                if maybe_unit in lignite_units.values:
                    lignite['values'][maybe_unit] = dict(zip([str(i) for i in range(1, last_col_index)], 
                        sheet.row_values(row_number, start_colx=1, end_colx=last_col_index)[:]))
                elif maybe_unit in ngas_units.values:
                    ngas['values'][maybe_unit] = dict(zip([str(i) for i in range(1, last_col_index)], 
                         sheet.row_values(row_number, start_colx=1, end_colx=last_col_index)[:]))
                elif maybe_unit in foil_units.values:
                    foil['values'][maybe_unit] = dict(zip([str(i) for i in range(1, last_col_index)], 
                         sheet.row_values(row_number, start_colx=1, end_colx=last_col_index)[:]))
                elif 'HERON' in str(maybe_unit) and maybe_unit[-1].isdigit():
                    data = sheet.row_values(row_number, start_colx=1, end_colx=last_col_index)
                    print([type(i) for i in data])
                    if not 'HERON' in ngas['values']:
                        ngas['values']['HERON'] = dict(zip([str(i) for i in range(1, last_col_index)], data))
                    else:
                        for j in range(1, last_col_index):
                            try:
                                ngas['values']['HERON'][str(j)] += data[j-1]
                            except TypeError:
                                if isinstance(ngas['values']['HERON'][str(j)], str):
                                    ngas['values']['HERON'][str(j)] = data[j-1]
                        
            load_list.append(load)
            smp_list.append(smp)
            mandatory_waters_list.append(mandatory_waters)
            res_list.append(res)
            hydro_list.append(hydro)
            border_list.append(border)
            lignite_list.append(lignite)
            ngas_list.append(ngas)
            foil_list.append(foil)
            
            if verbose:
                print('Day ' + day.strftime('%d/%m/%Y') + ' was put in document', end='\r')
                clear_output(wait=True)
            
        collection_names = ['results.%s' %factor for factor in ['load', 'smp', 
                        'hydro.mandatory', 'hydro', 'res', 'border', 'lignite', 'ngas', 'foil']]
        
        for name, factor in zip(collection_names, [load_list, smp_list, mandatory_waters_list,
                                hydro_list, res_list, border_list, lignite_list, ngas_list, foil_list]):        
            collection = db[name]
            collection.ensure_index("day", unique=True)
            try:        
                collection.insert(factor, continue_on_error=True)
            except DuplicateKeyError:
                continue
            
        if verbose:
            print('Up to day ' + day.strftime('%d/%m/%Y') + ' was put in database', end='\r')
            clear_output(wait=True)
        
    print('\n')
    return list(zip(failed_days, reason))
                    
    