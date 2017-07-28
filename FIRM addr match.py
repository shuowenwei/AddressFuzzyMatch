# -*- coding: utf-8 -*-
"""
Created on Wed Jul 12 14:45:07 2017

@author: k26609
"""

import reference
from string import punctuation
import pandas as pd
import time
import numpy as np
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

upsstore = pd.read_csv(r"C:\Users\k26609\Desktop\FIRM address match POC\upsstore_05-23-17.csv", dtype={"Store Num": str, "Zip Code": str, "County": str})
regus = pd.read_csv(r"C:\Users\k26609\Desktop\FIRM address match POC\regus_12-02-16.csv", dtype={"Store Number": str, "Zip Code": str, "County": str})
annex_brands = pd.read_csv(r"C:\Users\k26609\Desktop\FIRM address match POC\annex_brands_03-18-17.csv", dtype={"Store Number": str, "Zip Code": str, "County": str})

#encoding='latin1', encoding='iso-8859-1' or encoding='cp1252'
firm = pd.read_csv(r"C:\Users\k26609\Desktop\FIRM address match POC\firm.csv",encoding='iso-8859-1',  dtype={"FIRM_CRD_NB": str, "BD_MAIN_PSTL_CODE_TX": str, "BD_MAIL_PSTL_CODE_TX": str})


#na_values = '', keep_default_na = False,

#upsstore.replace(np.nan, 'Missing', regex=True)
#regus.replace(np.nan, 'Missing', regex=True)
#annex_brands.replace(np.nan, 'Missing', regex=True)
#firm.replace(np.nan, 'Missing', regex=True)
#upsstore.fillna('Missing')
#regus.fillna('Missing')
#annex_brands.fillna('Missing')
#firm.fillna('Missing')

# data validate
"""
dup1 = set(upsstore['Store Num']).intersection(set(regus['Store Number']))
dup2 = set(upsstore['Store Num']).intersection(set(annex_brands['Store Number']))
dup3 = set(annex_brands['Store Number']).intersection(set(regus['Store Number']))
if dup1 or dup2 or dup3:
    print('Duplicate Store Numbers exist')
else:
    print('Store Numbers are unique')


for a in upsstore['Zip Code'].append(regus['Zip Code']).append(annex_brands['Zip Code']).append(firm['BD_MAIN_PSTL_CODE_TX']).append(firm['BD_MAIL_PSTL_CODE_TX']):
    if len(str(a)) < 5 or a is None:
        if not math.isnan(a):
            print(a)
#    else:
#        print('good zip')

for a in upsstore['State'].append(regus['State']).append(annex_brands['State']).append(firm['BD_MAIN_STATE_CD']).append(firm['BD_MAIL_STATE_CD']):
    if len(str(a)) != 2 and not math.isnan(a):
        print(a)
#    else:
#        print('good state code')

for a in upsstore['Country Code'].append(regus['Country Code']).append(annex_brands['Country Code']).append(firm['BD_MAIN_CNTRY_CD']).append(firm['BD_MAIL_CNTRY_CD']):
    if len(str(a)) != 2 and a !='USA':
        if not math.isnan(a):
            print(a)
#    else:
#        print('good Country Code')
all_country_code = set(list(upsstore['Country Code'].append(regus['Country Code']).append(annex_brands['Country Code']).append(firm['BD_MAIN_CNTRY_CD']).append(firm['BD_MAIL_CNTRY_CD'])))
print(all_country_code)
# all_country_code = {nan, 'PR', 'US', 'CA', 'USA'}

for a in upsstore['County'].append(regus['County']).append(annex_brands['County']):
    if a is None or len(str(a)) < 1:# or type(a)==float:
        print(a)
#    else:
#        print('good County')
"""

# data pre-processing and combining 
#df=df.rename(columns = {'old_name':'new_name'})
# for upsstore:
upsstore['StoreNumber'] = 'ups_' + upsstore['Store Num']
upsstore = upsstore.rename(columns = {'Store Num':'Original Store Number'})
upsstore['Address_upper'] = upsstore['Address'].apply(lambda x: str(x).upper().strip()) 
upsstore['Zip'] = upsstore['Zip Code'].apply(lambda x: x[:5].strip()) 
upsstore['Country Code'] = upsstore['Country Code'].apply(lambda x: str(x).upper().strip() if not pd.isnull(x) else 'Missing') 
upsstore['State'] = upsstore['State'].apply(lambda x: str(x).upper().strip() if not pd.isnull(x) else 'Missing') 


# for regus:
regus['StoreNumber'] = 'regus_' + regus['Store Number']
regus = regus.rename(columns = {'Store Number':'Original Store Number'})
regus['Address_upper'] = regus['Address'].apply(lambda x: str(x).upper().strip()) + ' ' + regus['Address Line 2'].apply(lambda x: str(x).upper().strip() if not pd.isnull(x) else '') 
regus['Zip'] = regus['Zip Code'].apply(lambda x: x[:5].strip()) 
regus['Country Code'] = regus['Country Code'].apply(lambda x: str(x).upper().strip() if not pd.isnull(x) else 'Missing') 
regus['State'] = regus['State'].apply(lambda x: str(x).upper().strip() if not pd.isnull(x) else 'Missing') 


# for annex_brands:
annex_brands['StoreNumber'] = 'annex_' + annex_brands['Store Number']
annex_brands = annex_brands.rename(columns = {'Store Number':'Original Store Number'})
annex_brands['Address_upper'] = annex_brands['Address'].apply(lambda x: str(x).upper())
annex_brands['Zip'] = annex_brands['Zip Code'].apply(lambda x: x[:5].strip()) 
annex_brands['Country Code'] = annex_brands['Country Code'].apply(lambda x: str(x).upper().strip() if not pd.isnull(x) else 'Missing')  
annex_brands['State'] = annex_brands['State'].apply(lambda x: str(x).upper().strip() if not pd.isnull(x) else 'Missing') 


# for firm:
firm['Zip_main'] = firm['BD_MAIN_PSTL_CODE_TX'].apply(lambda x: x[:5].strip() if not pd.isnull(x) else 'Missing')
firm['Zip_mail'] = firm['BD_MAIL_PSTL_CODE_TX'].apply(lambda x: x[:5].strip() if not pd.isnull(x) else 'Missing')
firm['CNTRY_CD_main'] = firm['BD_MAIN_CNTRY_CD'].apply(lambda x: x[:2].strip() if not pd.isnull(x) else 'Missing') 
firm['CNTRY_CD_mail'] = firm['BD_MAIL_CNTRY_CD'].apply(lambda x: x[:2].strip() if not pd.isnull(x) else 'Missing') 


def strip_punctuation_country(s):
    return ''.join(c for c in s if c not in punctuation)

for index, row in firm.iterrows():
    if row['CNTRY_CD_main'] == 'Missing' and not pd.isnull(row['BD_MAIN_CNTRY_NM']):
        country_names = strip_punctuation_country(row['BD_MAIN_CNTRY_NM']).split()
        if len(country_names) > 0:
            for cn in country_names:
                if cn in reference.addr_map_country:
                    firm.loc[index, 'CNTRY_CD_main'] = reference.addr_map_country.get(cn)
        
    if row['CNTRY_CD_mail'] == 'Missing' and not pd.isnull(row['BD_MAIL_CNTRY_NM']):
        country_names = strip_punctuation_country(row['BD_MAIL_CNTRY_NM']).split()
        if len(country_names) > 0:
            for cn in country_names:
                if cn in reference.addr_map_country:
                    firm.loc[index, 'CNTRY_CD_mail'] = reference.addr_map_country.get(cn)
                    
firm['Address_upper_main'] = firm['BD_MAIN_STRT_1_NM'].apply(lambda x: str(x).upper().strip() if not pd.isnull(x) else '') + ' ' + firm['BD_MAIN_STRT_2_NM'].apply(lambda x: str(x).upper().strip() if not pd.isnull(x) else '') 
firm['Address_upper_mail'] = firm['BD_MAIL_STRT_1_NM'].apply(lambda x: str(x).upper().strip() if not pd.isnull(x) else '') + ' ' + firm['BD_MAIL_STRT_2_NM'].apply(lambda x: str(x).upper().strip() if not pd.isnull(x) else '') 
firm['STATE_CD_main'] = firm['BD_MAIN_STATE_CD'].apply(lambda x: x[:2].upper().strip() if not pd.isnull(x) else 'Missing') 
firm['STATE_CD_mail'] = firm['BD_MAIL_STATE_CD'].apply(lambda x: x[:2].upper().strip() if not pd.isnull(x) else 'Missing') 


#set(firm['BD_MAIN_CNTRY_NM'])
#set(firm['CNTRY_CD_main'])
#set(firm['CNTRY_CD_mail'])
#print(upsstore.columns)
#print(regus.columns)
#print(annex_brands.columns)
#print(firm.columns)

#def hasDuplicatedIndex(df):
#    duplicated_index = {}
#    for index, rows in df.iterrows():
#        if index not in duplicated_index:
#            duplicated_index[index] = 1
#        else:
#            duplicated_index[index] += 1
#    for k,v in duplicated_index.items():
#        if v > 1:
#            print('This dataframe contains duplicated index: ')
##            print(k, v)
#            return
#    print('No duplicated index, we are good!')
#    return

combine = pd.concat([upsstore[['StoreNumber', 'Original Store Number', 'Address_upper', 'City', 'State', 'Zip', 'Country Code']]
                    ,regus[['StoreNumber', 'Original Store Number', 'Address_upper', 'City', 'State', 'Zip', 'Country Code']]
                    ,annex_brands[['StoreNumber', 'Original Store Number', 'Address_upper', 'City', 'State', 'Zip', 'Country Code']]
                    ], ignore_index=True)

#len(firm['FIRM_CRD_NB'])  == len(set(firm['FIRM_CRD_NB']))   True: 19481
firm_main = firm[['FIRM_CRD_NB', 'BD_MAIN_STRT_1_NM', 'BD_MAIN_STRT_2_NM', 'BD_MAIN_CITY_NM', 'STATE_CD_main',
                  'BD_MAIN_CNTRY_NM', 'Zip_main', 'CNTRY_CD_main', 'Address_upper_main']].copy(deep=True)
firm_mail = firm[['FIRM_CRD_NB', 'BD_MAIL_STRT_1_NM', 'BD_MAIL_STRT_2_NM', 'BD_MAIL_CITY_NM', 'STATE_CD_mail', 
                  'BD_MAIL_CNTRY_NM', 'Zip_mail', 'CNTRY_CD_mail', 'Address_upper_mail']].copy(deep=True)

#hasDuplicatedIndex(combine)
#hasDuplicatedIndex(firm_main)
#hasDuplicatedIndex(firm_mail)
combine.index.is_unique
firm_main.index.is_unique
firm_mail.index.is_unique


# logic filters for country, state and zip:   
#for a in combine['Country Code'].append(firm_main['CNTRY_CD_main']).append(firm_mail['CNTRY_CD_mail']):
#    if len(str(a)) != 2 and a != 'Missing':
#        print(a)
def isSameCountry(c1, c2):
    if c1 == 'Missing' or c2 == 'Missing' or c1 == c2: #c1.upper().strip() == c2.upper().strip():
        return True
    else:
        return False
        
#for a in combine['State'].append(firm_main['STATE_CD_main']).append(firm_mail['STATE_CD_mail']):
#    if len(str(a)) != 2 and a != 'Missing':
#        print(a)
def compareState(st1, st2):
    if st2 == 'Missing' or st1 == 'Missing':
        return 'Missing State CD'
    elif st1 == st2: #st1.upper().strip() == st2.upper().strip():
        return 'Matched'
    else:
        return 'Unmatched'
    
def isNumber(num):
    try:
        float(num)
        return True
    except ValueError:
        return False

#for a in combine['Zip'].append(firm_main['Zip_main']).append(firm_mail['Zip_mail']):
#    if len(str(a)) != 5 or a is None:
#            print(a)
def compareZip(zip1, zip2):
    if zip2 == 'Missing' or zip1 == 'Missing' or not isNumber(zip1) or not isNumber(zip2) or len(zip1) != 5 or len(zip2) != 5:
        return 'Missing Zip Code or Bad Zip Code'
    elif abs(float(zip1) - float(zip2)) < 3.0:
        return 'Matched'
    else:
        return 'Unmatched'

def compareZip_score(zip1, zip2):
    if zip2 == 'Missing' or zip1 == 'Missing' or not isNumber(zip1) or not isNumber(zip2) or len(zip1) != 5 or len(zip2) != 5:
        return -5
    elif abs(float(zip1) - float(zip2)) < 3.0:
        return 25
    else:
        return -50
            
#import reference:
#reference.addr_map_direction
#reference.addr_map_street
#reference.addr_map_unit
#reference.addr_map_country
addr_map_street_exist = {}
addr_map_unit_exist = {}
for addr in combine['Address_upper'].append(firm['Address_upper_main']).append(firm['Address_upper_mail']):
    for st_key, st_val in reference.addr_map_street.items():
        if st_key in addr and st_key not in addr_map_street_exist:
            addr_map_street_exist[st_key] = st_val
            
    for unit_key, unit_val in reference.addr_map_unit.items():
        if unit_key in addr and unit_key not in addr_map_unit_exist:
            addr_map_unit_exist[unit_key] = unit_val

#address standardize functions:
#from string import punctuation
#print(punctuation)
#   !"#$%&'()*+,-./:;<=>?@[\]^_`{|}~ 
#   len(punctuation) = 32
punctuation_exist = set()
for a in combine['Address_upper'].append(firm['Address_upper_main']).append(firm['Address_upper_mail']):
    for pun in a:
        if pun in punctuation and pun not in punctuation_exist:
            punctuation_exist.add(pun)
punctuation_exist_dict = {} 
for pun in punctuation_exist:
    punctuation_exist_dict[pun] = 0
#print(punctuation_exist)
#  ,@:?.";)(#*~-/\'&!`=
#  len(punctuation_exist)

def strip_punctuation(s):
    return ''.join(c for c in s if c not in punctuation_exist_dict)

def std_address(raw_addr):
    if pd.isnull(raw_addr) or raw_addr is None:
        return ''
    list_raw_addr = raw_addr.split()
    std_addr = []
    for lra in list_raw_addr:
        if lra in reference.addr_map_direction:
            std_addr.append(reference.addr_map_direction.get(lra))
            
        lra_no_pun = strip_punctuation(lra)
        if not lra_no_pun:
            continue
        elif lra_no_pun in addr_map_street_exist:
           std_addr.append(addr_map_street_exist.get(lra_no_pun))        
        elif lra_no_pun in addr_map_unit_exist:
           std_addr.append(addr_map_unit_exist.get(lra_no_pun))
        else:
           std_addr.append(lra_no_pun)
#    return ' '.join(x for x in std_addr if x)
    return ' '.join(std_addr)

#raw_addr = '# 624 SOUTH GRAND AVE., STE 2600 624 SO. GRAND AVE. SUITE 2510'
#std_address(raw_addr)

combine['Address_upper_std'] = combine['Address_upper'].apply(lambda x: std_address(x))
firm_main['Address_upper_main_std'] = firm_main['Address_upper_main'].apply(lambda x: std_address(x))
firm_mail['Address_upper_mail_std'] = firm_mail['Address_upper_mail'].apply(lambda x: std_address(x))

###############################################################################
start_time = time.time()
###############################################################################

#Explore firm_main state and country      
#a = firm_main.groupby(['STATE_CD_main']).count()
#b = firm_main.groupby(['CNTRY_CD_main']).count()
#for index_m, fmn_add in firm_main.iterrows():
#    state_fmn = fmn_add['STATE_CD_main']
#    country_cd_fmn = fmn_add['CNTRY_CD_main']
#if pd.isnull(state_fmn):
#    print(state_fmn)
#if pd.isnull(country_cd_fmn):
#    print(state_fmn)
#No missing data in combine
#combine.groupby(['State']).count()
#combine.groupby(['Country Code']).count()

"""
firm_main['StoreNumber'] = ''
firm_main['confidence_score_ttl'] = 0
firm_main['rt_scr'] = 0
firm_main['prt_rt_scr'] = 0
#firm_main['prt_set_rt_scr'] = 0
#firm_main['prt_sort_rt_scr'] = 0
processed_num = 0
for index_m, fmn_add in firm_main.iterrows():
    processed_num += 1
    print('---------- Processed: ', processed_num, 'out of 19481 records, index_m: ' , index_m, ' ----------')
    max_score_ttl = 0
    max_rt_scr = 0
    max_prt_rt_scr = 0
#    max_prt_set_rt_scr = 0
#    max_prt_sort_rt_scr = 0
    most_similiar_vitual_addr = ''
    state_fmn = fmn_add['STATE_CD_main']
    zip_fmn = fmn_add['Zip_main']
    country_cd_fmn = fmn_add['CNTRY_CD_main']
    strt_fmn = fmn_add['Address_upper_main_std'] 
#    state_fmn = 'CA'
#    country_cd_fmn = 'US' 
    virtual_addr_pool = None 
    if state_fmn != 'Missing' and country_cd_fmn != 'Missing':
        virtual_addr_pool = combine[(combine['State'] == state_fmn) & (combine['Country Code'] == country_cd_fmn)].copy(deep=True)
    elif state_fmn != 'Missing':
        virtual_addr_pool = combine[combine['State'] == state_fmn].copy(deep=True)
    elif country_cd_fmn != 'Missing':
        virtual_addr_pool = combine[combine['Country Code'] == country_cd_fmn].copy(deep=True)
    else:
        virtual_addr_pool = combine.copy(deep=True)
        
    if virtual_addr_pool.empty:
        continue 
    virtual_addr_pool['zip_score'] = virtual_addr_pool['Zip'].apply(lambda x: compareZip_score(x, zip_fmn))
    virtual_addr_pool['rt_scr'] = virtual_addr_pool['Address_upper_std'].apply(lambda x: fuzz.ratio(x, strt_fmn)) 
    virtual_addr_pool['prt_rt_scr'] = virtual_addr_pool['Address_upper_std'].apply(lambda x: fuzz.partial_ratio(x, strt_fmn))  
    virtual_addr_pool['cond_scr'] = virtual_addr_pool['rt_scr'] + virtual_addr_pool['prt_rt_scr'] + virtual_addr_pool['zip_score']
    
#    for index_c, virtual_add in virtual_addr_pool.iterrows():
#        cond_scr = 0
#        rt_scr = 0
#        prt_rt_scr = 0 
##        prt_set_rt_scr = 0
##        prt_sort_rt_scr = 0
#
#        # step 1: compare zip code
#        zip_vta = virtual_add['Zip']
#        res_zip = compareZip(zip_vta, zip_fmn)
#        if res_zip == 'Unmatched':
#            cond_scr = cond_scr - 50
##            continue
#        elif res_zip == 'Matched':
#            cond_scr = cond_scr + 25
#        else:
#            cond_scr = cond_scr - 5
#                    
#        # step 2: compare address
#        strt_vta = virtual_add['Address_upper_std']
#        rt_scr = fuzz.ratio(strt_vta, strt_fmn)
#        prt_rt_scr = fuzz.partial_ratio(strt_vta, strt_fmn)
##        prt_set_rt_scr = fuzz.partial_token_set_ratio(strt_vta, strt_fmn)
##        prt_sort_rt_scr = fuzz.partial_token_sort_ratio(strt_vta, strt_fmn)
#        cond_scr = cond_scr + rt_scr + prt_rt_scr
#        
#        # finally:
#        if cond_scr > max_score_ttl:
#            print('---------- Processed: ', processed_num, 'out of 19481 records, index_m: ' , index_m, ', index_c: ', index_c, ' ----------')
#            print('-- cond_scr', cond_scr)
#            print('-- current max score', max_score_ttl)
#            max_score_ttl = cond_scr
#            most_similiar_vitual_addr = virtual_add['StoreNumber']
#            max_rt_scr = rt_scr
#            max_prt_rt_scr = prt_rt_scr
##            max_prt_set_rt_scr = prt_set_rt_scr
##            max_prt_sort_rt_scr = prt_sort_rt_scr

#x = df.max(axis=0)['a']
#x = df.nlargest(3, 'a')['a'].item()
    firm_main.loc[index_m, 'StoreNumber'] = virtual_addr_pool.nlargest(1, 'cond_scr')['StoreNumber'].item()
    firm_main.loc[index_m, 'confidence_score_ttl'] = virtual_addr_pool.nlargest(1, 'cond_scr')['cond_scr'].item()
    firm_main.loc[index_m, 'rt_scr'] = virtual_addr_pool.nlargest(1, 'cond_scr')['rt_scr'].item()
    firm_main.loc[index_m, 'prt_rt_scr'] = virtual_addr_pool.nlargest(1, 'cond_scr')['prt_rt_scr'].item()
#    firm_main.loc[index_m, 'StoreNumber'] = most_similiar_vitual_addr
#    firm_main.loc[index_m, 'confidence_score_ttl'] = max_score_ttl
#    firm_main.loc[index_m, 'rt_scr'] = max_rt_scr
#    firm_main.loc[index_m, 'prt_rt_scr'] = max_prt_rt_scr
##    firm_main.loc[index_m, 'prt_set_rt_scr'] = max_prt_set_rt_scr
##    firm_main.loc[index_m, 'prt_sort_rt_scr'] = max_prt_sort_rt_scr

# left join the FIRM table:
result_main = pd.merge(firm_main, combine, how='left', on = ['StoreNumber'])
output_file_name = 'FIRM_main_20170717_2.csv'
result_main.to_csv(output_file_name, index = False) 
"""

top_3_score = []
processed_num = 0
for index_m, fmn_add in firm_main.iterrows():
    processed_num += 1
    print('---------- Processed: ', processed_num, 'out of 19481 records, index_m: ' , index_m, ' ----------')

    state_fmn = fmn_add['STATE_CD_main']
    zip_fmn = fmn_add['Zip_main']
    country_cd_fmn = fmn_add['CNTRY_CD_main']
    strt_fmn = fmn_add['Address_upper_main_std'] 
    virtual_addr_pool = None 
    if state_fmn != 'Missing' and country_cd_fmn != 'Missing':
        virtual_addr_pool = combine[(combine['State'] == state_fmn) & (combine['Country Code'] == country_cd_fmn)].copy(deep=True)
    elif state_fmn != 'Missing':
        virtual_addr_pool = combine[combine['State'] == state_fmn].copy(deep=True)
    elif country_cd_fmn != 'Missing':
        virtual_addr_pool = combine[combine['Country Code'] == country_cd_fmn].copy(deep=True)
    else:
        virtual_addr_pool = combine.copy(deep=True)
    
    if virtual_addr_pool.empty:
        continue
    virtual_addr_pool['zip_score'] = virtual_addr_pool['Zip'].apply(lambda x: compareZip_score(x, zip_fmn))
    virtual_addr_pool['rt_scr'] = virtual_addr_pool['Address_upper_std'].apply(lambda x: fuzz.ratio(x, strt_fmn)) 
    virtual_addr_pool['prt_rt_scr'] = virtual_addr_pool['Address_upper_std'].apply(lambda x: fuzz.partial_ratio(x, strt_fmn))  
    virtual_addr_pool['cond_scr'] = virtual_addr_pool['rt_scr'] + virtual_addr_pool['prt_rt_scr'] + virtual_addr_pool['zip_score']

    virtual_addr_pool['FIRM_CRD_NB'] = fmn_add['FIRM_CRD_NB']
    top_3_score.append(virtual_addr_pool.nlargest(3, 'cond_scr').copy(deep=True))

top_3_score_pd  = pd.concat(top_3_score, ignore_index=True)
result_main_top_3 = pd.merge(firm_main, top_3_score_pd, how='left', on = ['FIRM_CRD_NB'])
output_file_name = 'FIRM_main_20170719_1.csv'
result_main_top_3.to_csv(output_file_name, index = False) 


"""
combine['matching_firm'] = ''
combine['confidence_score_ttl'] = 0
combine['rt_scr'] = 0
combine['prt_rt_scr'] = 0
combine['prt_set_rt_scr'] = 0
combine['prt_sort_rt_scr'] = 0     
processed_num = 0
for index_c, virtual_add in combine.iterrows():
    processed_num += 1
    max_score_ttl = 0 
    max_rt_scr = 0
    max_prt_rt_scr = 0 
    max_prt_set_rt_scr = 0
    max_prt_sort_rt_scr = 0
    most_similiar_firm_crd = '' 
    country_cd_vta = virtual_add['Country Code']
    state_vta = virtual_add['State']
    zip_vta = virtual_add['Zip']
    strt_vta = virtual_add['Address_upper_std']
    
    for index_m, fmn_add in firm_main.iterrows():
        cond_scr = 0
        rt_scr = 0
        prt_rt_scr = 0 
        prt_set_rt_scr = 0
        prt_sort_rt_scr = 0
        # step 1: compare state code
        state_fmn = fmn_add['STATE_CD_main']
        res_state = compareState(state_vta, state_fmn)
        skip_flag = 0 
        if res_state == 'Unmatched':
            continue
#        elif res_state == 'Matched':
#            cond_scr = cond_scr + 10
#        else:
#            cond_scr = cond_scr - 5
        
        # step 2: compare zip code
        zip_fmn = fmn_add['Zip_main']
        res_zip = compareZip(zip_vta, zip_fmn)
        if res_zip == 'Unmatched':
            cond_scr = cond_scr - 50
#            continue
        elif res_zip == 'Matched':
            cond_scr = cond_scr + 25
        else:
            cond_scr = cond_scr - 5
            
        # step 3: compare country code
        country_cd_fmn = fmn_add['CNTRY_CD_main']
        if not isSameCountry(country_cd_vta, country_cd_fmn):
            continue
        
        # step 4: compare address
        strt_fmn = fmn_add['Address_upper_main_std']
        rt_scr = fuzz.ratio(strt_vta, strt_fmn)
        prt_rt_scr = fuzz.partial_ratio(strt_vta, strt_fmn)
        prt_set_rt_scr = fuzz.partial_token_set_ratio(strt_vta, strt_fmn)
        prt_sort_rt_scr = fuzz.partial_token_sort_ratio(strt_vta, strt_fmn)
        cond_scr = cond_scr + rt_scr + prt_rt_scr + prt_set_rt_scr + prt_sort_rt_scr

        # finally:
        if cond_scr > max_score_ttl:
            print('---------- Processed: ', processed_num, 'out of 6262 records, index_c' , index_c, ', index_m: ', index_m, ' ----------')
            print('-- cond_scr', cond_scr)
            print('-- current max score', max_score_ttl)
            max_score_ttl = cond_scr
            most_similiar_firm_crd = fmn_add['FIRM_CRD_NB']
            max_rt_scr = rt_scr
            max_prt_rt_scr = prt_rt_scr
            max_prt_set_rt_scr = prt_set_rt_scr
            max_prt_sort_rt_scr = prt_sort_rt_scr
            
    combine.loc[index_c, 'matching_firm'] = most_similiar_firm_crd
    combine.loc[index_c, 'confidence_score_ttl'] = max_score_ttl
    combine.loc[index_c, 'rt_scr'] = max_rt_scr
    combine.loc[index_c, 'prt_rt_scr'] = max_prt_rt_scr
    combine.loc[index_c, 'prt_set_rt_scr'] = max_prt_set_rt_scr
    combine.loc[index_c, 'prt_sort_rt_scr'] = max_prt_sort_rt_scr

# left join the FIRM table: 
combine = combine.rename(columns = {'matching_firm': 'FIRM_CRD_NB'})
result_main = pd.merge(combine, firm_main, how = 'left', on = ['FIRM_CRD_NB'])
output_file_name = 'FIRM_main_20170714_4.cvs'
result_main.to_csv(output_file_name, index = False)
"""

###############################################################################
end_time = time.time()
time_used = end_time/60 - start_time/60
print("--- %s minutes ---" % time_used)
log_file_name = 'time_used_4_' + output_file_name + '.txt'
with open(log_file_name,'a') as time_log:
#    time_log.write('Time used in minutes: ' + str(time_used_4_addr_std) + '\n')    
    time_log.write('Time used in minutes: ' + str(time_used))
###############################################################################



# Firm mail address 
###############################################################################
start_time = time.time()
top_3_score = []
processed_num = 0
for index_m, fml_add in firm_mail.iterrows():
    processed_num += 1
    print('---------- Processed: ', processed_num, 'out of 19481 records, index_m: ' , index_m, ' ----------')

    state_fml = fml_add['STATE_CD_mail']
    zip_fml = fml_add['Zip_mail']
    country_cd_fml = fml_add['CNTRY_CD_mail']
    strt_fml = fml_add['Address_upper_mail_std'] 
    virtual_addr_pool = None 
    if state_fml != 'Missing' and country_cd_fml != 'Missing':
        virtual_addr_pool = combine[(combine['State'] == state_fml) & (combine['Country Code'] == country_cd_fml)].copy(deep=True)
    elif state_fml != 'Missing':
        virtual_addr_pool = combine[combine['State'] == state_fml].copy(deep=True)
    elif country_cd_fml != 'Missing':
        virtual_addr_pool = combine[combine['Country Code'] == country_cd_fml].copy(deep=True)
    else:
        virtual_addr_pool = combine.copy(deep=True)
    
    if virtual_addr_pool.empty:
        continue
    virtual_addr_pool['zip_score'] = virtual_addr_pool['Zip'].apply(lambda x: compareZip_score(x, zip_fmn))
    virtual_addr_pool['rt_scr'] = virtual_addr_pool['Address_upper_std'].apply(lambda x: fuzz.ratio(x, strt_fmn)) 
    virtual_addr_pool['prt_rt_scr'] = virtual_addr_pool['Address_upper_std'].apply(lambda x: fuzz.partial_ratio(x, strt_fmn))  
    virtual_addr_pool['cond_scr'] = virtual_addr_pool['rt_scr'] + virtual_addr_pool['prt_rt_scr'] + virtual_addr_pool['zip_score']

    virtual_addr_pool['FIRM_CRD_NB'] = fml_add['FIRM_CRD_NB']
    top_3_score.append(virtual_addr_pool.nlargest(1, 'cond_scr').copy(deep=True))

top_3_score_pd  = pd.concat(top_3_score, ignore_index=True)
result_mail_top_3 = pd.merge(firm_mail, top_3_score_pd, how='left', on = ['FIRM_CRD_NB'])
output_file_name = 'FIRM_mail_20170719_2.csv'
result_mail_top_3.to_csv(output_file_name, index = False) 

end_time = time.time()
time_used = end_time/60 - start_time/60
print("--- %s minutes ---" % time_used)
log_file_name = 'time_used_4_' + output_file_name + '.txt'
with open(log_file_name,'a') as time_log:
#    time_log.write('Time used in minutes: ' + str(time_used_4_addr_std) + '\n')    
    time_log.write('Time used in minutes: ' + str(time_used))
###############################################################################


print('Mission Accomplished!')



#upsstore.to_pickle(r'C:\Users\k26609\Desktop\FIRM address match POC\df_archive_20170717_1\upsstore.pkl')
#annex_brands.to_pickle(r'C:\Users\k26609\Desktop\FIRM address match POC\df_archive_20170717_1\annex_brands.pkl')
#regus.to_pickle(r'C:\Users\k26609\Desktop\FIRM address match POC\df_archive_20170717_1\regus.pkl')
#combine.to_pickle(r'C:\Users\k26609\Desktop\FIRM address match POC\df_archive_20170717_1\combine.pkl')
#firm.to_pickle(r'C:\Users\k26609\Desktop\FIRM address match POC\df_archive_20170717_1\firm.pkl')
#firm_main.to_pickle(r'C:\Users\k26609\Desktop\FIRM address match POC\df_archive_20170717_1\firm_main.pkl')
#firm_mail.to_pickle(r'C:\Users\k26609\Desktop\FIRM address match POC\df_archive_20170717_1\firm_mail.okl')
#result_main.to_pickle(r'C:\Users\k26609\Desktop\FIRM address match POC\df_archive_20170717_1\result_main.pkl')


#upsstore = pd.read_pickle(r'C:\Users\k26609\Desktop\FIRM address match POC\df_archive_20170715_3\upsstore.pkl')
#annex_brands = pd.read_pickle(r'C:\Users\k26609\Desktop\FIRM address match POC\df_archive_20170715_3\annex_brands.pkl')
#regus = pd.read_pickle(r'C:\Users\k26609\Desktop\FIRM address match POC\df_archive_20170715_3\regus.pkl')
#combine = pd.read_pickle(r'C:\Users\k26609\Desktop\FIRM address match POC\df_archive_20170715_3\combine.pkl')
#firm = pd.read_pickle(r'C:\Users\k26609\Desktop\FIRM address match POC\df_archive_20170715_3\firm.pkl')
#firm_main = pd.read_pickle(r'C:\Users\k26609\Desktop\FIRM address match POC\df_archive_20170715_3\firm_main.pkl')
#firm_mail = pd.read_pickle(r'C:\Users\k26609\Desktop\FIRM address match POC\df_archive_20170715_3\firm_mail.okl')
#result_main = pd.read_pickle(r'C:\Users\k26609\Desktop\FIRM address match POC\df_archive_20170715_3\result_main.pkl')
#
#for index, row in combine.iterrows():
#    if not isNumber(row['Zip']):
#        print(index, row['Zip'])
#
#num = 0 
#for index, row in firm_main.iterrows():
#    if not isNumber(row['Zip_main']):
#        num += 1 
#print(num)
