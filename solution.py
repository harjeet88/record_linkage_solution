import numpy as np
import pandas as pd
import os
import json
from pandas import DataFrame, Series
import pandas as pd
import numpy as np
from nltk.metrics import *
import re
from string import Template
from math import floor
from operator import truediv
import codecs

base_path = os.path.dirname(os.path.realpath(__file__))
print("Project Directory is : "+base_path)

listingData = [json.loads(line) for line in open(base_path+'/data/listings.txt')]
listings = DataFrame(listingData).reset_index()
listings.rename(columns={'index': 'original_listing_index'}, inplace=True)

products = DataFrame([json.loads(line) for line in open(base_path+'/data/products.txt')])

listingManuf = Series(Series(listings.manufacturer.ravel()).unique())
productManuf = Series(Series(products.manufacturer.ravel()).unique())

pManufsMapping = DataFrame( 
    { 'pManuf': productManuf, 'Keyword': productManuf.str.lower() }
)  # By default map each word to itself
pManufsMapping['Keyword'][pManufsMapping['pManuf'] == 'Konica Minolta'] = 'konica'
pManufsMapping = pManufsMapping.append( { 'pManuf': 'Konica Minolta', 'Keyword': 'minolta' }, ignore_index = True )
pManufsMapping = pManufsMapping.append( { 'pManuf': 'HP', 'Keyword': 'hewlett' }, ignore_index = True )
pManufsMapping = pManufsMapping.append( { 'pManuf': 'HP', 'Keyword': 'packard' }, ignore_index = True )
pManufsMapping['Keyword'][pManufsMapping['pManuf'] == 'Fujifilm'] = 'fuji'

pManufKeywords = pManufsMapping['Keyword']

def matchListingManufsToProductManufs(pManufsMapping, pManufKeywords):
    edit_distance_threshold = 2
    min_manuf_word_len = 4
    
    def matchManuf(lManuf):
        splits = lManuf.lower().split()
        for pManufKeyword in pManufKeywords:
            if pManufKeyword in splits:
                return pManufKeyword
        foundPManufs = [ p
                         for s in splits
                         for p in pManufKeywords
                         if s.find(p.lower()) >= 0
                       ]
        if len(foundPManufs) > 0:
            return foundPManufs[0]
        levenshteinPManufs = [ p
                               for s in splits
                               for p in pManufKeywords
                               if len(s) > min_manuf_word_len 
                               and edit_distance(s, p.lower()) <= edit_distance_threshold
                             ]
        if len(levenshteinPManufs) > 0:
            return levenshteinPManufs[0]
        return ''
    
    mapData = { 'lManuf': listingManuf,
                'pManufKeyword': listingManuf.apply( matchManuf )
              }
    lManufMap = DataFrame( mapData )
    lManufMap = pd.merge( lManufMap, pManufsMapping, how='left', left_on='pManufKeyword', right_on='Keyword')
    del lManufMap['Keyword']
    lManufMap['pManuf'] = lManufMap['pManuf'].fillna('')
    
    listingsByPManufAll = pd.merge( listings, lManufMap, how='inner', left_on='manufacturer', right_on='lManuf')
    return listingsByPManufAll[listingsByPManufAll['pManuf'] != ''].reindex(
        columns = ['pManuf','lManuf', 'title','currency','price', 'original_listing_index'])

def clean_column(df, col) :
    df = df[col].str.replace('-', '')
    df = df.str.strip()
    df = df.str.lower()
    return df

products['family_cleaned'] = clean_column(products,'family')

listingsByPManuf = matchListingManufsToProductManufs(pManufsMapping, pManufKeywords)

print(listingsByPManuf.head(5))

print(products.head(5))

def createJson(prod_val, df) :
    json = "{\"listings\": ["
    row_val=""
    for indx, row in df.iterrows() :
        row_val = row_val + " { \"currency\": "+row.iloc[3]+" , \"price\": "+row.iloc[4]+" , \"manufacturer\": "+row.iloc[1]+" , \"title\": "+row.iloc[2]+" },"
    row_val = row_val[:-1]
    prod_str = "] , \"product_name\": \""+prod_val+"\"}"
    
    json = json+ row_val+prod_str
    return json
os.remove(base_path+"/data/out.json")
out_file= open(base_path+"/data/out.json", "a")
for indx,prod in products.iterrows() :
    manufacturer = prod.iloc[2]
    #print(manu)
    #print("================================")
    filtered_listings = listingsByPManuf[listingsByPManuf.pManuf == manufacturer]
    
    model = prod.iloc[3]
    
    filtered_listings = filtered_listings[filtered_listings.title.str.contains(" "+model+" ")]
    
    family = prod.iloc[5]
    #print(family)
    #print( filtered_listings.size)
    if(not family ) :
        filtered_listings = filtered_listings[filtered_listings.title.str.contains(" "+family+" ")]
    
    #list_json = filtered_listings[['currency', 'price','title']]
    #list_json['manufacturer'] = filtered_listings.lManuf
    jsn = createJson(prod.iloc[4],filtered_listings)
    out_file.write(jsn)
    out_file.write("\n")

print("Output Written at path : "+base_path+"/data/out.json")
