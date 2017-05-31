from django.shortcuts import get_object_or_404
from rest_framework.views import APIView
from django.views import generic
from rest_framework.response import Response
from rest_framework import renderers
from rest_framework.reverse import reverse
from rest_framework import generics
from rest_framework import status
from django.http import JsonResponse
import math
from django.db.models import Count, Min, Max, Sum, Avg, F, FloatField
import datetime
from time import strftime,gmtime
from django.conf import settings
import pandas as pd
from django_pandas.io import read_frame
from django.utils import six
import json
import numpy as np
# import re


# Models for NPD Opportunity View
from .models import outperformance, pricebucket, unmatchedprod

# Models for NPD Impact View
from .models import bc_allprod_attributes, attribute_score_allbc, consolidated_calculated_cannibalization, npd_supplier_ads,consolidated_buckets,seasonality_index,uk_holidays,npd_calendar,merch_range,input_npd,brand_grp_mapping,range_space_store_future,store_details,product_contri,product_price,product_desc,cannibalization_vol_buckets

# Models for NPD Imapct Save Scenario
from .models import SaveScenario, ScenarioTracker

# Serializers for Unmacthed product and supplier table 
from .serializers import unmatchedprodSerializer, npd_impact_tableSerializer

# Serializers NPD Imapct Save Scenario
from .serializers import npd_SaveScenarioSerializer,npd_ViewScenarioSerializer

from django.core.paginator import Paginator 

import gzip
import xgboost as xgb
import pickle
#for cache 
from rest_framework_extensions.cache.decorators import cache_response


## for NPD Opportunity View

# How is tesco performing wrt market?
class market_outperformance(APIView):
    def get(self, request, *args):

        #input from header
        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}

        designation = args.pop('designation__iexact',None)
        user_id = args.pop('user_id__iexact',None)
        session_id = args.pop('session_id__iexact',None)
        user_name = args.pop('user_name__iexact', None)
        buying_controller_header = args.pop('buying_controller_header__iexact',None)
        buyer_header = args.pop('buyer_header__iexact',None)
        #header over

        #input from args
        kwargs = {
            'buying_controller': 'Meat Fish and Veg',
            'junior_buyer': 'Coated Fish'
        }
        args.pop('product_sub_group_description__iexact',None)

        week={}
        week["week_flag__iexact"]=args.pop('week_flag__iexact',None)

        if week["week_flag__iexact"]==None:
            week = {
                'week_flag__iexact': 'Latest 13 Weeks'
            }
        elif week["week_flag__iexact"]=='Latest 26 Weeks':
            week = {
                'week_flag__iexact': 'Latest 13 Weeks'
            }
        if not args:
            df = read_frame(outperformance.objects.filter(**kwargs).filter(**week).order_by('product_sub_group_description'))

        else:
            df = read_frame(outperformance.objects.filter(**args).filter(**week).order_by('product_sub_group_description'))

        df_required = df[['product_sub_group_description', 'tesco_outperformanc_percentage',
                          'tesco_outperformanc_unit_prcnt']]
        df_required['tesco_outperformanc_unit_prcnt'] = df_required['tesco_outperformanc_unit_prcnt'].astype(
            'float')
        df_required['tesco_outperformanc_percentage'] = df_required['tesco_outperformanc_percentage'].astype(
            'float')
        df_required['tesco_outperformanc_unit_prcnt'] = df_required['tesco_outperformanc_unit_prcnt'].round(
            decimals=2)
        df_required['tesco_outperformanc_percentage'] = df_required['tesco_outperformanc_percentage'].round(
            decimals=2)

        label = list(df_required['product_sub_group_description'].unique())
        sales = df_required.rename(columns={'tesco_outperformanc_percentage': 'values'})
        sales = pd.DataFrame(sales['values'])
        sales = sales.to_dict(orient="list")
        sales['label'] = 'TESCO Value (%)'

        volume = df_required.rename(columns={'tesco_outperformanc_unit_prcnt': 'values'})
        volume = pd.DataFrame(volume['values'])
        volume = volume.to_dict(orient="list")
        volume['label'] = 'TESCO Volume (%)'

        series = []
        series.append(sales)
        series.append(volume)

        output = {}
        output['labels'] = label
        output['series'] = series

        return JsonResponse(output, safe=False)


# How are sku's dIstributed across psg for Tesco and its competitors
class psgskudistribution(APIView):
    def get(self, request, *args):
        #input from header
        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        designation = args.pop('designation__iexact',None)
        user_id = args.pop('user_id__iexact',None)
        session_id = args.pop('session_id__iexact',None)
        user_name = args.pop('user_name__iexact', None)
        buying_controller_header = args.pop('buying_controller_header__iexact',None)
        buyer_header = args.pop('buyer_header__iexact',None)
        #header over

        #input from args
        kwargs = {
            'buying_controller': 'Meat Fish and Veg',
            'junior_buyer': 'Coated Fish'
        }

        args.pop('product_sub_group_description__iexact',None)
        week={}
        week["week_flag__iexact"]=args.pop('week_flag__iexact',None)
        if week["week_flag__iexact"]==None:
            week = {
                'week_flag__iexact': 'Latest 13 Weeks'
            }
        if not args:
            queryset_df = read_frame(pricebucket.objects.filter(**kwargs).filter(**week).values('retailer', 'product_sub_group_description','sku').order_by('retailer','product_sub_group_description').distinct())
        else:
            queryset_df = read_frame(pricebucket.objects.filter(**args).filter(**week).values('retailer', 'product_sub_group_description', 'sku').order_by('retailer','product_sub_group_description').distinct())

        queryset_df['sku'] = queryset_df['sku'].astype('float')
        psg_distinct = queryset_df['product_sub_group_description'].unique()
        final_data = []
        for j in range(0, len(psg_distinct)):
            dict_psg_data = {}
            dict_psg_data = {"psg": psg_distinct[j]}
            psg_subset = queryset_df[queryset_df['product_sub_group_description'] == psg_distinct[j]]
            psg_subset = psg_subset.reset_index(drop=True)
            p = {}
            for i in range(0, len(psg_subset)):
                # for i in range(0,9):
                psg_data = {
                    psg_subset["retailer"][i]: psg_subset["sku"][i]
                }
                p.update(psg_data)
            dict_psg_data.update(p)
            final_data.append(dict_psg_data)

        retailers_distinct = list(queryset_df['retailer'].unique())
        colors_list = ['#B2B2B2','#7FB256','#0931F6','#C288D6','#896219','#F60909','#E5F213']
        label = {}
        label = {
            "labels": retailers_distinct
        }
        data = {"data": final_data}
        colors={"colors":colors_list}
        label.update(data)
        label.update(colors)

        return JsonResponse(label, safe=False)

# To determine the number of sku's with Tesco and its competitor at psg within different price bands
class pricebucket_skudistribution(APIView):
    def get(self, request, *args):
        #input from header
        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}

        designation = args.pop('designation__iexact',None)
        user_id = args.pop('user_id__iexact',None)
        session_id = args.pop('session_id__iexact',None)
        user_name = args.pop('user_name__iexact', None)
        buying_controller_header = args.pop('buying_controller_header__iexact',None)
        buyer_header = args.pop('buyer_header__iexact',None)       
        #header over

        #input from args
        kwargs = {
            'buying_controller': 'Meat Fish and Veg',
            'junior_buyer': 'Coated Fish',
            'product_sub_group_description': 'FRZ SCAMPI'
        }
        week={}
        week["week_flag__iexact"]=args.pop('week_flag__iexact',None)
        if week["week_flag__iexact"]==None:
            week = {
                'week_flag__iexact': 'Latest 13 Weeks'
            }
        if not args:
            df = read_frame(pricebucket.objects.filter(**kwargs).filter(**week))
        else:
            df = read_frame(pricebucket.objects.filter(**args).filter(**week))

        df_required = df[['retailer', 'sku_gravity', 'price_gravity']]
        df_required['minvalue'] = df_required['price_gravity'].str.split('-').str[0]
        df_required['minvalue']=df_required['minvalue'].astype('float')
        df_required = df_required.sort_values(by='minvalue')
        price_bucket=list(df_required['price_gravity'].unique())

        df_required['sku_gravity'] = df_required['sku_gravity'].astype(float)
        comp_g = df_required['retailer'].unique()
        arr=[]
        color_comp=[]
        arr_colors={}
        arr_colors={
            "Tesco" : '#F60909',
            "Lidl" : '#C288D6',
            "Aldi" : '#B2B2B2',
            "Asda" : '#7FB256',
            "Morrisons" : '#896219',
            "JS" : '#0931F6',
            "Waitrose" : '#E5F213'
            }
        data_dict={}
        for i in range(0, len(comp_g)):
            competitor = comp_g[i]
            temp_g = df_required[df_required.retailer == comp_g[i]]
            temp_g = temp_g.reset_index(drop=True)
            for j in range(0, len(temp_g)):
                arr.append(
                    {
                    'sku_gravity': temp_g['sku_gravity'][j],
                    'price_gravity': temp_g['price_gravity'][j],
                    'id':comp_g[i]
                    }
                )
            color_comp.append(arr_colors[comp_g[i]])
        data_dict["price_bucket"]=price_bucket
        data_dict["data"]=arr
        data_dict["colors"]=color_comp
        return JsonResponse(data_dict, safe=False)

# List of prducts and their asp with competitors but not with TESCO
class unmatched_products(APIView):
    def get(self, request, *args):
        #input from header
        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}

        designation = args.pop('designation__iexact',None)
        user_id = args.pop('user_id__iexact',None)
        session_id = args.pop('session_id__iexact',None)
        user_name = args.pop('user_name__iexact', None)
        buying_controller_header = args.pop('buying_controller_header__iexact',None)
        buyer_header = args.pop('buyer_header__iexact',None)        
        #header over

        #input from args
        kwargs = {
            'buying_controller': 'Meat Fish and Veg',
            'junior_buyer': 'Coated Fish',
            'product_sub_group_description': 'FRZ SCAMPI'
        }

        week={}
        week["week_flag__iexact"]=args.pop('week_flag__iexact',None)
        if week["week_flag__iexact"]==None:
            week = {
                    'week_flag__iexact': 'Latest 13 Weeks'
            }
        if not args:
            queryset = unmatchedprod.objects.filter(**kwargs).filter(**week)
        else:
            queryset = unmatchedprod.objects.filter(**args).filter(**week)
        serializer_class = unmatchedprodSerializer(queryset, many=True)
        return JsonResponse({'table': serializer_class.data}, safe=False)

## For NPD Imapct View
# Net impact of introducing a new product with given attributes
# functions for npd impact
class npd_impact_functions:
    def __init__(self):
        self.prod_param_list={}
        self.df_list={}
        self.output_list ={}        

    ### To get the priceband , psg code and merch code
    def get_priceband_psg_merch_code(self,prod_param_list,df_list):
        ###Getting a price band based on user selection
        Buying_controller = prod_param_list['Buying_controller']
        asp = prod_param_list['asp']   
        input_dataset = df_list['input_dataset']
        Product_Sub_Group_Description = prod_param_list['Product_Sub_Group_Description']
        merch_range_df = df_list['merch_range_df']
        Merchandise_Group_Description = prod_param_list['Merchandise_Group_Description']

        if((Buying_controller == 'Frozen Impulse') | (Buying_controller == 'HotandSweet') |(Buying_controller == 'Beers') | (Buying_controller == 'Meat Fish and Veg') |(Buying_controller == 'Grocery Cereals')):
            if(0 < asp <= 2):
                price_band = '0 to 2'
            if(2 < asp <= 4):
                price_band = '2 to 4'
            if(4 < asp <= 6):
                price_band = '4 to 6'
            if(6 < asp <= 8):
                price_band = '6 to 8'
            if(8 < asp <= 10):
                price_band = '8 to 10'
            if(10 < asp <= 12):
                price_band = '10 to 12'
            if(12 < asp <= 14):
                price_band = '12 to 14'
            if(14 < asp <= 16):
                price_band = '14 to 16'
            if(16 < asp <= 18):
                price_band = '16 to 18'
            if(18 < asp <= 20):
                price_band = '18 to 20'
            if(20 < asp <= 23):
                price_band = '20 to 23'
            if(23 < asp <= 26):
                price_band = '23 to 29'
            if(26 < asp <= 29):
                price_band = '26 to 29'
            if(29 < asp ):
                price_band = 'GRTR 29'

        if (Buying_controller == 'Spirits'):
            if(0 < asp <= 3):
                price_band = '0 to 3' 
            if(3 < asp <= 6):
                price_band = '3 to 6'
            if(6 < asp <= 9):
                price_band = '6 to 9'
            if(9 < asp <= 12):
                price_band = '9 to 12'
            if(12 < asp <= 15):
                price_band = '12 to 15'
            if(15 < asp <= 18):
                price_band = '15 to 18'
            if(18 < asp <= 21):
                price_band = '18 to 21'
            if(21 < asp <= 24):
                price_band = '21 to 24'
            if(24 < asp <= 27):
                price_band = '24 to 27'
            if(27 < asp <= 30):
                price_band = '27 to 30'
            if(30 < asp <= 35):
                price_band = '30 to 35'
            if(35 < asp <= 40):
                price_band = '35 to 40'
            if(40 < asp <= 45):
                price_band = '40 to 45'
            if(45 < asp <= 50):
                price_band = '45 to 50'
            if(50 < asp <= 60):
                price_band = '50 to 60'
            if(60 < asp <= 70):
                price_band = '60 to 70'
            if(70 < asp <= 80):
                price_band = '70 to 80'
            if(80 < asp <= 90):
                price_band = '80 to 90'
            if(90 < asp <= 100):
                price_band = '90 to 100'
            if(100 < asp ):
                price_band = 'GRTR 100'

        if (Buying_controller == 'Wines'):
            if(0 < asp <= 5):
                price_band = '0 to 5'
            if(5 < asp <= 10):
                price_band = '5 to 10'
            if(10 < asp <= 15):
                price_band = '10 to 15'
            if(15 < asp <= 20):
                price_band = '15 to 20'
            if(20 < asp <= 25):
                price_band = '20 to 25'
            if(25 < asp <= 30):
                price_band = '25 to 30'
            if(30 < asp <= 35):
                price_band = '30 to 35'
            if(35 < asp <= 40):
                price_band = '35 to 40'
            if(40 < asp <= 45):
                price_band = '40 to 45'
            if(45 < asp <= 50):
                price_band = '45 to 50'
            if(50 < asp <= 60):
                price_band = '50 to 60'
            if(60 < asp <= 70):
                price_band = '60 to 70'
            if(70 < asp <= 80):
                price_band = '70 to 80'
            if(80 < asp <= 90):
                price_band = '80 to 90'
            if(90 < asp <= 100):
                price_band = '90 to 100'
            if(100 < asp <= 150):
                price_band = '100 to 150'
            if(150 < asp <= 200):
                price_band = '150 to 200'
            if(200 < asp <= 250):
                price_band = '200 to 250'
            if(250 < asp <= 300):
                price_band = '250 to 300'
            if(300 < asp <= 350):
                price_band = '300 to 350'
            if(350 < asp <= 400 ):
                price_band = '350 to 400'

        #####Getting a psg code based on psg desc
        psg_code = input_dataset[input_dataset['product_sub_group_description'] == Product_Sub_Group_Description].iloc[0]['product_sub_group_code']        
        merch_code = merch_range_df[merch_range_df['merchandise_group_description']==Merchandise_Group_Description].iloc[0]['merchandise_group_code']     
        psg_priceband_merch = {'psg_code' : psg_code, 'price_band' : price_band ,'merch_code' : merch_code}
        return psg_priceband_merch

    #### Making structure with 12 rows. Each row for a month
    def get_ads_structure(self,prod_param_list,df_list,output_list):
        dataset1 = output_list['dataset1']
        week_mapping = df_list['week_mapping']
        current_month = output_list['current_month']
        df =dataset1.iloc[0:11,]
        month_mapping_subset = week_mapping.loc[week_mapping.year_period_number>=current_month]
        ### To get the list of all 12 months
        month_list = pd.DataFrame(month_mapping_subset['year_period_number'].unique())
        month_list.columns = ['year_period_number']
        month_list.sort_values(by= ['year_period_number'],ascending=False)
        month_list = month_list.iloc[0:12]
        df = pd.concat([df.reset_index(drop=True), month_list], axis=1)
        df= df.fillna(0)
        return df
    ###On column added which is the year week number
    #### Have incorporated margin_percent and acp 
    def fill_ads_structure(self,prod_param_list,df_list,output_list):
        All_attribute = df_list['All_attribute']
        df = output_list['df'] 
        acp = prod_param_list['acp']
        Junior_Buyer = prod_param_list['Junior_Buyer']
        Buyer = prod_param_list['Buyer']
        Package_Type = prod_param_list['Package_Type']
        measure_type = prod_param_list['measure_type']
        Brand_Name = prod_param_list['Brand_Name']
        brand_grp_mapping_df = df_list['brand_grp_mapping_df']
        SI = df_list['SI']
        psg_code = output_list['psg_code'] 
        current_month = output_list['current_month']
        price_band = output_list['price_band']
        week_mapping = df_list['week_mapping']
        Product_Sub_Group_Description = prod_param_list['Product_Sub_Group_Description']
        Size = prod_param_list["Size"]
        asp = prod_param_list['asp']
        dataset = df
        ####Brand to ind and grp mapping 
        brand_grp_mapping = All_attribute
        ### Adding new feature as abs 
        margin_percent = abs((asp-acp)/acp)
        #### Assigning the value of '1' for the collected inputs (Categorical Variables)
        if "junior_buyer_" + Junior_Buyer in dataset.columns:
            dataset.loc[:,"junior_buyer_" + Junior_Buyer]=1
        if 'buyer_' + Buyer in dataset.columns:
            dataset.loc[:,"buyer_" + Buyer] = 1
        if "package_type_" + Package_Type in dataset.columns:
            dataset.loc[:,"package_type_" + Package_Type] = 1
        if "product_sub_group_description_" + Product_Sub_Group_Description in dataset.columns:
            dataset.loc[:,"product_sub_group_description_" + Product_Sub_Group_Description] = 1
        if "measure_type_" + measure_type in dataset.columns:
            dataset.loc[:,"measure_type_" + measure_type] = 1
        if "price_band_" + price_band in dataset.columns:
            dataset.loc[:,"price_band_" + price_band] = 1
                    
        #### Directly input from the users
        if "asp" in dataset.columns:
            dataset.loc[:,"asp"] = asp
        if "acp" in dataset.columns:
            dataset.loc[:,"acp"] = acp
        if "margin_percent" in dataset.columns:
            dataset.loc[:,"margin_percent"] = margin_percent
        if "size" in dataset.columns:
            dataset.loc[:,"size"] = Size

        #### Mapping Brand name to brand grp (Getting the rank of the brand selected)
        ##### Reading a mapping file
        brand_group = (brand_grp_mapping_df.loc[brand_grp_mapping_df['brand_name'] == Brand_Name]).iloc[0]['brand_grp20']

        #### Assigning the rank of the group in our ADS
        if "brand_grp20" in dataset.columns:
            dataset.loc[:,"brand_grp20"] = brand_group

        ####Getting the brand indicator 
        brand_ind = (brand_grp_mapping_df.loc[brand_grp_mapping_df['brand_name'] == Brand_Name]).iloc[0]['brand_ind']

        # Need to QC this
        if (brand_ind=='T'):
            brand_indicator = 1
        if (brand_ind == 'B'):
            brand_indicator = 0
        
        if "brand_ind" in dataset.columns:
            dataset.loc[:,"brand_ind"] = brand_indicator
        ### Filling all columns related to week number 
        #### Looping for all the weeks
        dataset.loc[:,"weeks_since_launch"] = 0
        #### Arranging week in ascending order
        month_mapping_subset = week_mapping.sort_values('year_period_number')
        #Converting date to the date format
        month_mapping_subset['calendar_date'] = pd.to_datetime(month_mapping_subset['calendar_date'], format= '%Y-%m-%d')
                
        date_map = month_mapping_subset[['year_period_number', 'quarter_number', 'period_number', 'year_number']].drop_duplicates()
        date_map.columns = ['year_period_number', 'quarter_number', 'period_number', 'year_number']
        date_sparse = pd.get_dummies(date_map, prefix = ['quarter_number', 'period_number'], 
                                               columns = ['quarter_number', 'period_number'])
        date_sparse.drop('period_number_13', 1, inplace= True)
        
        # Removed the previous hard coded value
        if all(x in dataset.columns for x in date_sparse.columns[date_sparse.columns != 'year_period_number']):
            dataset.drop(date_sparse.columns[date_sparse.columns != 'year_period_number'], 1, inplace =True)
            dataset = pd.merge(dataset, date_sparse, on = ['year_period_number'], how = 'left')
        
        # Getting week count in each month and use it to create a cumulative column. This column can be used as weeks since launch
        week_count = week_mapping.groupby('year_period_number', as_index =False)['year_week_number'].count()
        week_count.columns = ['year_period_number', 'period_week_count']
        week_count = week_count.loc[week_count.year_period_number>=current_month]
        week_count['weeks_since_launch'] = week_count.period_week_count.cumsum()
        
        if 'weeks_since_launch' in dataset.columns:
            dataset.drop('weeks_since_launch', 1, inplace =True)
            dataset = pd.merge(dataset, week_count, on = ['year_period_number'], how = 'left')
        
        #### Weeks since launch will be incremental 
        #Taking weeks from launch as 1 for the first week
        #### Reading a mapping file
        SI_psg = SI.loc[(SI['psg'] == psg_code)]
        SI_psg['adjusted_index']=SI_psg['adjusted_index'].astype(float)
        SI_psg = SI_psg[['months','adjusted_index']]
        ####Getting a week column to get the seasonality index
        dataset['months'] = dataset['year_period_number']%100
        dataset = pd.merge(dataset, SI_psg, left_on=['months'], right_on=['months'], how='left' )
        del(dataset['si'])
        dataset = dataset.rename(columns={'adjusted_index':'si'})
        del(dataset['months'])
        dataset['si'] = dataset['si'].astype(float)
        return dataset

    def similar_products(self,prod_param_list,df_list,output_list):

        All_attribute = df_list['All_attribute']
        Buying_controller = prod_param_list['Buying_controller']
        Brand_Name = prod_param_list['Brand_Name']
        Package_Type = prod_param_list['Package_Type']
        Till_Roll_Description = prod_param_list['Till_Roll_Description']
        Size = prod_param_list['Size']
        measure_type = prod_param_list['measure_type']
        price_band = output_list['price_band']
        Product_Sub_Group_Description = prod_param_list['Product_Sub_Group_Description']
        attribute_score = df_list['attribute_score']

        #### Getting number of subs same and different brand
        All_attribute_bc = All_attribute.loc[(All_attribute['bc'] == Buying_controller)]
        All_attribute_treated = All_attribute.dropna()
        # All_attribute_treated.to_csv('all_attribute.csv')
        # In the new logic, PSG is not a hard and fast rule. So PSG filter is not required

        ###Creating a empty data frame of attributes to compare. Will fill the values based on user selection
        match_df = pd.DataFrame(0, index = [0],  columns = [ "brand_name", "package_type", "till_roll_description", "size", "measure_type", "price_band", "psg"] )

        ##### As we need price band we need to convert the asp (user input) into price bands based on BC and asp
        #### Filling the empty data frames with values as now we have the price band

        match_df.loc[:,"brand_name"] = Brand_Name
        match_df.loc[:,"package_type"] = Package_Type
        match_df.loc[:,"till_roll_description"] = Till_Roll_Description
        match_df.loc[:,"size"] = Size
        match_df.loc[:,"measure_type"] = measure_type
        match_df.loc[:,"price_band"] = price_band
        match_df.loc[:,"psg"] = Product_Sub_Group_Description
        
        #### To compare with all attributes we need to merge all attributes to user selection. For that we will map it on a common key
        All_attribute_treated['key'] = 1
        match_df['key'] = 1
        match_all_prod = pd.merge(match_df, All_attribute_treated, on = 'key')
        
        match_all_prod['size_x'] = match_all_prod['size_x'].astype('float')
        match_all_prod['size_y'] = match_all_prod['size_y'].astype('float')

        ##### Assigning flags to every row for every user selection : psg,brand name, pkg type,size,measure,price band,till roll desc
        ### Measure and size are related we will be given commom flag based on some condition
        match_all_prod['brand_flag'] = np.where(match_all_prod.loc[:,"brand_name_x"] == match_all_prod.loc[:,"brand_name_y"], 1, 0)
        match_all_prod['package_flag'] = np.where(match_all_prod.loc[:,"package_type_x"] == match_all_prod.loc[:,"package_type_y"], 1, 0)
        match_all_prod['Size_flag'] = np.where((match_all_prod.loc[:,"measure_type_x"] == match_all_prod.loc[:,"measure_type_y"]) & 
                                   ((match_all_prod.loc[:,"measure_type_y"] == 'G')  & 
                                   ((match_all_prod.loc[:,"size_x"] - match_all_prod.loc[:,"size_y"]) <= 200)) |
                                   ((match_all_prod.loc[:,"measure_type_y"] == 'ML')  & 
                                   ((match_all_prod.loc[:,"size_x"] - match_all_prod.loc[:,"size_y"]) <= 200)) |
                                    ((match_all_prod.loc[:,"measure_type_y"] == 'SNGL')  & 
                                   ((match_all_prod.loc[:,"size_x"] - match_all_prod.loc[:,"size_y"]) <= 2)) , 1, 0)
        match_all_prod['price_flag'] = np.where(match_all_prod.loc[:,"price_band_x"] == match_all_prod.loc[:,"price_band_y"], 1, 0)
        match_all_prod['till_roll_flag'] = np.where(match_all_prod.loc[:,"till_roll_description_x"] == match_all_prod.loc[:,"till_roll_description_y"], 1, 0)
        match_all_prod['psg_flag'] = np.where(match_all_prod.loc[:,"psg"] == match_all_prod.loc[:,"product_sub_group_description"], 1, 0)
        
        ### Subsetting score importance based on PSG
        score = attribute_score[attribute_score.loc[:, 'bc'] == Buying_controller]

        
        ###Getting the percentage score based on flag*individual score
        match_all_prod['brand_score'] = match_all_prod.loc[:,"brand_flag"]*score['avg_brand'].values
        match_all_prod['package_score'] = match_all_prod.loc[:,"package_flag"]*score['avg_pkg'].values
        match_all_prod['Size_score'] = match_all_prod.loc[:,"Size_flag"]*score['avg_size'].values
        match_all_prod['price_score'] = match_all_prod.loc[:,"price_flag"]*score['avg_price'].values
        match_all_prod['till_roll_score'] = match_all_prod.loc[:,"till_roll_flag"]*score['avg_tillroll'].values
        match_all_prod['psg_score'] = match_all_prod.loc[:,"psg_flag"]*score['avg_psg'].values
        
        ####Getting the final score for every row based on summation of individual attribute score
        match_all_prod['final_score'] = match_all_prod['brand_score'] + match_all_prod['package_score'] + match_all_prod['Size_score'] + match_all_prod['price_score'] + match_all_prod['till_roll_score'] + match_all_prod['psg_score']

        #### Subsetting for score greater than 0.7 (threshold)
        # match_all_prod.to_csv('match_all_prod.csv')
        sim_prod = match_all_prod[match_all_prod['final_score'] > 0.4]

        return sim_prod

    def subs_same_different(self,prod_param_list,df_list,output_list):
        ####Total products left after putting a threshold of 0.7
        df1 = output_list['df1']
        sim_prod = output_list['sim_prod']
        dataset = df1

        print("sim_prod")
        print(sim_prod.head())
        sim_prod_copy = sim_prod[sim_prod['final_score']>0.7]
        tot_prod = sim_prod_copy.shape[0]
        #### Same brand product count
        same_brand_count = sum(sim_prod_copy['brand_flag'] == 1)

        #### Total - same = different brand
        diff_brand_count = tot_prod - sum(sim_prod_copy['brand_flag'] == 1)

        ##### Assigning values to our columns in ADS (no_of_subs_same_brand , no_of_subs_diff_brand)
        if 'no_of_subs_same_brand' in dataset.columns:
            dataset.loc[:, 'no_of_subs_same_brand'] = same_brand_count
        if 'no_of_subs_diff_brand' in dataset.columns:
            dataset.loc[:, 'no_of_subs_diff_brand'] = diff_brand_count
        return dataset

    def psg_pb_prodcount(self,prod_param_list,df_list,output_list):

        df2 = output_list['df2']
        All_attribute = df_list['All_attribute']
        Product_Sub_Group_Description = prod_param_list['Product_Sub_Group_Description']
        price_band = output_list['price_band']

        dataset = df2
        #### To get the psg prod count and price band count
        ###### Subsetting all attribute data for the selected psg
        All_attribute_psg = All_attribute.loc[(All_attribute['product_sub_group_description'] == Product_Sub_Group_Description)]

        #### Counting the distinct products in the selected psg
        count_psg_prod = len(All_attribute_psg['base_product_number'].unique())

        ### Counting the distinct products in the price band
        ### Price band we have calculated before based on if else conditions
        ## Subsetting for the selected price band
        All_attribute_pb = All_attribute.loc[(All_attribute['price_band']== price_band)]
        count_pb_prod = len(All_attribute_pb['base_product_number'].unique())
        #### Filling values for the count psg prod and price band 
        if 'psg_prod_count' in dataset.columns:
            dataset.loc[:, 'psg_prod_count'] = count_psg_prod
        if 'price_band_prod_count' in dataset.columns:
            dataset.loc[:, 'price_band_prod_count'] = count_pb_prod
        return dataset

    ### To incorporate merch grp and range class
    def no_of_stores_holidays(self,prod_param_list,df_list,output_list):

        df3 = output_list['df3']
        range_space_store = df_list['range_space_store']
        merch_code = output_list['merch_code']
        Range_class = prod_param_list['Range_class']
        store_details_df = df_list['store_details_df']
        uk_holidays_df = df_list['uk_holidays_df']
        current_month = output_list['current_month']

        dataset = df3

        stores = pd.DataFrame(range_space_store[(range_space_store['merchandise_group_code'] == merch_code) & 
                               (range_space_store['range_class'] >= Range_class)]['retail_outlet_number'])

        stores_size = pd.merge(stores, store_details_df, on = 'retail_outlet_number', how = 'left')
        # APC 1 & 5 are Express stores and remaining others are Main Estate
        
        print(store_details_df.head())
        print("stores")
        print(stores.head())
        print(stores.columns)
        print("stores_size")
        print(stores_size.columns)
        print(stores_size.head())
        stores_size['store_type']  = np.where((stores_size.area_price_code == 1) | (stores_size.area_price_code == 5), 'EXP', 'ME')
        
        # Previous APC_stores has been changed to store_count with modifications
        store_count = stores_size.groupby(['store_type'], as_index = False).aggregate({'retail_outlet_number': lambda x: x.nunique(),
                        'pfs_store': lambda x: x.nunique(), 'store_5k': lambda x: x.nunique(),
                        'store_20k': lambda x: x.nunique(), 'store_50k': lambda x: x.nunique(),
                        'store_100k': lambda x: x.nunique(), 'store_100kplus': lambda x: x.nunique()})
        store_count = store_count.loc[:,['store_type', 'retail_outlet_number', 'pfs_store', 'store_5k', 'store_20k', 'store_50k', 'store_100k', 'store_100kplus']]
        store_count.columns = ['store_type', 'no_stores', 'no_pfs_Stores', 'no_5k_stores',
           'no_20k_stores', 'no_50k_stores', 'no_100k_stores', 'no_100kplus_stores']
        store_count.columns = map(str.lower, store_count.columns)
        df_final = pd.DataFrame()
        for i in range(0,len(store_count)):
            df_temp = dataset.copy()
            ####### To update the no of stores based on store type
            if "store_type_" + str(store_count['store_type'][i]) in df_temp.columns:
                df_temp.loc[:,"store_type_" + str(store_count['store_type'][i])] = 1
            if "no_stores" in df_temp.columns:  
                df_temp.loc[:,"no_stores"] = store_count['no_stores'][i]
            if "no_pfs_stores" in df_temp.columns:  
                df_temp.loc[:,"no_pfs_stores"] = store_count['no_pfs_stores'][i]
            if "no_5k_stores" in df_temp.columns:
                df_temp.loc[:,"no_5k_stores"] = store_count['no_5k_stores'][i]
            if "no_20k_stores" in df_temp.columns:
                df_temp.loc[:,"no_20k_stores"] = store_count['no_20k_stores'][i]
            if "no_50k_stores" in df_temp.columns:
                df_temp.loc[:,"no_50k_stores"] = store_count['no_50k_stores'][i]
            if "no_100k_stores" in df_temp.columns:
                df_temp.loc[:,"no_100k_stores"] = store_count['no_100k_stores'][i]
            if "no_100kplus_stores" in df_temp.columns:
                df_temp.loc[:,"no_100kplus_stores"] = store_count['no_100kplus_stores'][i]
            
            uk_holidays_df['year_period_number'] = pd.to_numeric(uk_holidays_df.year_number.map(str) + uk_holidays_df.period_number.map("{:02}".format))
            #### To get the holidays count based on store type
            uk_holidays_df_store = uk_holidays_df[['year_period_number','holiday_count','store_type']]
            uk_holidays_df_store = uk_holidays_df_store.loc[uk_holidays_df['store_type']== store_count['store_type'][i]]
            uk_holidays_df_store = uk_holidays_df_store[['year_period_number','holiday_count']]

            #### To get the count of holidays in a week. Sme week will repeat for n number of holidays n times
            ####Doing a left join on our ADS to get the holiday count. Wherever it is NA it will be given 0 as our holiday table has only ####those weeks which has atleast 1 holiday 
            if 'holiday_count' in df_temp.columns:
                dataset.drop('holiday_count', 1, inplace =True)
                df_temp = pd.merge(df_temp,uk_holidays_df_store, left_on=['year_period_number'], right_on=['year_period_number'], how='left' )
                df_temp= df_temp.fillna(0)
            
            df_final = df_final.append(df_temp)
        df_final_new = df_final
        df_final_new.loc[:,'launch_month'] = current_month
        return df_final_new

    def run_cannibilization_model(self,prod_param_list,df_list,output_list):

        df_test = output_list['df_test']
        week_flag = prod_param_list['week_flag']
        time_frame = output_list['time_frame']
        xg_model = output_list['xg_model']
        asp = prod_param_list['asp']
        sim_prod = output_list['sim_prod']
        bc_cannibilization = df_list['bc_cannibilization']
        consolidated_buckets_df =df_list['consolidated_buckets_df']
        brand_grp_mapping_df = df_list['brand_grp_mapping_df']
        Brand_Name = prod_param_list['Brand_Name']
        cann_vol_bucket = df_list['cann_vol_bucket']
        Buying_controller = prod_param_list['Buying_controller']
        psg_code = output_list['psg_code']
        Junior_Buyer = prod_param_list['Junior_Buyer']
        All_attribute = df_list['All_attribute']
        product_contri_df = df_list['product_contri_df']
        Product_Sub_Group_Description = prod_param_list['Product_Sub_Group_Description']
        product_price_df = df_list['product_price_df']
        Buyer = prod_param_list['Buyer'] 
        acp = prod_param_list['acp']
        Size = prod_param_list['Size'] 
        Package_Type = prod_param_list['Package_Type'] 
        Till_Roll_Description = prod_param_list['Till_Roll_Description'] 
        price_band = output_list['price_band']

        input_test_dataset= df_test

        #### Similar product matching
        ###Getting base product number and their respective cannibilization
        bc_cannibilization = bc_cannibilization[['base_product_number', 'cannibalization']]
        ## To get the base product number and launch tesco week mapping
        BPN_launch_week = All_attribute[['base_product_number', 'launch_tesco_week']]
        ##Consolidated buckets subsetting only for the bc and making three datasets for calculating cannibilization percent
        consolidated_buckets_df = consolidated_buckets_df[consolidated_buckets_df['buying_controller']==Buying_controller]

        PSGVolBrandBuckets = consolidated_buckets_df[consolidated_buckets_df['bucket_flag']=='PSGVolBrandBuckets']
        PSGVolBuckets = consolidated_buckets_df[consolidated_buckets_df['bucket_flag']=='PSGVolBuckets']
        PSGBuckets = consolidated_buckets_df[consolidated_buckets_df['bucket_flag']=='PSGBuckets']
        
        # Newly added to get cannibalization for all NPDs
        Junior_BuyerVolBrandBuckets = consolidated_buckets_df[consolidated_buckets_df['bucket_flag']=='Junior_BuyerVolBrandBuckets']
        Junior_BuyerVolBuckets = consolidated_buckets_df[consolidated_buckets_df['bucket_flag']=='Junior_BuyerVolBuckets']
        Junior_BuyerBuckets = consolidated_buckets_df[consolidated_buckets_df['bucket_flag']=='Junior_BuyerBuckets']

        Cannibalization_perc = 0 
        week_flag = week_flag
        time_frame = time_frame
        ####Xg boost model pickled
        #### Have to be inside function which takes test datasets as a argument
        #### For xg boost to run we need to have our test dataset in matrix form. Converting our test dataframe to required matrix
        testdmat = xgb.DMatrix(input_test_dataset.loc[:,xg_model.feature_names])
        #### Predicting the volume for the futute weeks (13,26,51)
        Volume = xg_model.predict(testdmat)
        test = pd.DataFrame(Volume)
        ##### Summing all the volumes for the weeks in 13,26,and 52 window
        total_forecasted_volume = sum(Volume)

        #### As now we are aware of the total volume forecasted we can multiply with asp (user selection) to get total sales
        total_forecasted_sales = total_forecasted_volume*asp

        #### To check if any sister products are available. If len(sim_prod)=0 after merge it means no sister product available or no ##similar products passed the threshod of 0.8 
        sim_prod_new = pd.merge(sim_prod, bc_cannibilization, left_on=['base_product_number'], right_on=['base_product_number'], how='inner')
        brand_ind = (brand_grp_mapping_df.loc[brand_grp_mapping_df['brand_name'] == Brand_Name]).iloc[0]['brand_ind']
        
        # Need to add this as a model
        low_cutoff = (cann_vol_bucket.loc[cann_vol_bucket['buying_controller'] == Buying_controller]).iloc[0]['low_cutoff']
        high_cutoff = (cann_vol_bucket.loc[cann_vol_bucket['buying_controller'] == Buying_controller]).iloc[0]['high_cutoff']
        
        #### Cannibalization percentage
        sim_prod_subset = sim_prod_new[sim_prod_new['final_score']>0.8]
        if len(sim_prod_subset)==0:
            if week_flag =='Latest 13 Weeks':
                high_volume_cutoff = (high_cutoff/21)*13
                low_volume_cutoff = (low_cutoff/21)*13

            if week_flag =='Latest 26 Weeks':
                high_volume_cutoff = (high_cutoff/21)*26
                low_volume_cutoff = (low_cutoff/21)*26

            if week_flag =='Latest 52 Weeks':
                high_volume_cutoff = (high_cutoff/21)*52
                low_volume_cutoff = (low_cutoff/21)*52 

            if (total_forecasted_volume>high_volume_cutoff):
                Volume_flag= 'High'
            elif (total_forecasted_volume<low_volume_cutoff):
                Volume_flag = 'Low'
            else :
                Volume_flag = 'Medium'
            ####Need to import psg code to desc mapping
            if (psg_code+Volume_flag+brand_ind  in list(PSGVolBrandBuckets['bucket_value'])):                    
                Cannibalization_perc = (PSGVolBrandBuckets.loc[PSGVolBrandBuckets['bucket_value'] == psg_code+Volume_flag+brand_ind]).iloc[0]['cannibalization']
            
            elif (psg_code+Volume_flag  in list(PSGVolBuckets['bucket_value'])):
                Cannibalization_perc = (PSGVolBuckets.loc[PSGVolBuckets['bucket_value'] == psg_code+Volume_flag]).iloc[0]['cannibalization']

            elif (psg_code  in list(PSGBuckets['bucket_value'])):
                Cannibalization_perc = (PSGBuckets.loc[PSGBuckets['bucket_value'] == psg_code]).iloc[0]['cannibalization']
                
            elif (Junior_Buyer+Volume_flag+brand_ind  in list(Junior_BuyerVolBrandBuckets['bucket_value'])):
                Cannibalization_perc = (Junior_BuyerVolBrandBuckets.loc[Junior_BuyerVolBrandBuckets['bucket_value'] == Junior_Buyer+Volume_flag+brand_ind]).iloc[0]['cannibalization']

            elif (Junior_Buyer+Volume_flag  in list(Junior_BuyerVolBuckets['bucket_value'])):
                Cannibalization_perc = (Junior_BuyerVolBuckets.loc[Junior_BuyerVolBuckets['bucket_value'] == Junior_Buyer+Volume_flag]).iloc[0]['cannibalization']

            elif (Junior_Buyer  in list(Junior_BuyerBuckets['bucket_value'])):
                Cannibalization_perc = (Junior_BuyerBuckets.loc[Junior_BuyerBuckets['bucket_value'] == Junior_Buyer]).iloc[0]['cannibalization'] 
            else:
                Cannibalization_perc = 0
                
        # This should be used before only 
        #### Applying sim prod with threshold as 0.8
        if len(sim_prod_subset)>0:
            if (brand_ind=='T'):
                sim_prod_subset= sim_prod_subset.sort_values(['brand_ind','final_score', 'launch_tesco_week'], ascending=[False,False, False])
            elif (brand_ind=='B'):
                sim_prod_subset= sim_prod_subset.sort_values(['brand_ind','final_score', 'launch_tesco_week'], ascending=[True,False, False])
            Cannibalization_perc = sim_prod_subset.iloc[0,:]['cannibalization']

        ##### To get the cannibilized volume 
        total_forecasted_volume = float(total_forecasted_volume)
        Cannibalization_perc = float(Cannibalization_perc)
        forecasted_cannibilization_volume = Cannibalization_perc*total_forecasted_volume

        ##### To get the cannibilized sales
        forecasted_cannibilization_volume = float(forecasted_cannibilization_volume)
        forecasted_cannibilization_sales = forecasted_cannibilization_volume*asp

        Cannibalization_perc_sales = (forecasted_cannibilization_sales/total_forecasted_sales)

        ##### To get the net mpact in volume
        forecasted_net_impact_volume = total_forecasted_volume - forecasted_cannibilization_volume

        ##### To get the net impact in sales
        forecasted_net_impact_sales = total_forecasted_sales - forecasted_cannibilization_sales

        All_attribute_subset = All_attribute.dropna()
        All_attribute_subset = All_attribute_subset[['base_product_number','product_sub_group_description']]

        product_psg_mapping =pd.merge(product_contri_df, All_attribute_subset, left_on=['base_product_number'], right_on=['base_product_number'], how='left' )

        ## rolling volume at psg bpn level
        psg_product_contri_df = product_psg_mapping.groupby(['time_period','product_sub_group_description','base_product_number'], as_index=False).agg({'predicted_volume': sum})

        ## Subsetting above table for only the selected psg
        psg_product_contri_df = psg_product_contri_df.loc[psg_product_contri_df['product_sub_group_description']==Product_Sub_Group_Description]

        ## Taking variable from the function for getting the time frame
        psg_product_contri_df = psg_product_contri_df.loc[psg_product_contri_df['time_period']==time_frame]

        ## Getting the total volume for the selected psg and time period
        total_psg_forecasted_volume = sum(psg_product_contri_df['predicted_volume'])

        ## Getting the asp for all the bpn in product contri
        product_price_df_new = product_price_df
        product_price_df_new['asp'] = product_price_df_new['asp'].astype('float')
        
        product_price_df_new = product_price_df.groupby(['base_product_number'], as_index=False).agg({'asp': 'mean'})

        #### Doing a left join to get the asp for all bpn in product contri
        psg_product_contri_df = pd.merge(psg_product_contri_df, product_price_df_new, left_on=['base_product_number'], right_on=['base_product_number'], how='left')

        ### Getting the total sales for all the products in 
        psg_product_contri_df['predicted_volume'] = psg_product_contri_df['predicted_volume'].astype('float')
        psg_product_contri_df.loc[:,'predicted_sales'] = psg_product_contri_df['predicted_volume']*psg_product_contri_df['asp']

        ###Total would be sum of all the product sales
        total_psg_forecasted_sales = sum(psg_product_contri_df['predicted_sales'])

        #### Getting the change in % psg volume
        total_psg_forecasted_volume = float(total_psg_forecasted_volume)

        try :
            psg_perc_volume = (forecasted_net_impact_volume/total_psg_forecasted_volume)
        except:
            psg_perc_volume = 0

        #### Gettig the change in % psg sales
        total_psg_forecasted_sales = float(total_psg_forecasted_sales)
        psg_perc_sales = (forecasted_net_impact_sales/total_psg_forecasted_sales)

        data_volume = [{'forecast': total_forecasted_volume,'modified_forecast': total_forecasted_volume,
             'cannibilization_value' : forecasted_cannibilization_volume, 'cannibilization_perc' : Cannibalization_perc*100,
             'net_impact_value': forecasted_net_impact_volume , 'perc_change_in_psg' : psg_perc_volume*100,'buying_controller':
             Buying_controller ,'junior_buyer' : Junior_Buyer , 'buyer' : Buyer , 'psg' : Product_Sub_Group_Description,'asp' :
             asp , 'acp' : acp ,'size' : Size ,'package_type' : Package_Type ,'till_roll_desc': Till_Roll_Description , 'week_flag' : week_flag ,'price_band' : price_band}]

        data_sales = [{'forecast': total_forecasted_sales,'modified_forecast': total_forecasted_sales,
             'cannibilization_value' : forecasted_cannibilization_sales, 'cannibilization_perc' : Cannibalization_perc_sales*100,
             'net_impact_value': forecasted_net_impact_sales , 'perc_change_in_psg' : psg_perc_sales*100,'buying_controller':
             Buying_controller ,'junior_buyer' : Junior_Buyer , 'buyer' : Buyer , 'psg' : Product_Sub_Group_Description,'asp' :
             asp , 'acp' : acp ,'size' : Size ,'package_type' : Package_Type ,'till_roll_desc': Till_Roll_Description , 'week_flag' : week_flag,'price_band' : price_band }]

        model_output = [{'data_volume': data_volume , 'data_sales' : data_sales }]

        output_cannib = model_output
        output_cannib_volume = pd.DataFrame(output_cannib[0]['data_volume'])  ##Output 1 volume
        output_cannib_sales = pd.DataFrame(output_cannib[0]['data_sales'])  ## Output 1 Sales

        data_dict_volume={}
        data = [{
                   "name":"NPD Volume","value":int(output_cannib_volume['forecast'][0])
                   },
                {
                    "name":"Cannibalized Volume","value":-int(output_cannib_volume['cannibilization_value'][0])
                    }]

        volume={}
        volume={"Cannibilization_perc":output_cannib_volume['cannibilization_perc'][0].round(decimals=2),
                                        "perc_impact_psg":output_cannib_volume['perc_change_in_psg'][0].round(decimals=2)
                           }
        data_dict_volume["data"]=data
        data_dict_volume["impact"]=volume

        data_dict_sales={}
        data = [{
                    "name":"NPD Value","value":int(output_cannib_sales['forecast'][0])
                },
                {
                    "name":"Cannibalized Sales",
                    "value":-int(output_cannib_sales['cannibilization_value'][0])
                }]
        sales={}
        sales={
                "Cannibilization_perc" : output_cannib_sales['cannibilization_perc'][0].round(decimals=2),
                "perc_impact_psg":(output_cannib_sales['perc_change_in_psg'][0]).round(decimals=2)
               }
        data_dict_sales["data"]=data
        data_dict_sales["impact"]=sales

        volume_cann = int(output_cannib_volume['cannibilization_value'][0])
        volume_forecast = int(output_cannib_volume['forecast'][0])
        volume_impact = (int(output_cannib_volume['forecast'][0])) - (int(output_cannib_volume['cannibilization_value'][0]))

        value_cann = int(output_cannib_sales['cannibilization_value'][0])
        value_forecast = int(output_cannib_sales['forecast'][0])
        value_impact = (int(output_cannib_sales['forecast'][0])) - (int(output_cannib_sales['cannibilization_value'][0]))        

        final_output_data = {}
        final_output_data["data_dict_sales"] = data_dict_sales
        final_output_data["data_dict_volume"] = data_dict_volume
        final_output_data["volume_cann"] = volume_cann
        final_output_data["value_cann"] = value_cann
        final_output_data["volume_impact"] = volume_impact
        final_output_data["volume_forecast"] = volume_forecast
        final_output_data["value_impact"] = value_impact
        final_output_data["value_forecast"] = value_forecast
        
        return final_output_data

    def similar_product_cannabilized(self,prod_param_list,df_list,output_list):

        time_frame = output_list['time_frame']
        sim_prod = output_list['sim_prod']
        product_contri_df = df_list['product_contri_df']
        All_attribute = df_list['All_attribute']
        product_price_df = df_list['product_price_df']
        product_desc_df = df_list['product_desc_df']
        Product_Sub_Group_Description = prod_param_list['Product_Sub_Group_Description']

        sim_prod_copy = sim_prod
        sim_prod_copy = sim_prod_copy.sort_values(['final_score'], ascending = [False])
        sim_prod_copy = sim_prod_copy.head(10)
        sim_prod_product = sim_prod_copy[['base_product_number','final_score']]
        time_frame = time_frame
        #### GETTING ONLY THOSE bpn WHICH WERE THERE IN SIM PROD (THRESHOLD OF 0.8)
        product_contri_df_new = pd.merge(product_contri_df,sim_prod_product,left_on=['base_product_number'], right_on=['base_product_number'],how='inner')

        All_attribute_subset = All_attribute.dropna()
        All_attribute_subset = All_attribute_subset[['base_product_number','product_sub_group_description']]
        product_psg_mapping =pd.merge(product_contri_df_new, All_attribute_subset, left_on=['base_product_number'], right_on=['base_product_number'], how='left' )

        ### rolling volume at psg bpn level
        psg_product_contri_df = product_psg_mapping.groupby(['time_period','product_sub_group_description','base_product_number','final_score'], as_index=False).agg({'predicted_volume': sum})

        ### Subsetting above table for only the selected psg
        psg_product_contri_df = psg_product_contri_df.loc[psg_product_contri_df['product_sub_group_description']==Product_Sub_Group_Description]
    
        # #### Taking variable from the function for getting the time frame
        psg_product_contri_df = psg_product_contri_df.loc[psg_product_contri_df['time_period']==time_frame]

        product_price_df_new = product_price_df
        product_price_df_new['asp'] = product_price_df_new['asp'].astype('float')
        product_price_df_new = product_price_df_new.groupby(['base_product_number'], as_index=False).agg({'asp': 'mean'})

        #### Doing a left join to get the asp for all bpn in product contri
        psg_product_contri_df = pd.merge(psg_product_contri_df, product_price_df_new, left_on=['base_product_number'], right_on=['base_product_number'], how='left')

        ### Getting the total sales for all the products in 
        psg_product_contri_df['predicted_volume'] = psg_product_contri_df['predicted_volume'].astype(int)
        psg_product_contri_df.loc[:,'predicted_sales'] = psg_product_contri_df['predicted_volume']*psg_product_contri_df['asp']
        psg_product_contri_df['predicted_sales'] = psg_product_contri_df['predicted_sales'].astype(int)
        psg_product_contri_df['final_score'] = psg_product_contri_df['final_score'].astype(float)


        product_desc_branded = pd.merge(psg_product_contri_df, product_desc_df, left_on=['base_product_number'], right_on=['base_product_number'], how='left')
        product_desc_branded = product_desc_branded.rename(columns={'final_score':'similarity_score'})
        product_desc_branded = product_desc_branded[['long_description','brand_indicator','predicted_volume','predicted_sales','similarity_score']]

        product_desc_branded['brand_indicator'] = product_desc_branded['brand_indicator'].replace('T','Own Label')
        product_desc_branded['brand_indicator'] = product_desc_branded['brand_indicator'].replace('B','Branded')

        return product_desc_branded

    def modified_forecast_cal(self,prod_param_list,df_list,output_list):
        #### for calculating data table
        print("inside modified forecast function")
        print(prod_param_list)
        asp = prod_param_list['asp']
        total_forecasted_volume = prod_param_list['total_forecasted_volume']
        Cannibalization_perc = prod_param_list['Cannibalization_perc']
        All_attribute = df_list['All_attribute']
        product_contri_df = df_list['product_contri_df']
        Product_Sub_Group_Description = prod_param_list['Product_Sub_Group_Description']
        time_frame = output_list['time_frame']
        product_price_df = df_list['product_price_df']
        Buying_controller = prod_param_list['Buying_controller']
        acp = prod_param_list['acp']
        Junior_Buyer = prod_param_list['Junior_Buyer']
        Buyer = prod_param_list['Buyer']
        Size = prod_param_list['Size']
        Package_Type = prod_param_list['Package_Type']
        Till_Roll_Description = prod_param_list['Till_Roll_Description']
        week_flag = prod_param_list['week_flag']
        price_band = output_list['price_band']

        forecasted_cannibilization_volume = float(Cannibalization_perc) * float(total_forecasted_volume)
        ##### To get the cannibilized sales
        forecasted_cannibilization_sales = forecasted_cannibilization_volume*asp

        total_forecasted_sales = float(total_forecasted_volume)*float(asp)
        Cannibalization_perc_sales = (forecasted_cannibilization_sales/total_forecasted_sales)
        ##### To get the net mpact in volume
        forecasted_net_impact_volume = total_forecasted_volume - forecasted_cannibilization_volume

        ##### To get the net impact in sales
        forecasted_net_impact_sales = total_forecasted_sales - forecasted_cannibilization_sales

        All_attribute = All_attribute.dropna()
        All_attribute_subset = All_attribute[['base_product_number','product_sub_group_description']]

        product_psg_mapping =pd.merge(product_contri_df, All_attribute_subset, left_on=['base_product_number'], right_on=['base_product_number'], how='left' )

         ## rolling volume at psg bpn level
        psg_product_contri_df = product_psg_mapping.groupby(['time_period','product_sub_group_description','base_product_number'], as_index=False).agg({'predicted_volume': sum})
        
        ### Subsetting above table for only the selected psg
        psg_product_contri_df = psg_product_contri_df.loc[psg_product_contri_df['product_sub_group_description']==Product_Sub_Group_Description]

        #### Taking variable from the function for getting the time frame
        psg_product_contri_df = psg_product_contri_df.loc[psg_product_contri_df['time_period']==time_frame]

        ###### Getting the total volume for the selected psg and time period
        total_psg_forecasted_volume = sum(psg_product_contri_df['predicted_volume'])

        ##### Getting the asp for all the bpn in product contri

        product_price_df['asp'] = product_price_df['asp'].astype('float')
        product_price_df = product_price_df.groupby(['base_product_number'], as_index=False).agg({'asp': 'mean'})

        #### Doing a left join to get the asp for all bpn in product contri

        psg_product_contri_df = pd.merge(psg_product_contri_df, product_price_df, left_on=['base_product_number'], right_on=['base_product_number'], how='left')

        ### Getting the total sales for all the products in
        psg_product_contri_df['predicted_volume'] = psg_product_contri_df['predicted_volume'].astype('float')
        psg_product_contri_df.loc[:,'predicted_sales'] = psg_product_contri_df['predicted_volume']*psg_product_contri_df['asp']
        print("psg_product_contri_df")
        print(psg_product_contri_df)

        ###Total would be sum of all the product sales
        total_psg_forecasted_sales = sum(psg_product_contri_df['predicted_sales'])
        print("total_psg_forecasted_sales")
        print(total_psg_forecasted_sales)

        #### Getting the change in % psg volume
        total_psg_forecasted_volume = float(total_psg_forecasted_volume)

        try :
            psg_perc_volume = (forecasted_net_impact_volume/total_psg_forecasted_volume)
        except:
            psg_perc_volume = 0

        #### Gettig the change in % psg sales

        total_psg_forecasted_sales = float(total_psg_forecasted_sales)
        print("total_psg_forecasted_sales")
        print(total_psg_forecasted_sales)
        try:
            psg_perc_sales = (forecasted_net_impact_sales/total_psg_forecasted_sales)
        except:
            psg_perc_sales = 0

        data_volume = [{'forecast': total_forecasted_volume,'modified_forecast': total_forecasted_volume,
             'cannibilization_value' : forecasted_cannibilization_volume, 'cannibilization_perc' : Cannibalization_perc,
             'net_impact_value': forecasted_net_impact_volume , 'perc_change_in_psg' : psg_perc_volume*100,'buying_controller':
             Buying_controller ,'junior_buyer' : Junior_Buyer , 'buyer' : Buyer , 'psg' : Product_Sub_Group_Description,'asp' :
             asp , 'acp' : acp ,'size' : Size ,'package_type' : Package_Type ,'till_roll_desc': Till_Roll_Description , 'week_flag' : week_flag ,'price_band' : price_band}]

        data_sales = [{'forecast': total_forecasted_sales,'modified_forecast': total_forecasted_sales,
             'cannibilization_value' : forecasted_cannibilization_sales, 'cannibilization_perc' : Cannibalization_perc_sales,
             'net_impact_value': forecasted_net_impact_sales , 'perc_change_in_psg' : psg_perc_sales*100,'buying_controller':
             Buying_controller ,'junior_buyer' : Junior_Buyer , 'buyer' : Buyer , 'psg' : Product_Sub_Group_Description,'asp' :
             asp , 'acp' : acp ,'size' : Size ,'package_type' : Package_Type ,'till_roll_desc': Till_Roll_Description , 'week_flag' : week_flag,'price_band' : price_band }]

        output_cannib_volume = pd.DataFrame(data_volume)  ##Output 1 volume
        output_cannib_sales = pd.DataFrame(data_sales)  ## Output 1 Sales

        data_dict_volume={}
        data = [{
                   "name":"NPD Volume","value":int(output_cannib_volume['forecast'][0])
                   },
                {
                    "name":"Cannibalized Volume","value":-int(output_cannib_volume['cannibilization_value'][0])
                    }]

        volume={}
        volume={"Cannibilization_perc":output_cannib_volume['cannibilization_perc'][0],
                                        "perc_impact_psg":output_cannib_volume['perc_change_in_psg'][0].round(decimals=2)
                           }
        data_dict_volume["data"]=data
        data_dict_volume["impact"]=volume
        data_dict_sales={}
        data = [{
                "name":"NPD Value","value":int(output_cannib_sales['forecast'][0])
                },
                {
                "name":"Cannibalized Sales",
                "value":-int(output_cannib_sales['cannibilization_value'][0])
                }]
        sales={}
        sales={
                "Cannibilization_perc" : output_cannib_sales['cannibilization_perc'][0],
                "perc_impact_psg":(output_cannib_sales['perc_change_in_psg'][0]).round(decimals=2)
               }
        data_dict_sales["data"]=data
        data_dict_sales["impact"]=sales

        volume_cann = int(output_cannib_volume['cannibilization_value'][0])
        volume_forecast = int(output_cannib_volume['forecast'][0])
        volume_impact = (int(output_cannib_volume['forecast'][0])) - (int(output_cannib_volume['cannibilization_value'][0]))
        value_cann = int(output_cannib_sales['cannibilization_value'][0])
        value_forecast = int(output_cannib_sales['forecast'][0])
        value_impact = (int(output_cannib_sales['forecast'][0])) - (int(output_cannib_sales['cannibilization_value'][0]))

        final_output_data = {}
        final_output_data["data_dict_sales"] = data_dict_sales
        final_output_data["data_dict_volume"] = data_dict_volume
        final_output_data["volume_cann"] = volume_cann
        final_output_data["value_cann"] = value_cann
        final_output_data["volume_impact"] = volume_impact
        final_output_data["volume_forecast"] = volume_forecast
        final_output_data["value_impact"] = value_impact
        final_output_data["value_forecast"] = value_forecast
        return final_output_data

# for forecast and waterfall chart
class forecast_impact(APIView):
    def get(self, request, *args):
        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}

        week=args.pop('week_flag__iexact',None)
        week_flag = week
        if week==None:
            week = 'Latest 13 Weeks'
        #define variables

        #input from header
        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        designation = args.pop('designation__iexact',None)
        user_id = args.pop('user_id__iexact',None)
        session_id = args.pop('session_id__iexact',None)
        user_name = args.pop('user_name__iexact', None)
        buying_controller_header = args.pop('buying_controller_header__iexact',None)
        buyer_header = args.pop('buyer_header__iexact',None)
        #header over

        Buying_controller = args.pop('buying_controller__iexact', '')
        Buyer = args.pop('buyer__iexact', '')
        Junior_Buyer = args.pop('junior_buyer__iexact', '')
        Package_Type = args.pop('package_type__iexact', '')
        Product_Sub_Group_Description = args.pop('product_sub_group_description__iexact', '')
        measure_type =  args.pop('measure_type__iexact', '')

        asp = float(args.pop('asp__iexact', 0))
        acp = float(args.pop('acp__iexact', 0))
        Size = float(args.pop('size__iexact', 0))
        Brand_Name = args.pop('brand_name__iexact', '')
        Till_Roll_Description =args.pop('till_roll_description__iexact', '')
        Merchandise_Group_Description = args.pop('merchandise_group_description__iexact', '')
        Range_class = args.pop('range_class__iexact', '')

        ## for edit forecast
        modified_flag = int(args.pop('modified_flag__iexact', 0))
        modified_week = args.pop('modified_week__iexact',None)
        total_forecasted_volume = float(args.pop('modified_forecast__iexact',0))
        Cannibalization_perc = float(args.pop('Cannibalization_perc__iexact',0))
        #variables defined
        #read all files
        All_attribute = read_frame(bc_allprod_attributes.objects.all())
        bc_cannibilization = read_frame(consolidated_calculated_cannibalization.objects.all())
        input_dataset = read_frame(input_npd.objects.all())
        consolidated_buckets_df = read_frame(consolidated_buckets.objects.all())
        range_space_store = read_frame(range_space_store_future.objects.all())
        store_details_df = read_frame(store_details.objects.all())
        product_contri_df = read_frame(product_contri.objects.all())
        product_price_df = read_frame(product_price.objects.all())
        week_mapping = read_frame(npd_calendar.objects.all())
        product_desc_df = read_frame(product_desc.objects.all())
        brand_grp_mapping_df = read_frame(brand_grp_mapping.objects.all())
        merch_range_df = read_frame(merch_range.objects.all())
        uk_holidays_df = read_frame(uk_holidays.objects.all())
        attribute_score = read_frame(attribute_score_allbc.objects.all())
        SI = read_frame(seasonality_index.objects.all())    
        cann_vol_bucket = read_frame(cannibalization_vol_buckets.objects.all())

        # defining dict of parameters to be passed to npd functions from user input
        prod_param_list={}
        prod_param_list={
            'Buying_controller' : Buying_controller,
            'Buyer' : Buyer,
            'Junior_Buyer' : Junior_Buyer,
            'Product_Sub_Group_Description': Product_Sub_Group_Description,
            'Range_class' : Range_class,
            'Brand_Name' : Brand_Name,
            'Merchandise_Group_Description' : Merchandise_Group_Description,
            'Package_Type' : Package_Type,
            'measure_type' : measure_type,
            'Till_Roll_Description' : Till_Roll_Description,
            'Size' : Size,
            'asp' : asp,
            'acp' : acp,
            'week_flag' : week_flag,
            'total_forecasted_volume' :total_forecasted_volume, 
            'Cannibalization_perc' : Cannibalization_perc
            }
        df_list={}
        df_list = {
            'merch_range_df': merch_range_df,
            'All_attribute' : All_attribute,
            'brand_grp_mapping_df' : brand_grp_mapping_df,
            'SI' : SI,
            'attribute_score' : attribute_score,
            'range_space_store' : range_space_store,
            'store_details_df' : store_details_df,
            'uk_holidays_df' : uk_holidays_df,
            'bc_cannibilization' : bc_cannibilization,
            'cann_vol_bucket' : cann_vol_bucket,
            'input_dataset' : input_dataset,
            'consolidated_buckets_df' : consolidated_buckets_df,
            'product_contri_df' : product_contri_df,
            'product_price_df' : product_price_df,
            'week_mapping' : week_mapping,
            'product_desc_df' : product_desc_df,
        }

        # defining list of parameters to be passed to npd functions from output
        current_month = 0
        psg_code = 0
        price_band = 0
        merch_code = 0
        time_frame=''
        dataset1 = pd.DataFrame()
        df = pd.DataFrame()
        df1 = pd.DataFrame()
        sim_prod = pd.DataFrame()
        df2 = pd.DataFrame()
        df3 = pd.DataFrame()
        df_test = pd.DataFrame()
        xg_model = pd.DataFrame()

        output_list = {}
        output_list={
            'dataset1': dataset1,
            'current_month' : current_month,
            'df' : df,
            'psg_code': psg_code,
            'price_band': price_band,
            'df1': df1,
            'sim_prod': sim_prod,
            'df2': df2,
            'df3': df3,
            'merch_code': merch_code,
            'df_test': df_test,
            'time_frame': time_frame,
            'xg_model': xg_model
        }
        # create object of npd impact functions class 
        npd_functions = npd_impact_functions()

        ### for calculating forecast
        if (modified_flag==0):

            similar_product = pd.DataFrame()

            # call function for priceband and psg code
            psg_priceband_merch = npd_functions.get_priceband_psg_merch_code(prod_param_list,df_list)
            psg_code = psg_priceband_merch['psg_code']
            price_band = psg_priceband_merch['price_band']
            merch_code = psg_priceband_merch['merch_code']

            week_mapping['year_period_number'] = pd.to_numeric(week_mapping.year_number.map(str) + week_mapping.period_number.map("{:02}".format))
            week_mapping = week_mapping.sort_values('year_week_number')
            week_mapping['calendar_date'] = pd.to_datetime(week_mapping['calendar_date'], format='%Y-%m-%d')
            today = datetime.datetime.today().strftime('%Y-%m-%d')
            current_month = (week_mapping.loc[week_mapping['calendar_date'] == today]).iloc[0]['year_period_number']

            ### Taking Columns to spread
            ### Converting buying controller to lower case and concating the name
            bc_name = (Buying_controller.replace(" ", "").lower())
            
            gzip_pickle = gzip.open("api/pickle/xg_model_" + Buying_controller + ".pkl", "rb")
            xg_model = pickle.load(gzip_pickle)
            dataset1 =pd.DataFrame(columns=xg_model.feature_names)
            output_list['xg_model'] = xg_model
            output_list['current_month'] = current_month
            output_list['dataset1'] = dataset1
            output_list['price_band'] = price_band  
            # call function for creating ads structure
            df = npd_functions.get_ads_structure(prod_param_list,df_list,output_list)

            output_list['df'] = df
            output_list['psg_code'] = psg_code 
            output_list['merch_code'] = merch_code

            # call function for filling ads structure
            df1 = npd_functions.fill_ads_structure(prod_param_list,df_list,output_list)

            # call function for finding similar products
            sim_prod = npd_functions.similar_products(prod_param_list,df_list,output_list)
            output_list['df1'] = df1
            output_list['sim_prod'] = sim_prod

            #call function for substitute calculation
            if (len(sim_prod)) > 0:
                print("call subs same diff")
                df2 = npd_functions.subs_same_different(prod_param_list,df_list,output_list)
            else:
                df2 = df1
                if 'no_of_subs_same_brand' in df2.columns:
                    df2.loc[:, 'no_of_subs_same_brand'] = 0
                if 'no_of_subs_diff_brand' in df2.columns:
                    df2.loc[:, 'no_of_subs_diff_brand'] = 0

            All_attribute_pb = All_attribute.loc[(All_attribute['price_band']== price_band)]

            # call function for product count with same psg and price band
            output_list['df2'] = df2
            df3 = npd_functions.psg_pb_prodcount(prod_param_list,df_list,output_list)
            output_list['df3'] = df3
            df_final_new = npd_functions.no_of_stores_holidays(prod_param_list,df_list,output_list)
            month_index = df_final_new[['year_period_number']].drop_duplicates().reset_index()
            month_index = month_index.rename(columns={'index':'rank'})                

            # Creating a index for the week number
            df_final_new = pd.merge(df_final_new,month_index,on=['year_period_number'],how='left')
            df_test_52weeks = df_final_new
                
            if week == 'Latest 13 Weeks':
                df_test = df_test_52weeks[df_test_52weeks['rank']<=3]
                del(df_test['year_period_number'])
                del(df_test['rank'])

                prod_param_list['week_flag'] = "Latest 13 Weeks"
                output_list['time_frame'] = "3_months"
                output_list['df_test'] = df_test

                final_output_data = npd_functions.run_cannibilization_model(prod_param_list,df_list,output_list)
                similar_product = npd_functions.similar_product_cannabilized(prod_param_list,df_list,output_list)


            elif week == 'Latest 26 Weeks':
                df_test = df_test_52weeks[df_test_52weeks['rank']<=6]
                del(df_test['year_period_number'])
                del(df_test['rank'])

                prod_param_list['week_flag'] = "Latest 26 Weeks"
                output_list['time_frame'] = "6_months"
                output_list['df_test'] = df_test            
                final_output_data = npd_functions.run_cannibilization_model(prod_param_list,df_list,output_list)
                similar_product = npd_functions.similar_product_cannabilized(prod_param_list,df_list,output_list)


            elif week == 'Latest 52 Weeks':
                df_test = df_test_52weeks
                del(df_test['year_period_number'])
                del(df_test['rank'])

                prod_param_list['week_flag'] = "Latest 52 Weeks"
                output_list['time_frame'] = "12_months"
                output_list['df_test'] = df_test
                final_output_data = npd_functions.run_cannibilization_model(prod_param_list,df_list,output_list)
                similar_product = npd_functions.similar_product_cannabilized(prod_param_list,df_list,output_list)

            similar_product_final = similar_product
            final_data_dict_sales = {}
            final_data_dict_sales = final_output_data["data_dict_sales"]
            final_data_dict_volume = {}
            final_data_dict_volume = final_output_data["data_dict_volume"]  
        
        else:
            print("modified week")
            print(modified_week)
            if prod_param_list['week_flag'] == "Latest 13 Weeks":
                output_list['time_frame'] = "3_months"    
                time_frame = "3_months"
                print("inside 13 weeks")     
                if (modified_week =="Latest 13 Weeks"):
                    total_forecasted_volume = total_forecasted_volume
                elif (modified_week =="Latest 26 Weeks"):
                    total_forecasted_volume = total_forecasted_volume/2
                elif (modified_week =="Latest 52 Weeks"):
                    total_forecasted_volume = total_forecasted_volume/4

            elif prod_param_list['week_flag'] == "Latest 26 Weeks":
                output_list['time_frame'] = "6_months"   
                time_frame = "6_months"
                print("inside 26 weeks")       
                if (modified_week =="Latest 13 Weeks"):
                    print("modified week Latest 13 Weeks")                    
                    total_forecasted_volume = total_forecasted_volume*2
                elif (modified_week =="Latest 26 Weeks"):
                    print("modified week Latest 26 Weeks")                    
                    total_forecasted_volume = total_forecasted_volume
                elif (modified_week =="Latest 52 Weeks"):
                    print("modified week Latest 52 Weeks")
                    total_forecasted_volume = total_forecasted_volume/2

            elif prod_param_list['week_flag'] == "Latest 52 Weeks":
                output_list['time_frame'] = "12_months"     
                time_frame = "12_months"
                print("inside 52 weeks")
                if (modified_week =="Latest 13 Weeks"):
                    total_forecasted_volume = total_forecasted_volume*4
                elif (modified_week =="Latest 26 Weeks"):
                    total_forecasted_volume = total_forecasted_volume*2
                elif (modified_week =="Latest 52 Weeks"):
                    total_forecasted_volume = total_forecasted_volume                

            print("calculated total forecast volume")
            print(total_forecasted_volume)
            prod_param_list['total_forecasted_volume'] = total_forecasted_volume            
            psg_priceband_merch = npd_functions.get_priceband_psg_merch_code(prod_param_list,df_list)
            price_band = psg_priceband_merch['price_band']
            psg_code = psg_priceband_merch['psg_code']
            output_list['price_band'] = price_band
            output_list['psg_code'] = psg_code

            sim_prod = npd_functions.similar_products(prod_param_list,df_list,output_list)
            output_list['sim_prod'] = sim_prod 
            similar_product = npd_functions.similar_product_cannabilized(prod_param_list,df_list,output_list)
            print("prod_param_list")
            print(prod_param_list['asp'] )
            final_output_data = npd_functions.modified_forecast_cal(prod_param_list,df_list,output_list)

            final_data_dict_sales = {}
            final_data_dict_sales = final_output_data["data_dict_sales"]
            final_data_dict_volume = {}
            final_data_dict_volume = final_output_data["data_dict_volume"]  

        data_table = {
            'df': similar_product.to_dict(orient='records')
        }

        return JsonResponse({'sales_chart':final_data_dict_sales,'volume_chart':final_data_dict_volume,'similar_product_table':data_table},safe=False)

# npd impact save scenario
class npd_save_scenario(APIView):
    def get(self, request, *args):
        #input from args
        args = {reqobj : request.GET.get(reqobj) for reqobj in request.GET.keys()}
        args.pop('format', None)
        # pop week tab
        week_selected = args.pop('week_flag',None)
        # check if forecast is modified
        modified_flag = int(args.pop('modified_flag', 0))
        modified_week = args.pop('modified_week',None)
        total_forecasted_volume = float(args.pop('modified_forecast',0))
        Cannibalization_perc = float(args.pop('Cannibalization_perc',0))

        scenario_name = args.pop('scenario_name',None)
        scenario_tag = args.pop('scenario_tag',None)
        designation = args.pop('designation',None)
        # replacing apostrophe with _*_ in scenario name and scenario tag
        scenario_name = scenario_name.replace("'","_*_")
        scenario_tag = scenario_tag.replace("'","_*_")

        user_id = args.pop('user_id',None)
        session_id = args.pop('session_id',None)
        user_name = args.pop('user_name', None)
        buying_controller_header = args.pop('buying_controller_header',None)
        buyer_header = args.pop('buyer_header',None)
        user_attributes_args = args.copy()
        user_attributes = user_attributes_args
        system_time = strftime("%Y-%m-%d" ,gmtime())

        # for overwriting
        overwrite = int(args.pop('overwrite',0))

        # to check if the scenario name exists already        
        scenario = scenario_name.lower()
        tag = scenario_tag.lower()
        check_value = str(user_id) + '_' + scenario +  '_' + tag

        x= list(SaveScenario.objects.values_list('user_id','scenario_name','scenario_tag').distinct())
        x_df = pd.DataFrame(x,columns=["user_id","scenario_name","scenario_tag"])
        x_df['scenario_name'] = x_df['scenario_name'].str.lower()
        x_df['scenario_tag'] = x_df['scenario_tag'].str.lower()
        check_list=[]
        x_df['check_list'] = x_df['user_id'] + '_' + x_df['scenario_name'] + '_' + x_df['scenario_tag']

        check_list_data = list(x_df['check_list'])

        if overwrite == 0:
            if check_value in check_list_data:
                result = "FAILURE"
            else:
                result = "SUCCESS" 
        elif overwrite ==1:
            result = "SUCCESS"

        if result == "SUCCESS":
            Buying_controller = args.pop('buying_controller', None)
            par_supp = args.pop('parent_supplier',None)
            Buyer = args.pop('buyer', None)
            Junior_Buyer = args.pop('junior_buyer', '')
            Package_Type = args.pop('package_type', '')
            Product_Sub_Group_Description = args.pop('product_sub_group_description', '')
            measure_type =  args.pop('measure_type', '')
            asp = float(args.pop('asp', 0))
            acp = float(args.pop('acp', 0))
            Size = float(args.pop('size', 0))
            Brand_Name = args.pop('brand_name', '')
            Till_Roll_Description =args.pop('till_roll_description', '')
            Merchandise_Group_Description = args.pop('merchandise_group_description', '')
            Range_class = args.pop('range_class', '')
            week_flag = args.pop('week_flag',None)
            #variables defined
            #read all files
            All_attribute = read_frame(bc_allprod_attributes.objects.all())
            bc_cannibilization = read_frame(consolidated_calculated_cannibalization.objects.all())
            input_dataset = read_frame(input_npd.objects.all())
            consolidated_buckets_df = read_frame(consolidated_buckets.objects.all())
            range_space_store = read_frame(range_space_store_future.objects.all())
            store_details_df = read_frame(store_details.objects.all())
            product_contri_df = read_frame(product_contri.objects.all())
            product_price_df = read_frame(product_price.objects.all())
            week_mapping = read_frame(npd_calendar.objects.all())

            merch_range_df = read_frame(merch_range.objects.all())
            product_desc_df = read_frame(product_desc.objects.all())
            brand_grp_mapping_df = read_frame(brand_grp_mapping.objects.all())
            uk_holidays_df = read_frame(uk_holidays.objects.all())
            attribute_score = read_frame(attribute_score_allbc.objects.all())
            SI = read_frame(seasonality_index.objects.all())    
            cann_vol_bucket = read_frame(cannibalization_vol_buckets.objects.all())

            # defining dict of parameters to be passed to npd functions from user input
            prod_param_list={}
            prod_param_list={
                'Buying_controller' : Buying_controller,
                'Buyer' : Buyer,
                'Junior_Buyer' : Junior_Buyer,
                'Product_Sub_Group_Description': Product_Sub_Group_Description,
                'Range_class' : Range_class,
                'Brand_Name' : Brand_Name,
                'Merchandise_Group_Description' : Merchandise_Group_Description,
                'Package_Type' : Package_Type,
                'measure_type' : measure_type,
                'Till_Roll_Description' : Till_Roll_Description,
                'Size' : Size,
                'asp' : asp,
                'acp' : acp,
                'week_flag' : week_flag,
                'total_forecasted_volume' :total_forecasted_volume, 
                'Cannibalization_perc' : Cannibalization_perc                
            }
            df_list={}
            df_list = {
                'merch_range_df': merch_range_df,
                'All_attribute' : All_attribute,
                'brand_grp_mapping_df' : brand_grp_mapping_df,
                'SI' : SI,
                'attribute_score' : attribute_score,
                'range_space_store' : range_space_store,
                'store_details_df' : store_details_df,
                'uk_holidays_df' : uk_holidays_df,
                'bc_cannibilization' : bc_cannibilization,
                'cann_vol_bucket' : cann_vol_bucket,
                'input_dataset' : input_dataset,
                'consolidated_buckets_df' : consolidated_buckets_df,
                'product_contri_df' : product_contri_df,
                'product_price_df' : product_price_df,
                'week_mapping' : week_mapping,
                'product_desc_df' : product_desc_df,
            }

            # defining list of parameters to be passed to npd functions from output
            current_month = 0
            psg_code = 0
            price_band = 0
            merch_code = 0
            time_frame=''
            dataset1 = pd.DataFrame()
            df = pd.DataFrame()
            df1 = pd.DataFrame()
            sim_prod = pd.DataFrame()
            df2 = pd.DataFrame()
            df3 = pd.DataFrame()
            df_test = pd.DataFrame()
            xg_model = pd.DataFrame()

            output_list = {}
            output_list={
                'dataset1': dataset1,
                'current_month' : current_month,
                'df' : df,
                'psg_code': psg_code,
                'price_band': price_band,
                'df1': df1,
                'sim_prod': sim_prod,
                'df2': df2,
                'df3': df3,
                'merch_code': merch_code,
                'df_test': df_test,
                'time_frame': time_frame,
                'xg_model': xg_model
            }

            # create object of npd impact functions class 
            npd_functions = npd_impact_functions()
            if (modified_flag==0):

                similar_product = pd.DataFrame()
                # call function for priceband and psg code
                psg_priceband_merch = npd_functions.get_priceband_psg_merch_code(prod_param_list,df_list)
                price_band = psg_priceband_merch['price_band']
                psg_code = psg_priceband_merch['psg_code']
                merch_code = psg_priceband_merch['merch_code']
                
                week_mapping['year_period_number'] = pd.to_numeric(week_mapping.year_number.map(str) + week_mapping.period_number.map("{:02}".format))
                week_mapping = week_mapping.sort_values('year_week_number')
                week_mapping['calendar_date'] = pd.to_datetime(week_mapping['calendar_date'], format='%Y-%m-%d')
                today = datetime.datetime.today().strftime('%Y-%m-%d')
                current_month = (week_mapping.loc[week_mapping['calendar_date'] == today]).iloc[0]['year_period_number']

                ### Taking Columns to spread
                ### Converting buying controller to lower case and concating the name
                bc_name = (Buying_controller.replace(" ", "").lower())
                
                gzip_pickle = gzip.open("api/pickle/xg_model_" + Buying_controller + ".pkl", "rb")
                xg_model = pickle.load(gzip_pickle)
                dataset1 =pd.DataFrame(columns=xg_model.feature_names)
                output_list['xg_model'] = xg_model
                output_list['current_month'] = current_month
                output_list['dataset1'] = dataset1
                output_list['price_band'] = price_band  
                output_list['psg_code'] = psg_code 
                output_list['merch_code'] = merch_code
                # call function for creating ads structure
                df = npd_functions.get_ads_structure(prod_param_list,df_list,output_list)
                output_list['df'] = df

                # call function for filling ads structure
                df1 = npd_functions.fill_ads_structure(prod_param_list,df_list,output_list)
                output_list['df1'] = df1
                # call function for finding similar products
                sim_prod = npd_functions.similar_products(prod_param_list,df_list,output_list)
                output_list['sim_prod'] = sim_prod                
                #call function for substitute calculation
                if (len(sim_prod)) > 0:
                    df2 = npd_functions.subs_same_different(prod_param_list,df_list,output_list)
                else:
                    df2 = df1
                    if 'no_of_subs_same_brand' in df2.columns:
                        df2.loc[:, 'no_of_subs_same_brand'] = 0
                    if 'no_of_subs_diff_brand' in df2.columns:
                        df2.loc[:, 'no_of_subs_diff_brand'] = 0

                All_attribute_pb = All_attribute.loc[(All_attribute['price_band']== price_band)]

                # call function for product count with same psg and price band
                output_list['df2'] = df2
                df3 = npd_functions.psg_pb_prodcount(prod_param_list,df_list,output_list)
                output_list['df3'] = df3
                df_final_new = npd_functions.no_of_stores_holidays(prod_param_list,df_list,output_list)
                month_index = df_final_new[['year_period_number']].drop_duplicates().reset_index()
                month_index = month_index.rename(columns={'index':'rank'})                

                # Creating a index for the week number
                df_final_new = pd.merge(df_final_new,month_index,on=['year_period_number'],how='left')
                df_test_52weeks = df_final_new
                
                df_test_13weeks = df_test_52weeks[df_test_52weeks['rank']<=3]
                df_test_26weeks = df_test_52weeks[df_test_52weeks['rank']<=6]

                del(df_test_13weeks['year_period_number'])
                del(df_test_13weeks['rank'])

                del(df_test_26weeks['year_period_number'])
                del(df_test_26weeks['rank'])

                del(df_test_52weeks['year_period_number'])
                del(df_test_52weeks['rank'])

                # call functions for 13 weeks 
                prod_param_list['week_flag'] = "Latest 13 Weeks"
                output_list['time_frame'] = "3_months"
                output_list['df_test'] = df_test_13weeks
                output_cannib_13weeks = npd_functions.run_cannibilization_model(prod_param_list,df_list,output_list)
                #for similar products table 13 weeks
                similar_product_13weeks = npd_functions.similar_product_cannabilized(prod_param_list,df_list,output_list)
                data_13weeks_table = {'df': similar_product_13weeks.to_dict(orient='records')}                
                
                # call functions for 26 weeks 
                prod_param_list['week_flag'] = "Latest 26 Weeks"
                output_list['time_frame'] = "6_months"
                output_list['df_test'] = df_test_26weeks                
                output_cannib_26weeks = npd_functions.run_cannibilization_model(prod_param_list,df_list,output_list)                
                similar_product_26weeks = npd_functions.similar_product_cannabilized(prod_param_list,df_list,output_list)
                data_26weeks_table = {'df': similar_product_26weeks.to_dict(orient='records')}

                # call functions for 52 weeks 
                prod_param_list['week_flag'] = "Latest 52 Weeks"
                output_list['time_frame'] = "12_months"
                output_list['df_test'] = df_test_52weeks                
                output_cannib_52weeks = npd_functions.run_cannibilization_model(prod_param_list,df_list,output_list)
                similar_product_52weeks = npd_functions.similar_product_cannabilized(prod_param_list,df_list,output_list)
                data_52weeks_table = {'df': similar_product_52weeks.to_dict(orient='records')}

                final_data_dict_sales_13 = {}
                final_data_dict_sales_13 = output_cannib_13weeks["data_dict_sales"]
                final_data_dict_volume_13 = {}
                final_data_dict_volume_13 = output_cannib_13weeks["data_dict_volume"]          
                impact_data_13weeks = {'sales_chart': final_data_dict_sales_13,'volume_chart': final_data_dict_volume_13 }
                value_forecast_13 = output_cannib_13weeks["value_forecast"]
                value_impact_13 = output_cannib_13weeks["value_impact"]
                value_cann_13 = output_cannib_13weeks["value_cann"]
                volume_forecast_13 = output_cannib_13weeks["volume_forecast"]
                volume_impact_13 = output_cannib_13weeks["volume_impact"]
                volume_cann_13 = output_cannib_13weeks["volume_cann"]

                if overwrite ==1:
                    SaveScenario.objects.filter(user_id =user_id,scenario_tag = scenario_tag,scenario_name = scenario_name,week_tab=13).delete()
                save_scenario = SaveScenario(user_id = user_id,
                                            user_name = user_name,
                                            designation = designation,
                                            session_id = session_id,
                                            scenario_name = scenario_name,
                                            scenario_tag = scenario_tag,
                                            asp = asp,
                                            week_tab = 13,
                                            buying_controller = Buying_controller,
                                            buyer = Buyer,
                                            parent_supplier = par_supp,
                                            user_attributes = user_attributes,
                                            forecast_data = impact_data_13weeks,
                                            value_forecast = value_forecast_13,
                                            value_impact = value_impact_13,
                                            value_cannibalized = value_cann_13,
                                            volume_forecast = volume_forecast_13,
                                            volume_impact = volume_impact_13,
                                            volume_cannibalized = volume_cann_13,
                                            similar_products = data_13weeks_table,
                                            modified_flag = modified_flag,
                                            system_time = system_time,
                                            page = "npd")
                save_scenario.save()
                save_scenario_tracker = ScenarioTracker(user_id = user_id,
                                            user_name = user_name,
                                            designation = designation,
                                            session_id = session_id,
                                            scenario_name = scenario_name,
                                            scenario_tag = scenario_tag,
                                            asp = asp,
                                            week_tab = 13,
                                            buying_controller = Buying_controller,
                                            buyer = Buyer,
                                            parent_supplier = par_supp,
                                            user_attributes = user_attributes,
                                            forecast_data = impact_data_13weeks,
                                            value_forecast = value_forecast_13,
                                            value_impact = value_impact_13,
                                            value_cannibalized = value_cann_13,
                                            volume_forecast = volume_forecast_13,
                                            volume_impact = volume_impact_13,
                                            volume_cannibalized = volume_cann_13,
                                            similar_products = data_13weeks_table,
                                            modified_flag = modified_flag,
                                            system_time = system_time,
                                            page = "npd")
                save_scenario_tracker.save()            

                final_data_dict_sales_26 = {}
                final_data_dict_sales_26 = output_cannib_26weeks["data_dict_sales"]
                final_data_dict_volume_26 = {}
                final_data_dict_volume_26 = output_cannib_26weeks["data_dict_volume"]  
                impact_data_26weeks = {'sales_chart': final_data_dict_sales_26,'volume_chart': final_data_dict_volume_26 }

                value_forecast_26 = output_cannib_26weeks["value_forecast"]
                value_impact_26 = output_cannib_26weeks["value_impact"]
                value_cann_26 = output_cannib_26weeks["value_cann"]
                volume_forecast_26 = output_cannib_26weeks["volume_forecast"]
                volume_impact_26 = output_cannib_26weeks["volume_impact"]
                volume_cann_26 = output_cannib_26weeks["volume_cann"]  

                if overwrite ==1:
                    SaveScenario.objects.filter(user_id =user_id,scenario_tag = scenario_tag,scenario_name = scenario_name,week_tab=26).delete()
                
                save_scenario = SaveScenario(user_id = user_id,
                                            user_name = user_name,
                                            designation = designation,
                                            session_id = session_id,
                                            scenario_name = scenario_name,
                                            scenario_tag = scenario_tag,
                                            asp = asp,
                                            week_tab = 26,
                                            buying_controller = Buying_controller,
                                            buyer = Buyer,
                                            parent_supplier = par_supp,
                                            user_attributes = user_attributes,
                                            forecast_data = impact_data_26weeks,
                                            value_forecast = value_forecast_26,
                                            value_impact = value_impact_26,
                                            value_cannibalized = value_cann_26,
                                            volume_forecast = volume_forecast_26,
                                            volume_impact = volume_impact_26,
                                            volume_cannibalized = volume_cann_26,
                                            similar_products = data_26weeks_table,
                                            modified_flag = modified_flag,
                                            system_time = system_time,
                                            page = "npd")
                save_scenario.save()

                save_scenario_tracker = ScenarioTracker(user_id = user_id,
                                            user_name = user_name,
                                            designation = designation,
                                            session_id = session_id,
                                            scenario_name = scenario_name,
                                            scenario_tag = scenario_tag,
                                            asp = asp,
                                            week_tab = 26,
                                            buying_controller = Buying_controller,
                                            buyer = Buyer,
                                            parent_supplier = par_supp,
                                            user_attributes = user_attributes,
                                            forecast_data = impact_data_26weeks,
                                            value_forecast = value_forecast_26,
                                            value_impact = value_impact_26,
                                            value_cannibalized = value_cann_26,
                                            volume_forecast = volume_forecast_26,
                                            volume_impact = volume_impact_26,
                                            volume_cannibalized = volume_cann_26,
                                            similar_products = data_26weeks_table,
                                            modified_flag = modified_flag,
                                            system_time = system_time,
                                            page = "npd")
                save_scenario_tracker.save()

                print("saved for 26 weeks ")

                final_data_dict_sales_52 = {}
                final_data_dict_sales_52 = output_cannib_52weeks["data_dict_sales"]
                final_data_dict_volume_52 = {}
                final_data_dict_volume_52 = output_cannib_52weeks["data_dict_volume"]  
                impact_data_52weeks = {'sales_chart': final_data_dict_sales_52,'volume_chart': final_data_dict_volume_52 }

                value_forecast_52 = output_cannib_52weeks["value_forecast"]
                value_impact_52 = output_cannib_52weeks["value_impact"]
                value_cann_52 = output_cannib_52weeks["value_cann"]
                volume_forecast_52 = output_cannib_52weeks["volume_forecast"]
                volume_impact_52 = output_cannib_52weeks["volume_impact"]
                volume_cann_52 = output_cannib_52weeks["volume_cann"]            
                if overwrite ==1:
                    SaveScenario.objects.filter(user_id =user_id,scenario_tag = scenario_tag,scenario_name = scenario_name,week_tab=52).delete()
                
                save_scenario = SaveScenario(user_id = user_id,
                                            user_name = user_name,
                                            designation = designation,
                                            session_id = session_id,
                                            scenario_name = scenario_name,
                                            scenario_tag = scenario_tag,
                                            asp = asp,
                                            week_tab = 52,
                                            buying_controller = Buying_controller,
                                            buyer = Buyer,
                                            parent_supplier = par_supp,
                                            user_attributes = user_attributes,
                                            forecast_data = impact_data_52weeks,
                                            value_forecast = value_forecast_52,
                                            value_impact = value_impact_52,
                                            value_cannibalized = value_cann_52,
                                            volume_forecast = volume_forecast_52,
                                            volume_impact = volume_impact_52,
                                            volume_cannibalized = volume_cann_52,
                                            similar_products = data_52weeks_table,
                                            modified_flag = modified_flag,
                                            system_time = system_time,
                                            page = "npd")
                save_scenario.save()

                save_scenario_tracker = ScenarioTracker(user_id = user_id,
                                            user_name = user_name,
                                            designation = designation,
                                            session_id = session_id,
                                            scenario_name = scenario_name,
                                            scenario_tag = scenario_tag,
                                            asp = asp,
                                            week_tab = 52,
                                            buying_controller = Buying_controller,
                                            buyer = Buyer,
                                            parent_supplier = par_supp,
                                            user_attributes = user_attributes,
                                            forecast_data = impact_data_52weeks,
                                            value_forecast = value_forecast_52,
                                            value_impact = value_impact_52,
                                            value_cannibalized = value_cann_52,
                                            volume_forecast = volume_forecast_52,
                                            volume_impact = volume_impact_52,
                                            volume_cannibalized = volume_cann_52,
                                            similar_products = data_52weeks_table,
                                            modified_flag = modified_flag,
                                            system_time = system_time,
                                            page = "npd")
                save_scenario_tracker.save()

                print("saved for 52 weeks ")

            else:
                #calculate forecast for all week tabs
                if (week_selected =="Latest 13 Weeks"):
                    total_forecasted_volume_13 = total_forecasted_volume
                    total_forecasted_volume_26 =  2 * total_forecasted_volume_13
                    total_forecasted_volume_52 = 2 * total_forecasted_volume_26

                elif (week_selected =="Latest 26 Weeks"):
                    total_forecasted_volume_26 = total_forecasted_volume
                    total_forecasted_volume_13 = total_forecasted_volume_26/2
                    total_forecasted_volume_52 = 2 * total_forecasted_volume_26

                elif (week_selected =="Latest 52 Weeks"):
                    total_forecasted_volume_52 = total_forecasted_volume
                    total_forecasted_volume_26 = total_forecasted_volume_52/2
                    total_forecasted_volume_13 = total_forecasted_volume_26/2
                #### for calculating data table
                psg_priceband_merch = npd_functions.get_priceband_psg_merch_code(prod_param_list,df_list)
                price_band = psg_priceband_merch['price_band']
                psg_code = psg_priceband_merch['psg_code']
                output_list['price_band'] = price_band
                output_list['psg_code'] = psg_code

                # for 13 weeks        
                prod_param_list['week_flag'] == "Latest 13 Weeks"
                output_list['time_frame'] = "3_months" 
                prod_param_list['total_forecasted_volume'] = total_forecasted_volume_13
                sim_prod = npd_functions.similar_products(prod_param_list,df_list,output_list)
                
                output_list['sim_prod'] = sim_prod
                similar_product = npd_functions.similar_product_cannabilized(prod_param_list,df_list,output_list)            
                data_13weeks_table = {'df': similar_product.to_dict(orient='records')}

                output_cannib_13weeks = npd_functions.modified_forecast_cal(prod_param_list,df_list,output_list)
                data_13_dict_sales = {}
                data_13_dict_sales = output_cannib_13weeks["data_dict_sales"]
                data_13_dict_volume = {}
                data_13_dict_volume = output_cannib_13weeks["data_dict_volume"]
                value_forecast_13 = output_cannib_13weeks["value_forecast"]
                value_impact_13 = output_cannib_13weeks["value_impact"]
                value_cann_13 = output_cannib_13weeks["value_cann"]
                volume_forecast_13 = output_cannib_13weeks["volume_forecast"]
                volume_impact_13 = output_cannib_13weeks["volume_impact"]
                volume_cann_13 = output_cannib_13weeks["volume_cann"]
                impact_data_13weeks = {'sales_chart': data_13_dict_sales,'volume_chart': data_13_dict_volume}
                if overwrite ==1:
                    SaveScenario.objects.filter(user_id =user_id,scenario_tag = scenario_tag,scenario_name = scenario_name,week_tab=52).delete()

                save_scenario = SaveScenario(user_id = user_id,
                                            user_name = user_name,
                                            designation = designation,
                                            session_id = session_id,
                                            scenario_name = scenario_name,
                                            scenario_tag = scenario_tag,
                                            asp = asp,
                                            week_tab = 13,
                                            buying_controller = Buying_controller,
                                            buyer = Buyer,
                                            parent_supplier = par_supp,
                                            user_attributes = user_attributes,
                                            forecast_data = impact_data_13weeks,
                                            value_forecast = value_forecast_13,
                                            value_impact = value_impact_13,
                                            value_cannibalized = value_cann_13,
                                            volume_forecast = volume_forecast_13,
                                            volume_impact = volume_impact_13,
                                            volume_cannibalized = volume_cann_13,
                                            similar_products = data_13weeks_table,
                                            modified_flag = modified_flag,
                                            system_time = system_time,
                                            page = "npd")
                save_scenario.save()

                save_scenario_tracker = ScenarioTracker(user_id = user_id,
                                            user_name = user_name,
                                            designation = designation,
                                            session_id = session_id,
                                            scenario_name = scenario_name,
                                            scenario_tag = scenario_tag,
                                            asp = asp,
                                            week_tab = 13,
                                            buying_controller = Buying_controller,
                                            buyer = Buyer,
                                            parent_supplier = par_supp,
                                            user_attributes = user_attributes,
                                            forecast_data = impact_data_13weeks,
                                            value_forecast = value_forecast_13,
                                            value_impact = value_impact_13,
                                            value_cannibalized = value_cann_13,
                                            volume_forecast = volume_forecast_13,
                                            volume_impact = volume_impact_13,
                                            volume_cannibalized = volume_cann_13,
                                            similar_products = data_13weeks_table,
                                            modified_flag = modified_flag,
                                            system_time = system_time,
                                            page = "npd")
                save_scenario_tracker.save()

                ##for 26 weeks
                prod_param_list['week_flag'] == "Latest 26 Weeks"
                output_list['time_frame'] = "6_months"
                prod_param_list['total_forecasted_volume'] = total_forecasted_volume_26
                sim_prod = npd_functions.similar_products(prod_param_list,df_list,output_list)
                data_26weeks_table = {'df': similar_product.to_dict(orient='records')}

                output_list['sim_prod'] = sim_prod
                similar_product = npd_functions.similar_product_cannabilized(prod_param_list,df_list,output_list)            
                data_26weeks_table = {'df': similar_product.to_dict(orient='records')}

                output_cannib_26weeks = npd_functions.modified_forecast_cal(prod_param_list,df_list,output_list)
                data_26_dict_sales = {}
                data_26_dict_sales = output_cannib_26weeks["data_dict_sales"]
                data_26_dict_volume = {}
                data_26_dict_volume = output_cannib_26weeks["data_dict_volume"]
                value_forecast_26 = output_cannib_26weeks["value_forecast"]
                value_impact_26 = output_cannib_26weeks["value_impact"]
                value_cann_26 = output_cannib_26weeks["value_cann"]
                volume_forecast_26 = output_cannib_26weeks["volume_forecast"]
                volume_impact_26 = output_cannib_26weeks["volume_impact"]
                volume_cann_26 = output_cannib_26weeks["volume_cann"]
                impact_data_26weeks = {'sales_chart': data_26_dict_sales,'volume_chart': data_26_dict_volume}
                if overwrite ==1:
                    SaveScenario.objects.filter(user_id =user_id,scenario_tag = scenario_tag,scenario_name = scenario_name,week_tab=52).delete()

                save_scenario = SaveScenario(user_id = user_id,
                                            user_name = user_name,
                                            designation = designation,
                                            session_id = session_id,
                                            scenario_name = scenario_name,
                                            scenario_tag = scenario_tag,
                                            asp = asp,
                                            week_tab = 26,
                                            buying_controller = Buying_controller,
                                            buyer = Buyer,
                                            parent_supplier = par_supp,
                                            user_attributes = user_attributes,
                                            forecast_data = impact_data_26weeks,
                                            value_forecast = value_forecast_26,
                                            value_impact = value_impact_26,
                                            value_cannibalized = value_cann_26,
                                            volume_forecast = volume_forecast_26,
                                            volume_impact = volume_impact_26,
                                            volume_cannibalized = volume_cann_26,
                                            similar_products = data_26weeks_table,
                                            modified_flag = modified_flag,
                                            system_time = system_time,
                                            page = "npd")
                save_scenario.save()

                save_scenario_tracker = ScenarioTracker(user_id = user_id,
                                            user_name = user_name,
                                            designation = designation,
                                            session_id = session_id,
                                            scenario_name = scenario_name,
                                            scenario_tag = scenario_tag,
                                            asp = asp,
                                            week_tab = 26,
                                            buying_controller = Buying_controller,
                                            buyer = Buyer,
                                            parent_supplier = par_supp,
                                            user_attributes = user_attributes,
                                            forecast_data = impact_data_26weeks,
                                            value_forecast = value_forecast_26,
                                            value_impact = value_impact_26,
                                            value_cannibalized = value_cann_26,
                                            volume_forecast = volume_forecast_26,
                                            volume_impact = volume_impact_26,
                                            volume_cannibalized = volume_cann_26,
                                            similar_products = data_26weeks_table,
                                            modified_flag = modified_flag,
                                            system_time = system_time,
                                            page = "npd")
                save_scenario_tracker.save()

                ##for 52 weeks
                prod_param_list['week_flag'] == "Latest 52 Weeks"
                output_list['time_frame'] = "12_months"                                
                prod_param_list['total_forecasted_volume'] = total_forecasted_volume_52
                similar_product = npd_functions.similar_product_cannabilized(prod_param_list,df_list,output_list)
                data_52weeks_table = {'df': similar_product.to_dict(orient='records')}

                output_list['sim_prod'] = sim_prod
                similar_product = npd_functions.similar_product_cannabilized(prod_param_list,df_list,output_list)            
                data_52weeks_table = {'df': similar_product.to_dict(orient='records')}

                output_cannib_52weeks = npd_functions.modified_forecast_cal(prod_param_list,df_list,output_list)
                data_52_dict_sales = {}
                data_52_dict_sales = output_cannib_52weeks["data_dict_sales"]
                data_52_dict_volume = {}
                data_52_dict_volume = output_cannib_52weeks["data_dict_volume"]
                value_forecast_52 = output_cannib_52weeks["value_forecast"]
                value_impact_52 = output_cannib_52weeks["value_impact"]
                value_cann_52 = output_cannib_52weeks["value_cann"]
                volume_forecast_52 = output_cannib_52weeks["volume_forecast"]
                volume_impact_52 = output_cannib_52weeks["volume_impact"]
                volume_cann_52 = output_cannib_52weeks["volume_cann"]
                impact_data_52weeks = {'sales_chart': data_52_dict_sales,'volume_chart': data_52_dict_volume}
                if overwrite ==1:
                    SaveScenario.objects.filter(user_id =user_id,scenario_tag = scenario_tag,scenario_name = scenario_name,week_tab=52).delete()

                save_scenario = SaveScenario(user_id = user_id,
                                            user_name = user_name,
                                            designation = designation,
                                            session_id = session_id,
                                            scenario_name = scenario_name,
                                            scenario_tag = scenario_tag,
                                            asp = asp,
                                            week_tab = 52,
                                            buying_controller = Buying_controller,
                                            buyer = Buyer,
                                            parent_supplier = par_supp,
                                            user_attributes = user_attributes,
                                            forecast_data = impact_data_52weeks,
                                            value_forecast = value_forecast_52,
                                            value_impact = value_impact_52,
                                            value_cannibalized = value_cann_52,
                                            volume_forecast = volume_forecast_52,
                                            volume_impact = volume_impact_52,
                                            volume_cannibalized = volume_cann_52,
                                            similar_products = data_52weeks_table,
                                            modified_flag = modified_flag,
                                            system_time = system_time,
                                            page = "npd")
                save_scenario.save()

                save_scenario_tracker = ScenarioTracker(user_id = user_id,
                                            user_name = user_name,
                                            designation = designation,
                                            session_id = session_id,
                                            scenario_name = scenario_name,
                                            scenario_tag = scenario_tag,
                                            asp = asp,
                                            week_tab = 52,
                                            buying_controller = Buying_controller,
                                            buyer = Buyer,
                                            parent_supplier = par_supp,
                                            user_attributes = user_attributes,
                                            forecast_data = impact_data_52weeks,
                                            value_forecast = value_forecast_52,
                                            value_impact = value_impact_52,
                                            value_cannibalized = value_cann_52,
                                            volume_forecast = volume_forecast_52,
                                            volume_impact = volume_impact_52,
                                            volume_cannibalized = volume_cann_52,
                                            similar_products = data_52weeks_table,
                                            modified_flag = modified_flag,
                                            system_time = system_time,
                                            page = "npd")
                save_scenario_tracker.save()

        return JsonResponse({"save_scenario" : result}, safe = False)

# npd impact list scenario
class npd_list_scenario(APIView):
    def get(self,request,format=None):
        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        designation = args.pop('designation__iexact',None)
        session_id = args.pop('session_id__iexact',None)
        user_name = args.pop('user_name__iexact', None)
        buying_controller_header = args.pop('buying_controller_header__iexact',None)
        buyer_header = args.pop('buyer_header__iexact',None)
        user_id = args.get('user_id__iexact',None)
        delete_row = args.pop('delete__iexact',0)

        if delete_row==0:
            queryset = SaveScenario.objects.filter(user_id=user_id).values('system_time','scenario_name','scenario_tag').distinct().order_by('-system_time')
            serializer_class = npd_SaveScenarioSerializer(queryset,many=True)
        else:
            SaveScenario.objects.filter(**args).delete()
            queryset = SaveScenario.objects.filter(user_id=user_id).values('system_time','scenario_name','scenario_tag').distinct().order_by('-system_time')
            serializer_class = npd_SaveScenarioSerializer(queryset,many=True)

        return JsonResponse(serializer_class.data,safe=False)

# npd impact view scenario
class npd_view_scenario(APIView):
    def get(self,request,format=None):
        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}

        designation = args.pop('designation__iexact',None)
        session_id = args.pop('session_id__iexact',None)
        user_name = args.pop('user_name__iexact', None)
        buying_controller_header = args.pop('buying_controller_header__iexact',None)
        buyer_header = args.pop('buyer_header__iexact',None)
        user_id = args.get('user_id__iexact',None)

        scenario_name = args.get('scenario_name__iexact',None)
        scenario_tag = args.get('scenario_tag__iexact',None)

        user_attributes = read_frame(SaveScenario.objects.filter(**args).filter(week_tab=13).values('user_attributes'))
        queryset_13_pd = read_frame(SaveScenario.objects.filter(**args).filter(week_tab=13).values('forecast_data','similar_products'))
        queryset_26_pd = read_frame(SaveScenario.objects.filter(**args).filter(week_tab=26).values('forecast_data','similar_products'))
        queryset_52_pd = read_frame(SaveScenario.objects.filter(**args).filter(week_tab=52).values('forecast_data','similar_products'))
        week_13 = {}
        week_13 = {
            "forecast_data" : queryset_13_pd['forecast_data'][0],
            "similar_products" : queryset_13_pd['similar_products'][0]
        }

        week_26 = {}
        week_26 = {
            "forecast_data" : queryset_26_pd['forecast_data'][0],
            "similar_products" : queryset_26_pd['similar_products'][0]
        }
        week_52={}
        week_52 = {
            "forecast_data" : queryset_52_pd['forecast_data'][0],
            "similar_products" : queryset_52_pd['similar_products'][0]
        }

        if (user_id is None)|(scenario_name is None)|(scenario_tag is None):
            scenario_data_dict = { }
        else:
            scenario_data_dict = {
                    "user_id" : user_id,
                    "scenario_name" : scenario_name,
                    "scenario_tag" : scenario_tag,
                    "user_attributes" : user_attributes['user_attributes'][0],
                    "week_13" : week_13,
                    "week_26" : week_26,
                    "week_52" : week_52,
            }

        return JsonResponse(scenario_data_dict,safe=False)

# for bubble chart
class supplier_performance_chart(APIView):
    def get(self, request, *args):

        #input from header
        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        designation = args.pop('designation__iexact',None)
        user_id = args.pop('user_id__iexact',None)
        session_id = args.pop('session_id__iexact',None)
        user_name = args.pop('user_name__iexact', None)
        buying_controller_header = args.pop('buying_controller_header__iexact',None)
        buyer_header = args.pop('buyer_header__iexact',None)
        #header over

        #input from args
        par_supp = args.get('parent_supplier__iexact')
        bc_name = args.get('buying_controller__iexact')

        modified_week = args.pop('modified_week',None)
        #pop edit forecast variables
        args.pop('modified_flag', 0)
        args.pop('modified_forecast__iexact',0)
        args.pop('Cannibalization_perc__iexact',0)
        week={}
        week["time_period__iexact"]=args.pop('week_flag__iexact',None)
        if week["time_period__iexact"]==None:
                        week = {
                            'time_period__iexact': 'Last 13 Weeks'
                        }
        if week["time_period__iexact"]=="Latest 13 Weeks":
                        week = {
                            'time_period__iexact': 'Last 13 Weeks'
                        }
        if week["time_period__iexact"]=="Latest 26 Weeks":
                        week = {
                            'time_period__iexact': 'Last 26 Weeks'
                        }

        if week["time_period__iexact"]=="Latest 52 Weeks":
                        week = {
                            'time_period__iexact': 'Last 52 Weeks'
                        }

        args.pop('search__iexact', '')
        kwargs = {
            'buying_controller': bc_name ,
            'parent_supplier': par_supp
                    }
        kwargs = dict(filter(lambda item: item[1] is not None, kwargs.items()))

        data = read_frame(
            npd_supplier_ads.objects.filter(**kwargs).filter(**week).values('long_description', 'cps_quartile', 'pps_quartile',
                                                           'rate_of_sale','performance_quartile','cps'))                                                        
        data = data[data["rate_of_sale"]>0].reset_index(drop=True)
        data["cps_quartile"] = data["cps_quartile"].astype('float')
        data["pps_quartile"] = data["pps_quartile"].astype('float')
        data = data[data["cps"]>0].reset_index(drop=True)

        final_bubble_list = []
        final_bubble = []
        for i in range(0, len(data)):
            bubble_list = {}
            bubble_list = {
                "long_description": data["long_description"][i],
                "rate_of_sale": data["rate_of_sale"][i],
                "cps_quartile": data["cps_quartile"][i],
                "pps_quartile": data["pps_quartile"][i],
                "performance_quartile": data["performance_quartile"][i]
            }
            final_bubble.append(bubble_list)

        return JsonResponse(final_bubble, safe=False)

# for table 
class supplier_performance_table(APIView):
    def get(self, request, *args):

        #input from header
        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        designation = args.pop('designation__iexact',None)
        user_id = args.pop('user_id__iexact',None)
        session_id = args.pop('session_id__iexact',None)
        user_name = args.pop('user_name__iexact', None)
        buying_controller_header = args.pop('buying_controller_header__iexact',None)
        buyer_header = args.pop('buyer_header__iexact',None)
        #header over
        #input from args
        par_supp = args.get('parent_supplier__iexact')
        bc_name = args.get('buying_controller__iexact')
        modified_week = args.pop('modified_week',None)

        #pop edit forecast variables
        args.pop('modified_flag', 0)
        args.pop('modified_forecast__iexact',0)
        args.pop('Cannibalization_perc__iexact',0)

        ## week tab
        week={}
        week["time_period__iexact"]=args.pop('week_flag__iexact',None)
        if week["time_period__iexact"]==None:
                        week = {
                            'time_period__iexact': 'Last 13 Weeks'
                        }
        if week["time_period__iexact"]=="Latest 13 Weeks":
                        week = {
                            'time_period__iexact': 'Last 13 Weeks'
                        }
        if week["time_period__iexact"]=="Latest 26 Weeks":
                        week = {
                            'time_period__iexact': 'Last 26 Weeks'
                        }

        if week["time_period__iexact"]=="Latest 52 Weeks":
                        week = {
                            'time_period__iexact': 'Last 52 Weeks'
                        }

        kwargs = {
            'buying_controller': bc_name ,
            'parent_supplier': par_supp
                    }
        kwargs = dict(filter(lambda item: item[1] is not None, kwargs.items()))

        queryset = npd_supplier_ads.objects.filter(**kwargs).filter(**week)
        serializer_class = npd_impact_tableSerializer(queryset, many=True)
        return JsonResponse({'table': serializer_class.data}, safe=False)