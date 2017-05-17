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

#Models for negotiation
from .models import nego_ads_drf
# Models for Product Impact
from .models import product_hierarchy,product_impact_filter, pps_ros_quantile, shelf_review_subs, prod_similarity_subs, product_price, cts_data , supplier_share, product_contri, product_desc

# Models for Product Impact Save Scenario
from .models import delist_scenario

# Serializers Negotiation
from .serializers import negochartsSerializer

# Serializers NPD Imapct Save Scenario
from .serializers import delist_savescenarioserializer

from django.core.paginator import Paginator 
import numpy as np
import gzip
import xgboost as xgb
import pickle

#for cache 
from rest_framework_extensions.cache.decorators import cache_response


## for Negotiation

#for bubble chart
class supplier_importance_chart(APIView):
    def get(self, request, *args):
        #input from header
        args_header = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}

        designation = args_header.pop('designation__iexact',None)
        user_id = args_header.pop('user_id__iexact',None)
        session_id = args_header.pop('session_id__iexact',None)
        user_name = args_header.pop('user_name__iexact', None)
        buying_controller_header = args_header.pop('buying_controller_header__iexact',None)
        buyer_header = args_header.pop('buyer_header__iexact',None)
        #header over

        if buyer_header is None:
            kwargs_header = {
                'buying_controller__iexact' : buying_controller_header
            }
        else:
            kwargs_header = {
                'buying_controller__iexact' : buying_controller_header,
                'buyer__iexact' : buyer_header
            }

        #input from args

        kwargs = {
            'store_type': 'Main Estate'
        }

        args = {reqobj + '__in': request.GET.getlist(reqobj) for reqobj in request.GET.keys()}
        args.pop('designation__in',None)
        args.pop('user_id__in',None)
        args.pop('user_name__in',None)
        args.pop('buying_controller_header__in',None)
        args.pop('buyer_header__in',None)
        args.pop('session_id__in',None)
        w=args.pop('time_period__in',None)
        week={}
        if not w:
            week = {'time_period__iexact': 'Last 13 Weeks'}
        else:
            week['time_period__iexact'] = w[0]

        if not args:
            df = read_frame(nego_ads_drf.objects.filter(**week).filter(**kwargs_header).filter(**kwargs).values('base_product_number','long_description', 'rate_of_sale','cps_quartile','pps_quartile','cps','pps','brand_indicator'))

        else:
            df = read_frame(nego_ads_drf.objects.filter(**week).filter(**kwargs_header).filter(**args).values('base_product_number','long_description', 'rate_of_sale','cps_quartile','pps_quartile','cps','pps','brand_indicator'))


        df["rate_of_sale"] = df["rate_of_sale"].astype('float')
        df["cps_quartile"] = df["cps_quartile"].astype('float')
        df["pps_quartile"] = df["pps_quartile"].astype('float')
        df["cps"] = df["cps"].astype('float')
        df["pps"] = df["pps"].astype('float')
        df["base_product_number"] = df["base_product_number"].astype('float')


        final_bubble_list = []
        final_bubble = []
        for i in range(0, len(df)):
            bubble_list = {}
            bubble_list = {
                "base_product_number": df["base_product_number"][i],
                "long_description": df["long_description"][i],
                "rate_of_sale": df["rate_of_sale"][i],
                "cps" : df["cps_quartile"][i],
                "pps" : df["pps_quartile"][i],
                "cps_value" : df["cps"][i],
                "pps_value" : df["pps"][i],
                "brand_ind" : df["brand_indicator"][i]

            }
            final_bubble.append(bubble_list)
        return JsonResponse(final_bubble, safe=False)

#for table 
class supplier_importance_table(APIView):
    def get(self, request, *args):
        #input from header
        args_header = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        designation = args_header.pop('designation__iexact',None)
        user_id = args_header.pop('user_id__iexact',None)
        session_id = args_header.pop('session_id__iexact',None)
        user_name = args_header.pop('user_name__iexact', None)
        buying_controller_header = args_header.pop('buying_controller_header__iexact',None)
        buyer_header = args_header.pop('buyer_header__iexact',None)
        #header over
        if buyer_header is None:
            kwargs_header = {
                'buying_controller__iexact' : buying_controller_header
            }
        else:
            kwargs_header = {
                'buying_controller__iexact' : buying_controller_header,
                'buyer__iexact' : buyer_header
            }
        #input from args

        kwargs = {
            'store_type': 'Main Estate'
        }
        args = {reqobj + '__in': request.GET.getlist(reqobj) for reqobj in request.GET.keys()}
        args.pop('designation__in',None)
        args.pop('user_id__in',None)
        args.pop('user_name__in',None)
        args.pop('buying_controller_header__in',None)
        args.pop('buyer_header__in',None)
        args.pop('session_id__in',None)

        ## for week tab
        week={}
        w=args.pop('time_period__in',None)
        if not w:
            week = {'time_period__iexact': 'Last 13 Weeks'}
        else:
            week['time_period__iexact'] = w[0]


        #### To include pagination feature
        page = 1
        try:
            page = int(args.get('page__in')[0])


        except:
            page = 1

        start_row = (page-1)*8
        end_row = start_row + 8

        args.pop('page__in', None)


        #### To include search feature. Applicable for only long desc
        s = args.pop('search__in',[''])
        search = s[0]



        #### Getting the products
        product = args.pop('base_product_number__in',None)

        if product is None:


            if not args :
                queryset = nego_ads_drf.objects.filter(**week).filter(**kwargs_header).filter(**kwargs).filter(long_description__icontains=search)


            else:
                queryset = nego_ads_drf.objects.filter(**week).filter(**args).filter(**kwargs_header).filter(long_description__icontains=search)
            p = Paginator(queryset, 8)

            serializer_class = negochartsSerializer(p.page(page), many=True)
            return JsonResponse({'pagination_count': p.num_pages,'page': page, 'start_index': p.page(page).start_index(),'count': p.count,'end_index': p.page(page).end_index(),'table': serializer_class.data}, safe=False)

        else:
            queryset = read_frame(nego_ads_drf.objects.filter(**kwargs_header).filter(**week).filter(**args).filter(long_description__icontains=search))
            product_df = pd.DataFrame(product,columns=['base_product_number'])
            product_df['base_product_number'] = product_df['base_product_number'].astype('int')
            product_df['checked'] = True
            queryset = pd.merge(queryset,product_df,on='base_product_number',how='left')

            queryset['checked'] = queryset['checked'].fillna(False)
            queryset = queryset.sort_values(['checked'],ascending=False)
            num_pages = math.ceil((len(queryset)/8))
            start_index = (page-1)*5+1
            count = len(queryset)
            end_index = page*5
            queryset = queryset.reset_index()
            df_new=queryset.loc[start_row:end_row,]
            df= df_new.to_dict(orient='records')

            return JsonResponse({'pagination_count': num_pages,'page': page, 'start_index': start_index,'count': count,'end_index': end_index,'table': df}, safe=False)


## for Product Impact
#### class for product page logic
class vol_transfer_logic:

    def __init__(self,bc=None,store=None,future=None,input_tpns=None,delist=None,scenario_name = None,user_id = None,user_attributes=None,chart_attr = None,delist_attr = None,supp_attr = None ):
        self.bc = None
        self.store = None
        self.future = None
        self.input_tpns = None
        self.delist = None
        self.scenario_name = None
        self.user_id = None
        self.user_attributes = None
        self.chart_attr = None
        self.supp_attr = None
        self.delist_attr = None

    def volume_transfer_logic(self,bc, store, future, input_tpns, delist):

        # Predicted volume
        join_cate_fore = read_frame(
            product_contri.objects.all().filter(buying_controller__in=bc, store_type__in=store,
                                                base_product_number__in=delist, time_period__in=future))

        # reading quantile
        pps_ros = read_frame(pps_ros_quantile.objects.all().filter(buying_controller__in=bc, store_type__in=store))

        join_cate_fore_pps = pd.merge(join_cate_fore, pps_ros[
            ['base_product_number', 'pps_ros_quantile', 'ros_quantile', 'pps_quantile']],
                                      left_on=["base_product_number"], right_on=["base_product_number"], how="left")
        join_cate_fore_pps = join_cate_fore_pps[join_cate_fore_pps.pps_ros_quantile.notnull()]

        # In[5]:

        # dunnhumby substitutes
        All_BC_subs = read_frame(
            shelf_review_subs.objects.all().filter(buying_controller__in=bc, store_type__in=store).values(
                'productcode', 'substituteproductcode', 'substitutescore', 'tcs_per', 'exclusivity_per'))

        # join predicted volume with shelf review
        join_cate_fore_SH = pd.merge(join_cate_fore_pps, All_BC_subs, left_on=['base_product_number'],
                                     right_on=['productcode'], how='left')

        # check if SR present or missing
        cate_fore_SH_present_delisted = join_cate_fore_SH[(join_cate_fore_SH.productcode.notnull())]
        cate_fore_SH_missing_delised = join_cate_fore_SH[(join_cate_fore_SH.productcode.isnull())]

        # In[7]:

        # sh is present- DD quantile/Rest

        # check if sub is getting de-listed
        check_sub_delisted = pd.merge(cate_fore_SH_present_delisted, input_tpns, left_on=['substituteproductcode'],
                                      right_on=['base_product_number'], how="left")

        check_sub_delisted['substitutescore'] = check_sub_delisted['substitutescore'].astype('float')
        delisted_subs = check_sub_delisted[(check_sub_delisted.base_product_number_y.notnull())]
        alive_subs = check_sub_delisted[(check_sub_delisted.base_product_number_y.isnull())]

        # Roll up sub scores of live subs
        grp_subs_score_live = alive_subs.groupby(['productcode'], as_index=False).agg({'substitutescore': sum})
        grp_subs_score_live = grp_subs_score_live.rename(columns={'substitutescore': 'total_subs_score_alive'})

        # Roll up sub scores of de-listed subs
        grp_subs_score_delisted = delisted_subs.groupby(['productcode'], as_index=False).agg(
            {'substitutescore': sum})
        grp_subs_score_delisted = grp_subs_score_delisted.rename(
            columns={'substitutescore': 'total_subs_score_delisted'})

        # Join the total sub scores of alive and de-listed substitutes
        join_tot_alive = pd.merge(check_sub_delisted, grp_subs_score_live, left_on=['productcode'],
                                  right_on=['productcode'], how='left')
        join_tot_delisted = pd.merge(join_tot_alive, grp_subs_score_delisted, left_on=['productcode'],
                                     right_on=['productcode'], how='left')

        # Mask all rows to 0 where the substitute has been de-listed
        join_tot_delisted['total_subs_score_alive'] = (
            np.where(join_tot_delisted['base_product_number_y'].notnull(), 0,
                     join_tot_delisted['total_subs_score_alive']))
        join_tot_delisted['total_subs_score_delisted'] = (
            np.where(join_tot_delisted['base_product_number_y'].notnull(), 0,
                     join_tot_delisted['total_subs_score_delisted']))
        join_tot_delisted['total_subs_score_delisted'] = join_tot_delisted['total_subs_score_delisted'].fillna(0)
        join_tot_delisted['subs_score_alive'] = 0
        join_tot_delisted['subs_score_alive'] = np.where(join_tot_delisted['base_product_number_y'].notnull(), 0,
                                                         join_tot_delisted['substitutescore'])

        # Calculate final substitute score with recursive transfer of substitution
        join_tot_delisted['new_sub_score'] = 0
        join_tot_delisted['subs_score_alive'] = join_tot_delisted['subs_score_alive'].fillna(0)
        join_tot_delisted['total_subs_score_delisted'] = join_tot_delisted['total_subs_score_delisted'].fillna(0)
        join_tot_delisted['total_subs_score_alive'] = join_tot_delisted['total_subs_score_alive'].fillna(0)

        join_tot_delisted['total_subs_score_alive'] = join_tot_delisted['total_subs_score_alive'].replace(
            to_replace=0, value=np.nan)
        join_tot_delisted['subs_score_alive'] = join_tot_delisted['subs_score_alive'].astype('float')
        join_tot_delisted['total_subs_score_alive'] = join_tot_delisted['total_subs_score_alive'].astype('float')
        join_tot_delisted['total_subs_score_delisted'] = join_tot_delisted['total_subs_score_delisted'].astype(
            'float')
        join_tot_delisted['new_sub_score'] = (np.where(join_tot_delisted['total_subs_score_delisted'] == 0,
                                                       join_tot_delisted['subs_score_alive'],
                                                       join_tot_delisted['subs_score_alive'] + (
                                                           join_tot_delisted['subs_score_alive'] /
                                                           join_tot_delisted[
                                                               'total_subs_score_alive']) *
                                                       join_tot_delisted['total_subs_score_delisted']))
        join_tot_delisted['new_sub_score'] = join_tot_delisted['new_sub_score'].fillna(0)
        join_tot_delisted = join_tot_delisted[
            ['base_product_number_x', 'predicted_volume', 'pps_ros_quantile', 'productcode',
             'substituteproductcode',
             'substitutescore', 'tcs_per', 'exclusivity_per', 'new_sub_score']].drop_duplicates().fillna(
            0).reset_index(drop=True)

        # In[9]:

        dd_prods = read_frame(
            nego_ads_drf.objects.all().filter(buying_controller__in=bc, store_type__in=store,
                                              performance_quartile__in=['Low CPS/Low Profit'],
                                              time_period__in=['Last 52 Weeks']).values(
                'base_product_number').distinct())

        # In[10]:

        # DD quantile products
        dd_quantile_dh_delised = join_tot_delisted[
            join_tot_delisted['base_product_number_x'].isin(dd_prods['base_product_number'])]

        dd_quantile_dh_delised = dd_quantile_dh_delised.drop_duplicates().fillna(0).reset_index(drop=True)
        dd_quantile_dh_delised_grp = dd_quantile_dh_delised.groupby(['productcode'], as_index=False).agg(
            {'new_sub_score': sum})

        dd_quantile_dh_delised_grp = dd_quantile_dh_delised_grp.rename(
            columns={'new_sub_score': 'total_new_sub_score'})
        dd_quantile_dh_delised = pd.merge(dd_quantile_dh_delised, dd_quantile_dh_delised_grp, on=['productcode'],
                                          how='left')

        dd_quantile_dh_delised['adjusted_new_sub_score'] = (
            dd_quantile_dh_delised['new_sub_score'] / dd_quantile_dh_delised['total_new_sub_score'])

        dd_quantile_dh_delised['final_sub_score'] = 0
        dd_quantile_dh_delised['tcs_per'] = dd_quantile_dh_delised['tcs_per'].fillna(0)
        dd_quantile_dh_delised['final_sub_score'] = dd_quantile_dh_delised['adjusted_new_sub_score'].astype(
            'float') * dd_quantile_dh_delised['tcs_per'].astype('float')

        dd_quantile_dh_delised['vol_transfer'] = 0
        dd_quantile_dh_delised['predicted_volume'] = dd_quantile_dh_delised['predicted_volume'].fillna(0)
        dd_quantile_dh_delised['vol_transfer'] = dd_quantile_dh_delised['predicted_volume'].astype('float') * \
                                                 dd_quantile_dh_delised['final_sub_score'].astype('float')
        vol_transfer_dataset_dd = dd_quantile_dh_delised[
            ['productcode', 'substituteproductcode', 'predicted_volume', 'vol_transfer']]
        vol_transfer_dataset_dd = vol_transfer_dataset_dd.rename(columns={'predicted_volume': 'delist_pred_vol'})

        # In[12]:

        rest_dh_delised = join_tot_delisted[
            ~join_tot_delisted['base_product_number_x'].isin(dd_prods['base_product_number'])]

        rest_dh_delised['final_sub_score'] = 0
        rest_dh_delised['tcs_per'] = rest_dh_delised['tcs_per'].fillna(0)
        rest_dh_delised['final_sub_score'] = rest_dh_delised['new_sub_score'].astype('float') * rest_dh_delised[
            'tcs_per'].astype('float')

        rest_dh_delised['vol_transfer'] = 0
        rest_dh_delised['predicted_volume'] = rest_dh_delised['predicted_volume'].fillna(0)
        rest_dh_delised['vol_transfer'] = rest_dh_delised['predicted_volume'].astype('float') * rest_dh_delised[
            'final_sub_score'].astype('float')
        vol_transfer_dataset_rest = rest_dh_delised[
            ['productcode', 'substituteproductcode', 'predicted_volume', 'vol_transfer']]
        vol_transfer_dataset_rest = vol_transfer_dataset_rest.rename(
            columns={'predicted_volume': 'delist_pred_vol'})
        vol_transfer_dataset = vol_transfer_dataset_rest.append(vol_transfer_dataset_dd)
        vol_transfer_dataset = vol_transfer_dataset[
            ['productcode', 'substituteproductcode', 'delist_pred_vol', 'vol_transfer']]

        # In[14]:

        # Prob subs
        prob_sub_score = read_frame(
            prod_similarity_subs.objects.all().filter(buying_controller__in=bc, store_type__in=store,
                                                      base_prod__in=delist).values('base_prod', 'sub_prod',
                                                                                   'actual_similarity_score',
                                                                                   'similarity_score'))
        cate_fore_SH_present_delisted = cate_fore_SH_present_delisted[
            ['base_product_number', 'predicted_volume', 'pps_ros_quantile', 'tcs_per',
             'exclusivity_per']].drop_duplicates().reset_index(drop=True).fillna(0)
        join_on_parent_prob = pd.merge(cate_fore_SH_present_delisted,
                                       prob_sub_score[['base_prod', 'sub_prod', 'actual_similarity_score']],
                                       left_on=['base_product_number'],
                                       right_on=['base_prod'], how='left')
        join_on_parent_prob = join_on_parent_prob.drop_duplicates().fillna(0).reset_index(drop=True)

        # In[15]:

        check_sub_delisted1 = pd.merge(join_on_parent_prob, input_tpns, left_on=['sub_prod'],
                                       right_on=['base_product_number'], how='left')

        # check if substitute product is getting delisted
        check_sub_delisted1['actual_similarity_score'] = check_sub_delisted1['actual_similarity_score'].astype(
            'float')
        delisted_subs1 = check_sub_delisted1[(check_sub_delisted1.base_product_number_y.notnull())]
        alive_subs1 = check_sub_delisted1[(check_sub_delisted1.base_product_number_y.isnull())]

        # Roll up sub scores of live subs
        grp_subs_score_live1 = alive_subs1.groupby(['base_prod'], as_index=False).agg(
            {'actual_similarity_score': sum})
        grp_subs_score_live1 = grp_subs_score_live1.rename(
            columns={'actual_similarity_score': 'total_subs_score_alive'})

        # Roll up sub scores of de-listed subs
        grp_subs_score_delisted1 = delisted_subs1.groupby(['base_prod'], as_index=False).agg(
            {'actual_similarity_score': sum})
        grp_subs_score_delisted1 = grp_subs_score_delisted1.rename(
            columns={'actual_similarity_score': 'total_subs_score_delisted'})

        # Join the total sub scores of alive and de-listed substitutes
        join_tot_alive1 = pd.merge(check_sub_delisted1, grp_subs_score_live1, left_on=['base_prod'],
                                   right_on=['base_prod'], how='left')
        join_tot_delisted1 = pd.merge(join_tot_alive1, grp_subs_score_delisted1, left_on=['base_prod'],
                                      right_on=['base_prod'], how='left')

        # Mask all rows to 0 where the substitute has been de-listed
        join_tot_delisted1['total_subs_score_alive'] = (
            np.where(join_tot_delisted1['base_product_number_y'].notnull(), 0,
                     join_tot_delisted1['total_subs_score_alive']))
        join_tot_delisted1['total_subs_score_delisted'] = (
            np.where(join_tot_delisted1['base_product_number_y'].notnull(), 0,
                     join_tot_delisted1['total_subs_score_delisted']))
        join_tot_delisted1['total_subs_score_delisted'] = join_tot_delisted1['total_subs_score_delisted'].fillna(0)
        join_tot_delisted1['subs_score_alive'] = 0
        join_tot_delisted1['subs_score_alive'] = np.where(join_tot_delisted1['base_product_number_y'].notnull(), 0,
                                                          join_tot_delisted1['actual_similarity_score'])

        # Calculate final substitute score with recursive transfer of substitution
        join_tot_delisted1['new_sub_score'] = 0
        join_tot_delisted1['total_subs_score_alive'] = join_tot_delisted1['total_subs_score_alive'].replace(
            to_replace=0, value=np.nan)
        join_tot_delisted1['new_sub_score'] = (
            np.where(join_tot_delisted1['total_subs_score_delisted'] == 0, join_tot_delisted1['subs_score_alive'],
                     join_tot_delisted1['subs_score_alive'].astype('float') + (
                         join_tot_delisted1['subs_score_alive'].astype('float') / join_tot_delisted1[
                             'total_subs_score_alive'].astype('float')) * join_tot_delisted1[
                         'total_subs_score_delisted'].astype('float')))
        join_tot_delisted1['new_sub_score'] = join_tot_delisted1['new_sub_score'].fillna(0)

        join_tot_delisted1['tcs_per'] = join_tot_delisted1['tcs_per'].astype('float')
        join_tot_delisted1['exclusivity_per'] = join_tot_delisted1['exclusivity_per'].astype('float')

        # In[16]:

        dd_quantile_prob_delised = join_tot_delisted1[
            join_tot_delisted1['base_product_number_x'].isin(dd_prods['base_product_number'])]

        dd_quantile_prob_delised = dd_quantile_prob_delised.drop_duplicates().fillna(0).reset_index(drop=True)
        dd_quantile_prob_delised_grp = dd_quantile_prob_delised.groupby(['base_product_number_x'],
                                                                        as_index=False).agg({'new_sub_score': sum})
        dd_quantile_prob_delised_grp = dd_quantile_prob_delised_grp.rename(
            columns={'new_sub_score': 'total_new_sub_score'})
        dd_quantile_prob_delised = pd.merge(dd_quantile_prob_delised, dd_quantile_prob_delised_grp,
                                            on=['base_product_number_x'], how='left')

        dd_quantile_prob_delised['adjusted_new_sub_score'] = (
            dd_quantile_prob_delised['new_sub_score'] / dd_quantile_prob_delised['total_new_sub_score'])

        dd_quantile_prob_delised['final_sub_score'] = 0
        dd_quantile_prob_delised['final_sub_score'] = dd_quantile_prob_delised['adjusted_new_sub_score'] * (
            1 - dd_quantile_prob_delised['tcs_per'] - dd_quantile_prob_delised['exclusivity_per'])

        dd_quantile_prob_delised['vol_transfer_prob'] = 0
        dd_quantile_prob_delised['vol_transfer_prob'] = dd_quantile_prob_delised['predicted_volume'].astype(
            'float') * dd_quantile_prob_delised['final_sub_score'].astype('float')

        # Rest prods
        rest_prob_delised = join_tot_delisted1[
            ~join_tot_delisted1['base_product_number_x'].isin(dd_prods['base_product_number'])]
        rest_prob_delised['final_sub_score'] = 0
        rest_prob_delised['final_sub_score'] = rest_prob_delised['new_sub_score'] * (
            1 - rest_prob_delised['tcs_per'] - rest_prob_delised['exclusivity_per'])
        rest_prob_delised['vol_transfer_prob'] = 0
        rest_prob_delised['vol_transfer_prob'] = rest_prob_delised['predicted_volume'].astype('float') * \
                                                 rest_prob_delised['final_sub_score'].astype('float')

        dd_quantile_prob_delised = dd_quantile_prob_delised[
            ['base_product_number_x', 'sub_prod', 'predicted_volume', 'vol_transfer_prob']]
        rest_prob_delised = rest_prob_delised[
            ['base_product_number_x', 'sub_prod', 'predicted_volume', 'vol_transfer_prob']]

        sh_present_prob = dd_quantile_prob_delised.append(rest_prob_delised)

        # In[17]:

        # Dunnhumby missing data
        cate_fore_SH_missing_delised = cate_fore_SH_missing_delised[
            ['base_product_number', 'predicted_volume', 'pps_ros_quantile',
             'ros_quantile']].drop_duplicates().reset_index(drop=True).fillna(0)
        cate_fore_similar_prods = pd.merge(cate_fore_SH_missing_delised, prob_sub_score[
            ['base_prod', 'sub_prod', 'actual_similarity_score', 'similarity_score']],
                                           left_on='base_product_number', right_on='base_prod', how='left')
        cate_fore_similar_prods_present = cate_fore_similar_prods[cate_fore_similar_prods.base_prod.notnull()]


        # In[19]:

        cate_fore_similar_prods_present = cate_fore_similar_prods_present[
            [u'base_product_number', u'predicted_volume', u'pps_ros_quantile', u'base_prod', u'sub_prod',
             u'actual_similarity_score', u'similarity_score', 'ros_quantile']]

        Low_dh_missing_delised = cate_fore_similar_prods_present[
            cate_fore_similar_prods_present.pps_ros_quantile == "Low"]

        # Low_dh_missing_delised = pd.merge(Low_dh_missing_delised[['base_product_number','predicted_volume']],prob_sub[['base_prod','sub_prod','actual_similarity_score']], left_on ="base_product_number", right_on = "base_prod", how = "left")
        check_sub_delisted1 = pd.merge(Low_dh_missing_delised, input_tpns, left_on=['sub_prod'],
                                       right_on=['base_product_number'], how='left')

        # check if substitute product is getting delisted
        check_sub_delisted1['actual_similarity_score'] = check_sub_delisted1['actual_similarity_score'].astype(
            'float')
        delisted_subs1 = check_sub_delisted1[(check_sub_delisted1.base_product_number_y.notnull())]
        alive_subs1 = check_sub_delisted1[(check_sub_delisted1.base_product_number_y.isnull())]

        # Roll up sub scores of live subs
        grp_subs_score_live1 = alive_subs1.groupby(['base_prod'], as_index=False).agg(
            {'actual_similarity_score': sum})
        grp_subs_score_live1 = grp_subs_score_live1.rename(
            columns={'actual_similarity_score': 'total_subs_score_alive'})

        # Roll up sub scores of de-listed subs
        grp_subs_score_delisted1 = delisted_subs1.groupby(['base_prod'], as_index=False).agg(
            {'actual_similarity_score': sum})
        grp_subs_score_delisted1 = grp_subs_score_delisted1.rename(
            columns={'actual_similarity_score': 'total_subs_score_delisted'})

        # Join the total sub scores of alive and de-listed substitutes
        join_tot_alive1 = pd.merge(check_sub_delisted1, grp_subs_score_live1, left_on=['base_prod'],
                                   right_on=['base_prod'], how='left')
        join_tot_delisted1 = pd.merge(join_tot_alive1, grp_subs_score_delisted1, left_on=['base_prod'],
                                      right_on=['base_prod'], how='left')

        # Mask all rows to 0 where the substitute has been de-listed
        join_tot_delisted1['total_subs_score_alive'] = (
            np.where(join_tot_delisted1['base_product_number_y'].notnull(), 0,
                     join_tot_delisted1['total_subs_score_alive']))
        join_tot_delisted1['total_subs_score_delisted'] = (
            np.where(join_tot_delisted1['base_product_number_y'].notnull(), 0,
                     join_tot_delisted1['total_subs_score_delisted']))
        join_tot_delisted1['total_subs_score_delisted'] = join_tot_delisted1['total_subs_score_delisted'].fillna(0)
        join_tot_delisted1['subs_score_alive'] = 0
        join_tot_delisted1['subs_score_alive'] = np.where(join_tot_delisted1['base_product_number_y'].notnull(), 0,
                                                          join_tot_delisted1['actual_similarity_score'])

        # Calculate final substitute score with recursive transfer of substitution
        join_tot_delisted1['new_sub_score'] = 0
        join_tot_delisted1['total_subs_score_alive'] = join_tot_delisted1['total_subs_score_alive'].replace(
            to_replace=0, value=np.nan)
        join_tot_delisted1['new_sub_score'] = (
            np.where(join_tot_delisted1['total_subs_score_delisted'] == 0, join_tot_delisted1['subs_score_alive'],
                     join_tot_delisted1['subs_score_alive'].astype('float') + (
                         join_tot_delisted1['subs_score_alive'].astype('float') / join_tot_delisted1[
                             'total_subs_score_alive'].astype('float')) * join_tot_delisted1[
                         'total_subs_score_delisted'].astype('float')))
        join_tot_delisted1['new_sub_score'] = join_tot_delisted1['new_sub_score'].fillna(0)

        Low_dh_missing_delised = join_tot_delisted1
        low_prob_delised_grp = join_tot_delisted1.groupby(['base_product_number_x'], as_index=False).agg(
            {'new_sub_score': sum})
        low_prob_delised_grp = low_prob_delised_grp.rename(columns={'new_sub_score': 'Total_new_score'})

        Low_dh_missing_delised_sub = pd.merge(Low_dh_missing_delised, low_prob_delised_grp,
                                              left_on='base_product_number_x', right_on='base_product_number_x',
                                              how='left')
        Low_dh_missing_delised_sub['Adjusted_new_score'] = Low_dh_missing_delised_sub['new_sub_score'] * (
            1 / Low_dh_missing_delised_sub['Total_new_score'])

        Low_dh_missing_delised_sub['final_sub_score'] = 0
        Low_dh_missing_delised_sub['final_sub_score'] = Low_dh_missing_delised_sub['Adjusted_new_score']
        Low_dh_missing_delised_sub = Low_dh_missing_delised_sub.fillna(0)

        Low_dh_missing_delised_sub['predicted_volume'] = Low_dh_missing_delised_sub['predicted_volume'].astype(
            'float')
        Low_dh_missing_delised_sub['vol_transfer_prob'] = 0
        Low_dh_missing_delised_sub['vol_transfer_prob'] = Low_dh_missing_delised_sub['predicted_volume'] * \
                                                          Low_dh_missing_delised_sub['final_sub_score']

        # In[20]:

        # for medium and high quantile products

        med_dh_missing_delised = cate_fore_similar_prods_present[
            ~(cate_fore_similar_prods_present.pps_ros_quantile == "Low")]

        med_dh_missing_delised = pd.merge(med_dh_missing_delised, All_BC_subs, left_on=['sub_prod'],
                                          right_on=['productcode'], how="left")
        med_dh_missing_delised = med_dh_missing_delised[med_dh_missing_delised.productcode.notnull()]

        med_dh_missing_delised['predicted_volume'] = med_dh_missing_delised['predicted_volume'].astype('float')
        med_dh_missing_delised['tcs_per'] = med_dh_missing_delised['tcs_per'].astype('float')
        med_dh_missing_delised['exclusivity_per'] = med_dh_missing_delised['exclusivity_per'].astype('float')
        med_dh_missing_delised['substitutescore'] = med_dh_missing_delised['substitutescore'].astype('float')

        # In[21]:

        # for condition base prod ros bucket = sub prod ros bucket

        sub_prod = pd.merge(med_dh_missing_delised[['sub_prod', 'ros_quantile']], pps_ros, left_on="sub_prod",
                            right_on="base_product_number", how='left')
        sub_prod = sub_prod.rename(columns={'ros_quantile_y': 'ros_tag_sub'})
        sub_prod = sub_prod[['sub_prod', 'ros_tag_sub']].drop_duplicates().reset_index(drop=True).fillna(0)
        # get similar prods above avg similariy score
        cut_off = med_dh_missing_delised[['base_prod', 'similarity_score']].drop_duplicates()
        cut_off = cut_off[['similarity_score']].mean()
        cut_off = cut_off['similarity_score']

        similar_prods = med_dh_missing_delised[
            [u'base_product_number', u'sub_prod', u'ros_quantile', u'similarity_score']].drop_duplicates().reset_index(
            drop=True).fillna(0)
        similar_prods = pd.merge(similar_prods, sub_prod[['sub_prod', 'ros_tag_sub']], left_on="sub_prod",
                                 right_on='sub_prod', how="left")

        similar_prods.similarity_score = similar_prods.similarity_score.astype(float)

        similar_prods_filter1 = similar_prods[
            (similar_prods.similarity_score >= cut_off) & (similar_prods.ros_quantile == similar_prods.ros_tag_sub)]

        similar_prods_filter1['sim_ros'] = "P"

        # if couldnt satisfy above condition, take prods above similar prods only
        similar_prods_filter2 = pd.merge(similar_prods, similar_prods_filter1[['base_product_number', 'sim_ros']],
                                         left_on="base_product_number", right_on="base_product_number", how='left')
        similar_prods_filter2 = similar_prods_filter2[(similar_prods_filter2.sim_ros.isnull())]

        similar_prods_filter2 = similar_prods_filter2[(similar_prods_filter2.similarity_score >= cut_off)]
        similar_prods_filter = similar_prods_filter1.append(similar_prods_filter2)

        similar_prods_filter = similar_prods_filter.reset_index(drop=True)

        # In[22]:

        # take only similar prods
        med_dh_missing_delised = med_dh_missing_delised[
            med_dh_missing_delised['sub_prod'].isin(similar_prods_filter['sub_prod'])]
        # In[23]:

        similar_prods_w_excl_TCS_grp = med_dh_missing_delised.groupby(['base_product_number'], as_index=False).agg(
            {'tcs_per': 'mean', 'exclusivity_per': 'mean'})
        similar_prods_w_excl_TCS_grp = similar_prods_w_excl_TCS_grp.rename(
            columns={'tcs_per': 'tcs_per_avg', 'exclusivity_per': 'exclusivity_per_avg'})

        cate_fore_similar_prods_present = pd.merge(med_dh_missing_delised, similar_prods_w_excl_TCS_grp,
                                                   left_on="base_product_number", right_on="base_product_number",
                                                   how="left")

        # In[24]:

        cate_fore_similar_prods_present = cate_fore_similar_prods_present[
            ['base_product_number', 'predicted_volume', 'pps_ros_quantile',
             'base_prod', 'sub_prod', 'actual_similarity_score', 'similarity_score',
             'ros_quantile', 'productcode', 'tcs_per_avg', 'exclusivity_per_avg']].drop_duplicates().reset_index(
            drop=True)

        # In[25]:

        check_sub_delisted1 = pd.merge(cate_fore_similar_prods_present, input_tpns, left_on=['sub_prod'],
                                       right_on=['base_product_number'], how='left')

        # check if substitute product is getting delisted
        check_sub_delisted1['actual_similarity_score'] = check_sub_delisted1['actual_similarity_score'].astype(
            'float')
        delisted_subs1 = check_sub_delisted1[(check_sub_delisted1.base_product_number_y.notnull())]
        alive_subs1 = check_sub_delisted1[(check_sub_delisted1.base_product_number_y.isnull())]

        # Roll up sub scores of live subs
        grp_subs_score_live1 = alive_subs1.groupby(['base_prod'], as_index=False).agg(
            {'actual_similarity_score': sum})
        grp_subs_score_live1 = grp_subs_score_live1.rename(
            columns={'actual_similarity_score': 'total_subs_score_alive'})

        # Roll up sub scores of de-listed subs
        grp_subs_score_delisted1 = delisted_subs1.groupby(['base_prod'], as_index=False).agg(
            {'actual_similarity_score': sum})
        grp_subs_score_delisted1 = grp_subs_score_delisted1.rename(
            columns={'actual_similarity_score': 'total_subs_score_delisted'})

        # Join the total sub scores of alive and de-listed substitutes
        join_tot_alive1 = pd.merge(check_sub_delisted1, grp_subs_score_live1, left_on=['base_prod'],
                                   right_on=['base_prod'], how='left')
        join_tot_delisted1 = pd.merge(join_tot_alive1, grp_subs_score_delisted1, left_on=['base_prod'],
                                      right_on=['base_prod'], how='left')

        # Mask all rows to 0 where the substitute has been de-listed
        join_tot_delisted1['total_subs_score_alive'] = (
            np.where(join_tot_delisted1['base_product_number_y'].notnull(), 0,
                     join_tot_delisted1['total_subs_score_alive']))
        join_tot_delisted1['total_subs_score_delisted'] = (
            np.where(join_tot_delisted1['base_product_number_y'].notnull(), 0,
                     join_tot_delisted1['total_subs_score_delisted']))
        join_tot_delisted1['total_subs_score_delisted'] = join_tot_delisted1['total_subs_score_delisted'].fillna(0)
        join_tot_delisted1['subs_score_alive'] = 0
        join_tot_delisted1['subs_score_alive'] = np.where(join_tot_delisted1['base_product_number_y'].notnull(), 0,
                                                          join_tot_delisted1['actual_similarity_score'])

        join_tot_delisted1['new_sub_score'] = 0
        join_tot_delisted1['total_subs_score_alive'] = join_tot_delisted1['total_subs_score_alive'].replace(
            to_replace=0, value=np.nan)
        join_tot_delisted1['new_sub_score'] = (
            np.where(join_tot_delisted1['total_subs_score_delisted'] == 0, join_tot_delisted1['subs_score_alive'],
                     join_tot_delisted1['subs_score_alive'].astype('float') + (
                         join_tot_delisted1['subs_score_alive'].astype('float') / join_tot_delisted1[
                             'total_subs_score_alive'].astype('float')) * join_tot_delisted1[
                         'total_subs_score_delisted'].astype('float')))
        join_tot_delisted1['new_sub_score'] = join_tot_delisted1['new_sub_score'].fillna(0)

        # In[26]:

        # join_tot_delisted1 = join_tot_delisted1.drop_duplicates().fillna(0).reset_index(drop=True)
        join_tot_delisted1 = join_tot_delisted1[
            ['base_product_number_x', 'sub_prod', 'predicted_volume', 'tcs_per_avg', 'exclusivity_per_avg',
             'actual_similarity_score', 'new_sub_score', 'pps_ros_quantile']]

        # In[27]:

        join_tot_delisted1 = join_tot_delisted1.drop_duplicates().fillna(0).reset_index(drop=True)

        # In[28]:

        Med_dh_missing_delised = join_tot_delisted1[join_tot_delisted1.pps_ros_quantile == "Med"]

        med_prob_delised_grp = Med_dh_missing_delised.groupby(['base_product_number_x'], as_index=False).agg(
            {'new_sub_score': sum})
        med_prob_delised_grp = med_prob_delised_grp.rename(columns={'new_sub_score': 'Total_new_score'})
        med_dh_missing_delised_sub = pd.merge(Med_dh_missing_delised, med_prob_delised_grp,
                                              on=['base_product_number_x'], how='left')
        med_dh_missing_delised_sub['Adjusted_new_score'] = med_dh_missing_delised_sub['new_sub_score'] * (
            1 / med_dh_missing_delised_sub['Total_new_score'])

        med_dh_missing_delised_sub['final_sub_score'] = 0
        med_dh_missing_delised_sub['final_sub_score'] = med_dh_missing_delised_sub['Adjusted_new_score'] * (
            1 - med_dh_missing_delised_sub['exclusivity_per_avg'])

        med_dh_missing_delised_sub['predicted_volume'] = med_dh_missing_delised_sub['predicted_volume'].astype(
            'float')

        med_dh_missing_delised_sub['vol_transfer_prob'] = 0
        med_dh_missing_delised_sub['vol_transfer_prob'] = med_dh_missing_delised_sub['predicted_volume'] * \
                                                          med_dh_missing_delised_sub['final_sub_score']

        # In[29]:

        substituted_vols_med = med_dh_missing_delised_sub.groupby(['base_product_number_x'], as_index=False).agg(
            {'vol_transfer_prob': sum})


        delisted_vols_med = med_dh_missing_delised_sub[
            ['base_product_number_x', 'predicted_volume']].drop_duplicates()
        delisted_vols_med = delisted_vols_med.groupby(['base_product_number_x'], as_index=False).agg(
            {'predicted_volume': sum})


        # In[30]:

        High_dh_missing_delised = join_tot_delisted1[join_tot_delisted1.pps_ros_quantile == "High"]


        High_dh_missing_delised['final_sub_score'] = 0
        High_dh_missing_delised['final_sub_score'] = High_dh_missing_delised['new_sub_score'] * (
            1 - High_dh_missing_delised['exclusivity_per_avg'])

        High_dh_missing_delised['predicted_volume'] = High_dh_missing_delised['predicted_volume'].astype('float')

        High_dh_missing_delised['vol_transfer_prob'] = 0
        High_dh_missing_delised['vol_transfer_prob'] = High_dh_missing_delised['predicted_volume'] * \
                                                       High_dh_missing_delised['final_sub_score']

        # In[32]:

        substituted_vols_high = High_dh_missing_delised.groupby(['base_product_number_x'], as_index=False).agg(
            {'vol_transfer_prob': sum})
       

        delisted_vols_high = High_dh_missing_delised[
            ['base_product_number_x', 'predicted_volume']].drop_duplicates()
        delisted_vols_high = delisted_vols_high.groupby(['base_product_number_x'], as_index=False).agg(
            {'predicted_volume': sum})


        # In[33]:

        Low_dh_missing_delised_sub = Low_dh_missing_delised_sub[
            ['base_product_number_x', 'sub_prod', 'predicted_volume', 'vol_transfer_prob']]
        med_dh_missing_delised_sub = med_dh_missing_delised_sub[
            ['base_product_number_x', 'sub_prod', 'predicted_volume', 'vol_transfer_prob']]
        High_dh_missing_delised = High_dh_missing_delised[
            ['base_product_number_x', 'sub_prod', 'predicted_volume', 'vol_transfer_prob']]

        # In[34]:

        sh_miss_prob = Low_dh_missing_delised_sub.append(med_dh_missing_delised_sub)
        sh_miss_prob = sh_miss_prob.append(High_dh_missing_delised)
        sh_miss_prob = sh_miss_prob.drop_duplicates().fillna(0).reset_index(drop=True)

        # In[36]:

        # prob based
        prob_transfer_dataset = sh_present_prob.append(sh_miss_prob)

        prob_transfer_dataset = prob_transfer_dataset.rename(
            columns={'base_product_number_x': 'base_prod', 'predicted_volume': 'prob_pred_vol'})

        # In[37]:

        # product level impact
        product_dataset = pd.merge(vol_transfer_dataset, prob_transfer_dataset,
                                   left_on=['productcode', 'substituteproductcode'],
                                   right_on=['base_prod', 'sub_prod'], how='outer')
        product_dataset = product_dataset[
            ['productcode', 'substituteproductcode', 'base_prod', 'sub_prod', 'delist_pred_vol', 'prob_pred_vol',
             'vol_transfer', 'vol_transfer_prob']]
        product_dataset['base_prod'] = product_dataset['base_prod'].fillna(product_dataset['productcode'])
        product_dataset['productcode'] = product_dataset['productcode'].fillna(product_dataset['base_prod'])
        product_dataset['sub_prod'] = product_dataset['sub_prod'].fillna(product_dataset['substituteproductcode'])
        product_dataset['substituteproductcode'] = product_dataset['substituteproductcode'].fillna(
            product_dataset['sub_prod'])
        product_dataset['prob_pred_vol'] = product_dataset['prob_pred_vol'].fillna(
            product_dataset['delist_pred_vol'])
        product_dataset['delist_pred_vol'] = product_dataset['delist_pred_vol'].fillna(
            product_dataset['prob_pred_vol'])

        product_dataset = product_dataset[
            ['productcode', 'substituteproductcode', 'delist_pred_vol', 'vol_transfer', 'vol_transfer_prob']]
        product_dataset = product_dataset.fillna(0)
        product_dataset = product_dataset.drop_duplicates()
        product_dataset['tot_vols_transfer'] = product_dataset['vol_transfer'] + product_dataset[
            'vol_transfer_prob']
        prod_hrchy = read_frame(
            product_desc.objects.all().filter(buying_controller__in=bc).values('base_product_number', 'brand_indicator',
                                                                               'long_description').distinct())
        product_dataset = pd.merge(product_dataset, prod_hrchy, left_on=['substituteproductcode'],
                                   right_on=['base_product_number'], how='left')
        product_dataset = product_dataset[
            ['productcode', 'substituteproductcode', 'delist_pred_vol', 'tot_vols_transfer', 'brand_indicator']]

        # In[39]:

        # read product price data and assign it to varaible 'price' on base prod
        prod_price_data = read_frame(
            product_price.objects.filter(buying_controller__in=bc, store_type__in=store).values(
                'base_product_number', 'asp', 'acp'))
        prod_price_data = prod_price_data.drop_duplicates()
        product_dataset = pd.merge(product_dataset, prod_price_data, left_on=['productcode'],
                                   right_on=['base_product_number'], how='left')
        del product_dataset['base_product_number']

        # CTS on base prod
        cts = read_frame(cts_data.objects.filter(buying_controller__in=bc, store_type__in=store).all())
        cts = cts[['base_product_number', 'long_description', 'cts_per_unit']]
        cts = cts.drop_duplicates()
        product_dataset = pd.merge(product_dataset, cts, left_on=['productcode'], right_on=['base_product_number'],
                                   how='left')

        del product_dataset['base_product_number']
        del product_dataset['long_description']

        product_dataset['predicted_value'] = product_dataset['delist_pred_vol'].astype('float') * product_dataset[
            'asp'].astype('float')
        product_dataset['predicted_cgm'] = product_dataset['delist_pred_vol'].astype('float') * (
            product_dataset['asp'] - product_dataset['acp']).astype('float')
        product_dataset['predicted_cts'] = product_dataset['delist_pred_vol'].astype('float') * product_dataset[
            'cts_per_unit'].astype('float')

        product_dataset = product_dataset.rename(columns={'delist_pred_vol': 'predicted_volume'})

        # In[40]:

        # for sub prod
        product_dataset = product_dataset[
            ['productcode', 'substituteproductcode', 'brand_indicator', 'predicted_volume',
             'predicted_value', 'predicted_cgm', 'predicted_cts', 'tot_vols_transfer']]
        # price with sub
        product_dataset = pd.merge(product_dataset, prod_price_data, left_on=['substituteproductcode'],
                                   right_on=['base_product_number'], how='left')
        del product_dataset['base_product_number']

        # cts with sub
        product_dataset = pd.merge(product_dataset, cts, left_on=['substituteproductcode'],
                                   right_on=['base_product_number'], how='left')
        del product_dataset['base_product_number']
        del product_dataset['long_description']

        product_dataset['value_transfer'] = product_dataset['tot_vols_transfer'].astype('float') * product_dataset[
            'asp'].astype('float')
        product_dataset['cgm_transfer'] = product_dataset['tot_vols_transfer'].astype('float') * (
            product_dataset['asp'] - product_dataset['acp']).astype('float')
        product_dataset['cts_transfer'] = product_dataset['tot_vols_transfer'].astype('float') * product_dataset[
            'cts_per_unit'].astype('float')
        product_dataset = product_dataset.rename(columns={'tot_vols_transfer': 'volume_transfer'})
        product_dataset = product_dataset[
            ['productcode', 'substituteproductcode', 'brand_indicator', 'predicted_volume',
             'predicted_value', 'predicted_cgm', 'predicted_cts', 'volume_transfer',
             'value_transfer', 'cgm_transfer', 'cts_transfer']]

        # In[43]:

        product_dataset['predicted_volume'] = product_dataset['predicted_volume'].astype('float')
        product_dataset['predicted_value'] = product_dataset['predicted_value'].astype('float')
        product_dataset['predicted_cgm'] = product_dataset['predicted_cgm'].astype('float')
        product_dataset['predicted_cts'] = product_dataset['predicted_cts'].astype('float')

        product_dataset['volume_transfer'] = product_dataset['volume_transfer'].astype('float')
        product_dataset['value_transfer'] = product_dataset['value_transfer'].astype('float')
        product_dataset['cgm_transfer'] = product_dataset['cgm_transfer'].astype('float')
        product_dataset['cts_transfer'] = product_dataset['cts_transfer'].astype('float')

        return product_dataset


    def waterfall_chart(self,product_dataset,bc,future):
        initial_volume = product_dataset[['productcode', 'predicted_volume']].drop_duplicates().reset_index(
            drop=True).groupby(['productcode'], as_index=False).agg({'predicted_volume': max})
        initial_volume = initial_volume['predicted_volume'].sum()
        volume_transfer_brand = product_dataset[product_dataset['brand_indicator'] == "B"].groupby(
            ['brand_indicator'], as_index=False).agg({'volume_transfer': sum})
        volume_transfer_ownlabel = product_dataset[product_dataset['brand_indicator'] == "T"].groupby(
            ['brand_indicator'], as_index=False).agg({'volume_transfer': sum})

        volume_final_loss = initial_volume - product_dataset['volume_transfer'].sum()
        delist_impact_volume = product_dataset['volume_transfer'].sum()
        vols_waterfall = pd.DataFrame()
        vols_waterfall['name'] = []
        vols_waterfall['value'] = []
        vols_waterfall.ix[0, 'name'] = 'Volume lost from delist products'
        vols_waterfall.ix[1, 'name'] = 'Volume transfer to substitute brand'
        vols_waterfall.ix[2, 'name'] = 'Volume transfer to substitute OL'

        vols_waterfall.ix[0, 'value'] = initial_volume
        if volume_transfer_brand.empty:
            vols_waterfall.ix[1, 'value'] = 0
        else:
            vols_waterfall.ix[1, 'value'] = -volume_transfer_brand['volume_transfer'].iloc[0]

        if volume_transfer_ownlabel.empty:
            vols_waterfall.ix[2, 'value'] = 0
        else:
            vols_waterfall.ix[2, 'value'] = -volume_transfer_ownlabel['volume_transfer'].iloc[0]
        vols_waterfall = vols_waterfall.to_dict(orient='records')
        if initial_volume == 0:
            vol_tot_transfer = 0
        else:
            vol_tot_transfer = (product_dataset['volume_transfer'].sum() / initial_volume) * 100
            vol_tot_transfer = (format(vol_tot_transfer, '.1f'))
            vol_tot_transfer = float(vol_tot_transfer)

        # In[45]:

        initial_sales = product_dataset[['productcode', 'predicted_value']].drop_duplicates().reset_index(
            drop=True).groupby(['productcode'], as_index=False).agg({'predicted_value': max})
        initial_sales = initial_sales['predicted_value'].sum()
        sales_transfer_brand = product_dataset[product_dataset['brand_indicator'] == "B"].groupby(
            ['brand_indicator'], as_index=False).agg({'value_transfer': sum})
        sales_transfer_ownlabel = product_dataset[product_dataset['brand_indicator'] == "T"].groupby(
            ['brand_indicator'], as_index=False).agg({'value_transfer': sum})
        sales_final_loss = initial_sales - product_dataset['value_transfer'].sum()
        delist_impact_sales = product_dataset['value_transfer'].sum()
        sales_waterfall = pd.DataFrame()
        sales_waterfall['name'] = []
        sales_waterfall['value'] = []
        sales_waterfall.ix[0, 'name'] = 'Value lost from delist products'
        sales_waterfall.ix[1, 'name'] = 'Value transfer to substitute brand'
        sales_waterfall.ix[2, 'name'] = 'Value transfer to substitute OL'

        sales_waterfall.ix[0, 'value'] = initial_sales
        if sales_transfer_brand.empty:
            sales_waterfall.ix[1, 'value'] = 0
        else:
            sales_waterfall.ix[1, 'value'] = -sales_transfer_brand['value_transfer'].iloc[0]
        if sales_transfer_ownlabel.empty:
            sales_waterfall.ix[2, 'value'] = 0
        else:
            sales_waterfall.ix[2, 'value'] = -sales_transfer_ownlabel['value_transfer'].iloc[0]
        sales_waterfall = sales_waterfall.to_dict(orient='records')

        if initial_sales == 0:
            sales_tot_transfer = 0
        else:
            sales_tot_transfer = (product_dataset['value_transfer'].sum() / initial_sales) * 100
            sales_tot_transfer = (format(sales_tot_transfer, '.1f'))
            sales_tot_transfer = float(sales_tot_transfer)

        # In[46]:

        initial_cgm = product_dataset[['productcode', 'predicted_cgm']].drop_duplicates().reset_index(
            drop=True).groupby(['productcode'], as_index=False).agg({'predicted_cgm': max})
        initial_cgm = initial_cgm['predicted_cgm'].sum()
        cgm_transfer_brand = product_dataset[product_dataset['brand_indicator'] == "B"].groupby(['brand_indicator'],
                                                                                                as_index=False).agg(
            {'cgm_transfer': sum})
        cgm_transfer_ownlabel = product_dataset[product_dataset['brand_indicator'] == "T"].groupby(
            ['brand_indicator'], as_index=False).agg({'cgm_transfer': sum})
        cgm_final_loss = initial_cgm - product_dataset['cgm_transfer'].sum()

        cgm_waterfall = pd.DataFrame()
        cgm_waterfall['name'] = []
        cgm_waterfall['value'] = []
        cgm_waterfall.ix[0, 'name'] = 'CGM lost from delist products'
        cgm_waterfall.ix[1, 'name'] = 'CGM transfer to substitute brand'
        cgm_waterfall.ix[2, 'name'] = 'CGM transfer to substitute OL'

        cgm_waterfall.ix[0, 'value'] = initial_cgm
        if cgm_transfer_brand.empty:
            cgm_waterfall.ix[1, 'value'] = 0
        else:
            cgm_waterfall.ix[1, 'value'] = -cgm_transfer_brand['cgm_transfer'].iloc[0]
        if cgm_transfer_ownlabel.empty:
            cgm_waterfall.ix[2, 'value'] = 0
        else:
            cgm_waterfall.ix[2, 'value'] = -cgm_transfer_ownlabel['cgm_transfer'].iloc[0]
        cgm_waterfall = cgm_waterfall.to_dict(orient='records')

        if initial_cgm == 0:
            cgm_tot_transfer = 0
        else:
            cgm_tot_transfer = (product_dataset['cgm_transfer'].sum() / initial_cgm) * 100
            cgm_tot_transfer = (format(cgm_tot_transfer, '.1f'))
            cgm_tot_transfer = float(cgm_tot_transfer)


        # In[47]:

        initial_cts = product_dataset[['productcode', 'predicted_cts']].drop_duplicates().reset_index(
            drop=True).groupby(['productcode'], as_index=False).agg({'predicted_cts': max})
        initial_cts = initial_cts['predicted_cts'].sum()
        cts_transfer_brand = product_dataset[product_dataset['brand_indicator'] == "B"].groupby(['brand_indicator'],
                                                                                                as_index=False).agg(
            {'cts_transfer': sum})
        cts_transfer_ownlabel = product_dataset[product_dataset['brand_indicator'] == "T"].groupby(
            ['brand_indicator'], as_index=False).agg({'cts_transfer': sum})
        cts_final_loss = initial_cts - product_dataset['cts_transfer'].sum()

        cts_waterfall = pd.DataFrame()
        cts_waterfall['name'] = []
        cts_waterfall['value'] = []
        cts_waterfall.ix[0, 'name'] = 'CTS gain from delist products'
        cts_waterfall.ix[1, 'name'] = 'CTS transfer to substitute brand'
        cts_waterfall.ix[2, 'name'] = 'CTS transfer to substitute OL'

        cts_waterfall.ix[0, 'value'] = initial_cts
        if cts_transfer_brand.empty:
            cts_waterfall.ix[1, 'value'] = 0
        else:
            cts_waterfall.ix[1, 'value'] = -cts_transfer_brand['cts_transfer'].iloc[0]
        if cts_transfer_ownlabel.empty:
            cts_waterfall.ix[2, 'value'] = 0
        else:
            cts_waterfall.ix[2, 'value'] = -cts_transfer_ownlabel['cts_transfer'].iloc[0]
        cts_waterfall = cts_waterfall.to_dict(orient='records')

        if initial_cts == 0:
            cts_tot_transfer = 0
        else:
            cts_tot_transfer = (product_dataset['cts_transfer'].sum() / initial_cts) * 100
            cts_tot_transfer = (format(cts_tot_transfer, '.1f'))
            cts_tot_transfer = float(cts_tot_transfer)

    #def bc_contri_logic(self,bc,store,future,input_tpns_main,delist_main):
        prod_price_data = read_frame(
            product_price.objects.filter(buying_controller__in=bc, store_type__in=['Main Estate']).values(
                'base_product_number', 'asp', 'acp'))
        prod_price_data = prod_price_data.drop_duplicates().fillna(0).reset_index(drop=True)

        cts_main = read_frame(cts_data.objects.filter(buying_controller__in=bc, store_type__in=['Main Estate']).all())

        contribution_main = read_frame(
            product_contri.objects.all().filter(buying_controller__in=bc, store_type__in=['Main Estate'],
                                                time_period__in=future).values_list())

        contribution_main = contribution_main.rename(columns={'base_product_number': 'productcode'})

        bc_predict_main = pd.merge(contribution_main, prod_price_data, left_on=['productcode'],
                                   right_on=['base_product_number'], how='left')
        bc_predict_main = pd.merge(bc_predict_main, cts_main, left_on=['base_product_number'],
                                   right_on=['base_product_number'], how='left')
        bc_predict_main = bc_predict_main.drop_duplicates().fillna(0).reset_index(drop=True)

        bc_predict_main['predicted_volume'] = bc_predict_main['predicted_volume'].astype('float')
        bc_predict_main['asp'] = bc_predict_main['asp'].astype('float')
        bc_predict_main['acp'] = bc_predict_main['acp'].astype('float')
        bc_predict_main['cts_per_unit'] = bc_predict_main['cts_per_unit'].astype('float')

        bc_predict_main['predicted_sales'] = bc_predict_main['predicted_volume'] * bc_predict_main['asp']
        bc_predict_main['predicted_cgm'] = bc_predict_main['predicted_volume'] * (
        bc_predict_main['asp'] - bc_predict_main['acp'])
        bc_predict_main['predicted_cts'] = bc_predict_main['predicted_volume'] * bc_predict_main['cts_per_unit']
        bc_predict_main = bc_predict_main[
            ['base_product_number', 'predicted_volume', 'predicted_sales', 'predicted_cgm', 'predicted_cts', 'asp',
             'acp', 'cts_per_unit']]

        # In[19]:

        prod_price_data = read_frame(
            product_price.objects.filter(buying_controller__in=bc, store_type__in=['Express']).values(
                'base_product_number', 'asp', 'acp'))
        prod_price_data = prod_price_data.drop_duplicates().fillna(0).reset_index(drop=True)

        cts_exp = read_frame(cts_data.objects.filter(buying_controller__in=bc, store_type__in=['Express']).all())

        contribution_exp = read_frame(
            product_contri.objects.all().filter(buying_controller__in=bc, store_type__in=['Express'],
                                                time_period__in=future).values_list())

        contribution_exp = contribution_exp.rename(columns={'base_product_number': 'productcode'})

        bc_predict_exp = pd.merge(contribution_exp, prod_price_data, left_on=['productcode'],
                                  right_on=['base_product_number'], how='left')
        bc_predict_exp = pd.merge(bc_predict_exp, cts_exp, left_on=['base_product_number'],
                                  right_on=['base_product_number'], how='left')
        bc_predict_exp = bc_predict_exp.drop_duplicates().fillna(0).reset_index(drop=True)

        bc_predict_exp['predicted_volume'] = bc_predict_exp['predicted_volume'].astype('float')
        bc_predict_exp['asp'] = bc_predict_exp['asp'].astype('float')
        bc_predict_exp['acp'] = bc_predict_exp['acp'].astype('float')
        bc_predict_exp['cts_per_unit'] = bc_predict_exp['cts_per_unit'].astype('float')

        bc_predict_exp['predicted_sales'] = bc_predict_exp['predicted_volume'] * bc_predict_exp['asp']
        bc_predict_exp['predicted_cgm'] = bc_predict_exp['predicted_volume'] * (
        bc_predict_exp['asp'] - bc_predict_exp['acp'])
        bc_predict_exp['predicted_cts'] = bc_predict_exp['predicted_volume'] * bc_predict_exp['cts_per_unit']
        bc_predict_exp = bc_predict_exp[
            ['base_product_number', 'predicted_volume', 'predicted_sales', 'predicted_cgm', 'predicted_cts', 'asp',
             'acp', 'cts_per_unit']]

        bc_predict = pd.merge(bc_predict_main, bc_predict_exp, left_on=['base_product_number'],
                              right_on=['base_product_number'], how='outer')
        bc_predict = bc_predict.drop_duplicates().fillna(0).reset_index(drop=True)
        bc_predict['predicted_volume'] = bc_predict['predicted_volume_x'] + bc_predict['predicted_volume_y']
        bc_predict['predicted_sales'] = bc_predict['predicted_sales_x'] + bc_predict['predicted_sales_y']
        bc_predict['predicted_cgm'] = bc_predict['predicted_cgm_x'] + bc_predict['predicted_cgm_y']
        bc_predict['predicted_cts'] = bc_predict['predicted_cts_x'] + bc_predict['predicted_cts_y']
        bc_predict = bc_predict[
            ['base_product_number', 'predicted_volume', 'predicted_sales', 'predicted_cgm', 'predicted_cts']]

        bc_cgm = bc_predict['predicted_cgm'].sum()
        if bc_cgm == 0:
            bc_cgm_contri = 0
            bc_cgm_contri = float(bc_cgm_contri)
        else:
            bc_cgm_contri = (float(cgm_final_loss) / float(bc_cgm)) * (-100)
            bc_cgm_contri = (format(bc_cgm_contri, '.1f'))
            bc_cgm_contri = float(bc_cgm_contri)

        bc_cts = bc_predict['predicted_cts'].sum()
        if bc_cts == 0:
            bc_cts_contri = 0
            bc_cts_contri = float(bc_cts_contri)
        else:
            bc_cts_contri = (float(cts_final_loss) / float(bc_cts)) * (100)
            bc_cts_contri = (format(bc_cts_contri, '.1f'))
            bc_cts_contri = float(bc_cts_contri)

        bc_sales = bc_predict['predicted_sales'].sum()
        if bc_sales == 0:
            bc_sales_contri = 0
            bc_sales_contri = float(bc_sales_contri)
        else:
            bc_sales_contri = (float(sales_final_loss) / float(bc_sales)) * (-100)
            bc_sales_contri = (format(bc_sales_contri, '.1f'))
            bc_sales_contri = float(bc_sales_contri)

        bc_vols = bc_predict['predicted_volume'].sum()
        if bc_vols == 0:
            bc_vols_contri = 0
            bc_vols_contri = float(bc_vols_contri)
        else:
            bc_vols_contri = (float(volume_final_loss) / float(bc_vols)) * (-100)
            bc_vols_contri = (format(bc_vols_contri, '.1f'))
            bc_vols_contri = float(bc_vols_contri)



        data = {
            'cgm_chart': cgm_waterfall,
                'cts_chart': cts_waterfall,
                'sales_chart': sales_waterfall,
                'vols_chart': vols_waterfall,
                'bc_vols_contri': bc_vols_contri,
                'bc_sales_contri': bc_sales_contri,
                'bc_cgm_contri': bc_cgm_contri,
                'bc_cts_contri': bc_cts_contri,
                'vol_tot_transfer' : vol_tot_transfer,
                'sales_tot_transfer': sales_tot_transfer,
                'cgm_tot_transfer': cgm_tot_transfer,
                'cts_tot_transfer': cts_tot_transfer,
                'delist_impact_sales':delist_impact_sales,
                'delist_impact_volume':delist_impact_volume,
                'volume_final_loss':volume_final_loss,
                'sales_final_loss': sales_final_loss
        }
        return data


    def supplier_table(self,product_dataset_main, store, future, bc):
        sup_share = read_frame(
            supplier_share.objects.all().filter(buying_controller__in=bc, store_type__in=store).values(
                'parent_supplier', 'base_product_number', 'volume_share'))
        contribution_main = read_frame(
            product_contri.objects.all().filter(buying_controller__in=bc, store_type__in=store,time_period__in=['12_months']).values_list())
        contribution_main = contribution_main.rename(columns={'base_product_number': 'productcode'})

        prod_contri = contribution_main[['productcode', 'predicted_volume']].drop_duplicates().fillna(0).reset_index(
            drop=True)
        prod_contri = prod_contri.rename(columns={'productcode': 'base_product_number'})
        sup_table = pd.merge(sup_share, prod_contri, left_on=['base_product_number'], right_on=['base_product_number'],
                             how='left')
        sup_table = sup_table.drop_duplicates().fillna(0).reset_index(drop=True)

        impact_product = product_dataset_main[['productcode', 'predicted_volume']].drop_duplicates().fillna(
            0).reset_index(drop=True)
        impact_product = impact_product.rename(columns={'predicted_volume': 'predicted_volume_x'})

        sup_table = pd.merge(sup_table, impact_product[['productcode', 'predicted_volume_x']],
                             left_on=['base_product_number'], right_on=['productcode'], how='left')
        del sup_table['productcode']
        sup_table = sup_table.drop_duplicates().fillna(0).reset_index(drop=True)
        sup_table = sup_table.rename(columns={'predicted_volume_x': 'vols_loss'})

        impact_subs = product_dataset_main[['substituteproductcode', 'volume_transfer']].groupby(
            ['substituteproductcode'], as_index=False).agg({'volume_transfer': sum})
        sup_table = pd.merge(sup_table, impact_subs[['substituteproductcode', 'volume_transfer']],
                             left_on=['base_product_number'], right_on=['substituteproductcode'], how='left')

        sup_table = sup_table.rename(columns={'volume_transfer': 'vols_gain'})

        del sup_table['substituteproductcode']
        sup_table = sup_table.drop_duplicates().fillna(0).reset_index(drop=True)

        sup_table['vols_gain'] = sup_table['vols_gain'].astype('float')
        sup_table['vols_loss'] = sup_table['vols_loss'].astype('float')
        sup_table['predicted_volume'] = sup_table['predicted_volume'].astype('float')
        sup_table['volume_share'] = sup_table['volume_share'].astype('float')

        sup_table['vols_gain_share'] = sup_table['vols_gain'] * sup_table['volume_share']
        sup_table['vols_loss_share'] = sup_table['vols_loss'] * sup_table['volume_share']
        sup_table['predicted_volume_share'] = sup_table['predicted_volume'] * sup_table['volume_share']

        del sup_table['vols_gain']
        del sup_table['vols_loss']
        del sup_table['predicted_volume']

        prod_price_data = read_frame(
            product_price.objects.filter(buying_controller__in=bc, store_type__in=['Main Estate']).values(
                'base_product_number', 'asp', 'acp'))

        sup_table = pd.merge(sup_table, prod_price_data[['base_product_number', 'asp']],
                             left_on=['base_product_number'], right_on=['base_product_number'], how='left')

        sup_table['asp'] = sup_table['asp'].astype('float')

        sup_table['value_gain_share'] = sup_table['vols_gain_share'] * sup_table['asp']
        sup_table['value_loss_share'] = sup_table['vols_loss_share'] * sup_table['asp']
        sup_table['predicted_value_share'] = sup_table['predicted_volume_share'] * sup_table['asp']
        #sup_table_main = sup_table
        sup_table_share = sup_table.groupby(['parent_supplier'], as_index=False).agg(
            {'predicted_volume_share': sum, 'vols_gain_share': sum, 'vols_loss_share': sum,
             'predicted_value_share': sum, 'value_gain_share': sum,
             'value_loss_share': sum})

        sup_table_share['vol_impact'] = sup_table_share['vols_gain_share'] - sup_table_share['vols_loss_share']
        sup_table_share['value_impact'] = sup_table_share['value_gain_share'] - sup_table_share['value_loss_share']

        sup_table_share['predicted_volume_share'] = sup_table_share['predicted_volume_share'].replace(0, 1)
        sup_table_share['predicted_value_share'] = sup_table_share['predicted_value_share'].replace(0, 1)

        try:
            sup_table_share['vol_impact_per'] = (sup_table_share['vol_impact'] * 100) / sup_table_share[
                'predicted_volume_share']
        except:
            sup_table_share['vol_impact_per'] = 0

        try:
            sup_table_share['value_impact_per'] = (sup_table_share['value_impact'] * 100) / sup_table_share[
                'predicted_value_share']
        except:
            sup_table_share['value_impact_per'] = 0

        sup_sales_table = sup_table_share[sup_table_share['vol_impact'] != 0]
        sup_sales_table_main = sup_sales_table[
            ['parent_supplier', 'predicted_volume_share', 'vols_gain_share', 'vols_loss_share', 'vol_impact',
             'vol_impact_per', 'predicted_value_share', 'value_gain_share', 'value_loss_share', 'value_impact',
             'value_impact_per']]
        sup_sales_table_main['vol_impact_per'] = sup_sales_table_main['vol_impact_per'].round(decimals=1)
        sup_sales_table_main['value_impact_per'] = sup_sales_table_main['value_impact_per'].round(decimals=1)
        sup_sales_table_main = sup_sales_table_main.drop_duplicates().fillna(0).reset_index(drop=True)

        return sup_table,sup_sales_table_main

    def delist_subs(self,bc,store,future, delist_main):
        delist_prod_subs = read_frame(
            shelf_review_subs.objects.all().filter(buying_controller__in=bc, store_type__in=store,
                                                   productcode__in=delist_main).values('productcode',
                                                                                       'substituteproductcode'))
        prod_sim = read_frame(
            prod_similarity_subs.objects.all().filter(buying_controller__in=bc, store_type__in=store,
                                                      base_prod__in=delist_main).values('base_prod', 'sub_prod',
                                                                                        'actual_similarity_score'))
        contribution_main = read_frame(
            product_contri.objects.all().filter(buying_controller__in=bc, store_type__in=store,
                                                time_period__in=future).values_list())
        contribution_main = contribution_main.rename(columns={'base_product_number': 'productcode'})

        prod_hrchy = read_frame(
            product_desc.objects.all().values('base_product_number', 'brand_indicator', 'long_description').distinct())

        delist_prod_sim = prod_sim.sort_values(['base_prod', 'actual_similarity_score'], ascending=False).groupby(
            'base_prod').head(10)
        delist_prod_sim = delist_prod_sim[['base_prod', 'sub_prod']]
        delist_prod_sim = delist_prod_sim.rename(
            columns={'base_prod': 'productcode', 'sub_prod': 'substituteproductcode'})

        delist_prod_subs_main = pd.DataFrame()
        delist_prod_subs_main = delist_prod_subs.append(delist_prod_sim)
        delist_prod_subs_main = delist_prod_subs_main.drop_duplicates().fillna(0).reset_index(drop=True)

        delist_prod_subs_main = delist_prod_subs_main[-delist_prod_subs_main['substituteproductcode'].isin(delist_main)]
        delist_prod_subs_main = delist_prod_subs_main[
            delist_prod_subs_main['productcode'].isin(contribution_main['productcode'])]

        delist_prod_subs_main = pd.merge(delist_prod_subs_main, prod_hrchy, left_on=['productcode'],
                                         right_on=['base_product_number'], how='left')
        delist_prod_subs_main = delist_prod_subs_main.rename(
            columns={'long_description': 'productdescription'})
        delist_prod_subs_main = pd.merge(delist_prod_subs_main, prod_hrchy, left_on=['substituteproductcode'],
                                         right_on=['base_product_number'], how='left')
        delist_prod_subs_main = delist_prod_subs_main.rename(
            columns={'long_description': 'substituteproductdescription'})
        delist_prod_subs_main = delist_prod_subs_main[
            ['productcode', 'productdescription', 'substituteproductcode', 'substituteproductdescription']]

        return delist_prod_subs_main

    # buyer impact - table in compare scenario
    def buyer_table(self,product_dataset_main, store, future, bc):
        buyer = read_frame(
            product_hierarchy.objects.all().filter(buying_controller__in=bc).values('buyer', 'base_product_number'))
        contribution = read_frame(
            product_contri.objects.all().filter(buying_controller__in=bc, store_type__in=store,
                                                time_period__in=['12_months']).values_list())
        contribution= contribution.rename(columns={'base_product_number': 'productcode'})
        prod_contri = contribution[['productcode', 'predicted_volume']].drop_duplicates().fillna(0).reset_index(
            drop=True)
        prod_contri = prod_contri.rename(columns={'productcode': 'base_product_number'})
        buyer_table = pd.merge(buyer, prod_contri, left_on=['base_product_number'], right_on=['base_product_number'],
                               how='left')
        buyer_table = buyer_table.drop_duplicates().fillna(0).reset_index(drop=True)

        impact_product = product_dataset_main[['productcode', 'predicted_volume']].drop_duplicates().fillna(
            0).reset_index(drop=True)
        impact_product = impact_product.rename(columns={'predicted_volume': 'predicted_volume_x'})
        buyer_table = pd.merge(buyer_table, impact_product[['productcode', 'predicted_volume_x']],
                               left_on=['base_product_number'], right_on=['productcode'], how='left')
        del buyer_table['productcode']
        buyer_table = buyer_table.drop_duplicates().fillna(0)
        buyer_table = buyer_table.rename(columns={'predicted_volume_x': 'vols_loss'})

        impact_subs = product_dataset_main[['substituteproductcode', 'volume_transfer']].groupby(
            ['substituteproductcode'], as_index=False).agg({'volume_transfer': sum})
        buyer_table = pd.merge(buyer_table, impact_subs[['substituteproductcode', 'volume_transfer']],
                               left_on=['base_product_number'], right_on=['substituteproductcode'], how='left')

        buyer_table = buyer_table.rename(columns={'volume_transfer': 'vols_gain'})

        del buyer_table['substituteproductcode']
        buyer_table = buyer_table.drop_duplicates().fillna(0)

        buyer_table['vols_gain'] = buyer_table['vols_gain'].astype('float')
        buyer_table['vols_loss'] = buyer_table['vols_loss'].astype('float')
        buyer_table['predicted_volume'] = buyer_table['predicted_volume'].astype('float')
        # buyer_table['volume_share'] = buyer_table['volume_share'].astype('float')

        buyer_table['vols_gain_share'] = buyer_table['vols_gain']  # * sup_table['volume_share']
        buyer_table['vols_loss_share'] = buyer_table['vols_loss']  # * sup_table['volume_share']
        buyer_table['predicted_volume_share'] = buyer_table['predicted_volume']  # * sup_table['volume_share']

        del buyer_table['vols_gain']
        del buyer_table['vols_loss']
        del buyer_table['predicted_volume']

        prod_price_data = read_frame(
            product_price.objects.filter(buying_controller__in=bc, store_type__in=['Main Estate']).values(
                'base_product_number', 'asp', 'acp'))

        buyer_table = pd.merge(buyer_table, prod_price_data[['base_product_number', 'asp']],
                               left_on=['base_product_number'], right_on=['base_product_number'], how='left')

        buyer_table['asp'] = buyer_table['asp'].astype('float')

        buyer_table['value_gain_share'] = buyer_table['vols_gain_share'] * buyer_table['asp']
        buyer_table['value_loss_share'] = buyer_table['vols_loss_share'] * buyer_table['asp']
        buyer_table['predicted_value_share'] = buyer_table['predicted_volume_share'] * buyer_table['asp']

        buyer_table_share = buyer_table.groupby(['buyer'], as_index=False).agg(
            {'predicted_volume_share': sum, 'vols_gain_share': sum, 'vols_loss_share': sum,
             'predicted_value_share': sum, 'value_gain_share': sum,
             'value_loss_share': sum})

        buyer_table_share['vol_impact'] = buyer_table_share['vols_gain_share'] - buyer_table_share['vols_loss_share']
        buyer_table_share['value_impact'] = buyer_table_share['value_gain_share'] - buyer_table_share[
            'value_loss_share']

        buyer_table_share['predicted_volume_share'] = buyer_table_share['predicted_volume_share'].replace(0, 1)
        buyer_table_share['predicted_value_share'] = buyer_table_share['predicted_value_share'].replace(0, 1)

        try:
            buyer_table_share['vol_impact_per'] = (buyer_table_share['vol_impact'] * 100) / buyer_table_share[
                'predicted_volume_share']
        except:
            buyer_table_share['vol_impact_per'] = 0

        try:
            buyer_table_share['value_impact_per'] = (buyer_table_share['value_impact'] * 100) / buyer_table_share[
                'predicted_value_share']
        except:
            buyer_table_share['value_impact_per'] = 0

        buyer_sales_table = buyer_table_share[buyer_table_share['vol_impact'] != 0]

        buyer_sales_table = buyer_sales_table[
            ['buyer', 'predicted_volume_share', 'vols_gain_share', 'vols_loss_share', 'vol_impact',
             'vol_impact_per', 'predicted_value_share', 'value_gain_share', 'value_loss_share', 'value_impact',
             'value_impact_per']]
        buyer_sales_table['vol_impact_per'] = buyer_sales_table['vol_impact_per'].round(decimals=1)
        buyer_sales_table['value_impact_per'] = buyer_sales_table['value_impact_per'].round(decimals=1)
        buyer_sales_table = buyer_sales_table.drop_duplicates().fillna(0).reset_index(drop=True)
        return buyer_sales_table

    #psg impact for delist table
    def psg_impact(self,bc, delist_main, store, future):
        psg = read_frame(product_hierarchy.objects.all().filter(buying_controller__in=bc,
                                                                base_product_number__in=delist_main).values(
            'product_sub_group_code', 'base_product_number', 'product_sub_group_description').distinct())
        psg_list = list(psg['product_sub_group_code'])

        psg_impact = read_frame(product_hierarchy.objects.all().filter(product_sub_group_code__in=psg_list).values(
            'product_sub_group_description', 'base_product_number').distinct())

        prod_price_data = read_frame(
            product_price.objects.filter(buying_controller__in=bc, store_type__in=store).values(
                'base_product_number', 'asp'))
        prod_price_data = prod_price_data.drop_duplicates().fillna(0).reset_index(drop=True)

        contribution_main = read_frame(
            product_contri.objects.all().filter(buying_controller__in=bc, store_type__in=store,
                                                time_period__in=future).values_list())
        contribution_main = contribution_main.rename(columns={'base_product_number': 'productcode'})

        bc_predict_main = pd.merge(contribution_main, prod_price_data, left_on=['productcode'],
                                   right_on=['base_product_number'], how='left')
        bc_predict_main = bc_predict_main.drop_duplicates().fillna(0).reset_index(drop=True)

        bc_predict_main['predicted_volume'] = bc_predict_main['predicted_volume'].astype('float')
        bc_predict_main['asp'] = bc_predict_main['asp'].astype('float')

        bc_predict_main['predicted_sales'] = bc_predict_main['predicted_volume'] * bc_predict_main['asp']
        # bc_predict_main = bc_predict_main[['base_product_number','predicted_volume','predicted_sales','asp']]

        psg_predict_main = pd.merge(bc_predict_main, psg_impact, left_on=['base_product_number'],
                                    right_on=['base_product_number'], how='inner')
        psg_predict_main = psg_predict_main.drop_duplicates().fillna(0).reset_index(drop=True)
        psg_predict_main['predicted_volume'] = psg_predict_main['predicted_volume'].astype('float')

        psg_predict_main['predicted_sales'] = psg_predict_main['predicted_volume'] * psg_predict_main['asp']
        # psg_predict_main = psg_predict_main[['base_product_number','predicted_volume','predicted_sales','predicted_cgm','predicted_cts']]
        psg_predict_main = psg_predict_main.groupby(['product_sub_group_description'], as_index=False).agg(
            {'predicted_sales': sum})
        psg_predict_main = psg_predict_main.rename(columns={'predicted_sales': 'psg_predicted_sales'})
        return psg_predict_main

# chart
class product_impact_chart(vol_transfer_logic,APIView):

    def get(self, request, *args):
        #reading args from url
        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        args.pop('format__iexact', None)

        #pop below fields for user auth
        designation = args.pop('designation__iexact', None)
        user_id = args.pop('user_id__iexact', None)
        session_id = args.pop('session_id__iexact', None)
        user_name = args.pop('user_name__iexact', None)
        buying_controller_header = args.pop('buying_controller_header__iexact', None)
        buyer_header = args.pop('buyer_header__iexact', None)

        #reading list of products to delist

        args_list = {reqobj + '__in': request.GET.getlist(reqobj) for reqobj in request.GET.keys()}
        #print(args_list)


        designation = args_list.pop('designation__in', None)
        user_id = args_list.pop('user_id__in', None)
        session_id = args_list.pop('session_id__in', None)
        user_name = args_list.pop('user_name__in', None)
        buying_controller_header = args_list.pop('buying_controller_header__in', None)
        buyer_header = args_list.pop('buyer_header__in', None)

        vol_logic = vol_transfer_logic()

        if not args:
            bc = ['Meat Fish and Veg']
            store = ['Overview']
            future = ['3_months']
            input_tpns = 0
            product_impact_filter.objects.all().delete()
            instance_insert = product_impact_filter.objects.create(input_tpns=input_tpns, future=future[0],
                                                                   store=store[0], bc=bc[0])

        else:
            bc = args.get('buying_controller__iexact')
            if bc is not None:
                bc = bc.replace('__', '&')
                bc = [bc]
            else:
                bc = ['Meat Fish and Veg']
            store = args.get('store_type__iexact')
            if store is not None:
                store = [store]
            else:
                store = ['Overview']

            future = args.get('time_period__iexact')
            if future is None:
                future = '13_weeks'
            if future == '13_weeks':
                future = ['3_months']
            elif future == '26_weeks':
                future = ['6_months']
            else:
                future = ['12_months']


            input_tpns = args_list.pop('long_description__in', 0)
            #print(input_tpns)
            input_tpns_main = pd.DataFrame(input_tpns).reset_index(drop=True)
            input_tpns_main['base_product_number'] = input_tpns_main[0].copy()
            input_tpns_main['base_product_number'] = input_tpns_main['base_product_number'].str[-8:]
            input_tpns_main['base_product_number'] = input_tpns_main['base_product_number'].astype('int')
            input_tpns = input_tpns_main['base_product_number'].drop_duplicates().values.tolist()

            #print('filter at product_impact_chart \n')
            #print(type(input_tpns))
            #print(input_tpns)

            product_impact_filter.objects.all().delete()

            if input_tpns == 0 :
                instance_insert = product_impact_filter.objects.create(input_tpns=input_tpns, future=future[0],
                                                                       store=store[0], bc=bc[0])
            else:
                for i,val in enumerate(input_tpns):
                    instance_insert = product_impact_filter.objects.create(input_tpns=val, future=future[0],
                                                                           store=store[0], bc=bc[0])


        #insert values into the model for the filters selected
        #print('Product_impact_chart')
        if store == ['Overview']:
            #print("#######overview###############")
            if input_tpns == 0:
                input_tpns_main = read_frame(
                    nego_ads_drf.objects.all().filter(buying_controller__in=bc, store_type__in=['Main Estate'],
                                                      performance_quartile__in=['Low CPS/Low Profit'],
                                                      time_period__in=['Last 52 Weeks']).values(
                        'base_product_number').distinct())

                delist_main = list(input_tpns_main['base_product_number'])
                #print(delist_main)
            else:
                delist_main = input_tpns
                #input_tpns_main = pd.DataFrame(input_tpns).reset_index(drop=True)
                #input_tpns_main['base_product_number'] = input_tpns_main[0].copy()
                #input_tpns_main['base_product_number'] = input_tpns_main['base_product_number'].str[-8:]
                #input_tpns_main['base_product_number'] = input_tpns_main['base_product_number'].astype('int')
                #delist_main = input_tpns_main['base_product_number'].drop_duplicates().values.tolist()

            #print('below values are passed - main estate')
            #print(bc, store, future, input_tpns_main,delist_main)

            # In[6]:
            #call for main estate

            product_dataset_main = vol_logic.volume_transfer_logic(bc, ['Main Estate'], future, input_tpns_main, delist_main)
            #print("#printing main dataset")
            #print(product_dataset_main)

            if input_tpns == 0:
                input_tpns_exp = read_frame(
                    nego_ads_drf.objects.all().filter(buying_controller__in=bc, store_type__in=['Express'],
                                                      performance_quartile__in=['Low CPS/Low Profit'],
                                                      time_period__in=['Last 52 Weeks']).values(
                        'base_product_number').distinct())

                delist_exp = list(input_tpns_exp['base_product_number'])
            else:
                input_tpns_exp=input_tpns_main
                delist_exp = input_tpns
                #input_tpns_exp = pd.DataFrame(input_tpns).reset_index(drop=True)
                #input_tpns_exp['base_product_number'] = input_tpns_exp[0].copy()
                #input_tpns_exp['base_product_number'] = input_tpns_exp['base_product_number'].str[-8:]
                #input_tpns_exp['base_product_number'] = input_tpns_exp['base_product_number'].astype('int')
                #delist_exp = input_tpns_exp['base_product_number'].drop_duplicates().values.tolist()
            #call for Express
            product_dataset_exp = vol_logic.volume_transfer_logic(bc, ['Express'], future, input_tpns_exp, delist_exp)
            product_dataset_exp.head()

            # In[9]:

            product_dataset = pd.merge(product_dataset_main, product_dataset_exp,
                                       left_on=['productcode', 'substituteproductcode'],
                                       right_on=['productcode', 'substituteproductcode'], how='outer')

            # In[10]:

            product_dataset = product_dataset.drop_duplicates().fillna(0).reset_index(drop=True)
            # In[11]:

            product_dataset['predicted_volume'] = product_dataset['predicted_volume_x'] + product_dataset[
                'predicted_volume_y']
            product_dataset['predicted_value'] = product_dataset['predicted_value_x'] + product_dataset[
                'predicted_value_y']
            product_dataset['predicted_cgm'] = product_dataset['predicted_cgm_x'] + product_dataset['predicted_cgm_y']
            product_dataset['predicted_cts'] = product_dataset['predicted_cts_x'] + product_dataset['predicted_cts_y']
            product_dataset['volume_transfer'] = product_dataset['volume_transfer_x'] + product_dataset[
                'volume_transfer_y']
            product_dataset['value_transfer'] = product_dataset['value_transfer_x'] + product_dataset[
                'value_transfer_y']
            product_dataset['cgm_transfer'] = product_dataset['cgm_transfer_x'] + product_dataset['cgm_transfer_y']
            product_dataset['cts_transfer'] = product_dataset['cts_transfer_x'] + product_dataset['cts_transfer_y']
            product_dataset['brand_indicator_x'] = product_dataset['brand_indicator_x'].fillna(
                product_dataset['brand_indicator_y'])
            product_dataset['brand_indicator_y'] = product_dataset['brand_indicator_x'].fillna(
                product_dataset['brand_indicator_y'])
            product_dataset['brand_indicator'] = product_dataset.brand_indicator_x.combine_first(
                product_dataset.brand_indicator_y)
            del product_dataset["brand_indicator_x"]
            del product_dataset["brand_indicator_y"]

            product_dataset = product_dataset[
                ['productcode', 'substituteproductcode', 'brand_indicator', 'predicted_volume', 'predicted_value',
                 'predicted_cgm', 'predicted_cts', 'volume_transfer', 'value_transfer', 'cgm_transfer', 'cts_transfer']]



            # In[13]:
            #waterfall charts
            initial_volume = product_dataset[['productcode', 'predicted_volume']].drop_duplicates().reset_index(
                drop=True).groupby(['productcode'], as_index = False).agg({'predicted_volume':max})

            initial_volume = initial_volume['predicted_volume'].sum()
            volume_transfer_brand = product_dataset[product_dataset['brand_indicator'] == "B"].groupby(
                ['brand_indicator'], as_index=False).agg({'volume_transfer': sum})
            volume_transfer_ownlabel = product_dataset[product_dataset['brand_indicator'] == "T"].groupby(
                ['brand_indicator'], as_index=False).agg({'volume_transfer': sum})
            volume_final_loss = initial_volume - product_dataset['volume_transfer'].sum()

            vols_waterfall = pd.DataFrame()
            vols_waterfall['name'] = []
            vols_waterfall['value'] = []
            vols_waterfall.ix[0, 'name'] = 'Volume lost from delist products'
            vols_waterfall.ix[1, 'name'] = 'Volume transfer to substitute brand'
            vols_waterfall.ix[2, 'name'] = 'Volume transfer to substitute OL'

            vols_waterfall.ix[0, 'value'] = initial_volume
            if volume_transfer_brand.empty:
                vols_waterfall.ix[1, 'value'] = 0
            else:
                vols_waterfall.ix[1, 'value'] = -volume_transfer_brand['volume_transfer'].iloc[0]

            if volume_transfer_ownlabel.empty:
                vols_waterfall.ix[2, 'value'] = 0
            else:
                vols_waterfall.ix[2, 'value'] = -volume_transfer_ownlabel['volume_transfer'].iloc[0]
            vols_waterfall = vols_waterfall.to_dict(orient='records')
            if initial_volume == 0:
                vol_tot_transfer = 0
            else:
                vol_tot_transfer = (product_dataset['volume_transfer'].sum() / initial_volume)*100
                vol_tot_transfer = (format(vol_tot_transfer, '.1f'))
                vol_tot_transfer = float(vol_tot_transfer)


            # In[45]:
            #sales chart
            initial_sales = product_dataset[['productcode', 'predicted_value']].drop_duplicates().reset_index(drop=True).groupby(['productcode'], as_index = False).agg({'predicted_value':max})
            initial_sales = initial_sales['predicted_value'].sum()
            sales_transfer_brand = product_dataset[product_dataset['brand_indicator'] == "B"].groupby(
                ['brand_indicator'], as_index=False).agg({'value_transfer': sum})
            sales_transfer_ownlabel = product_dataset[product_dataset['brand_indicator'] == "T"].groupby(
                ['brand_indicator'], as_index=False).agg({'value_transfer': sum})
            sales_final_loss = initial_sales - product_dataset['value_transfer'].sum()

            sales_waterfall = pd.DataFrame()
            sales_waterfall['name'] = []
            sales_waterfall['value'] = []
            sales_waterfall.ix[0, 'name'] = 'Value lost from delist products'
            sales_waterfall.ix[1, 'name'] = 'Value transfer to substitute brand'
            sales_waterfall.ix[2, 'name'] = 'Value transfer to substitute OL'

            sales_waterfall.ix[0, 'value'] = initial_sales
            if sales_transfer_brand.empty:
                sales_waterfall.ix[1, 'value'] = 0
            else:
                sales_waterfall.ix[1, 'value'] = -sales_transfer_brand['value_transfer'].iloc[0]
            if  sales_transfer_ownlabel.empty:
                sales_waterfall.ix[2, 'value'] = 0
            else:
                sales_waterfall.ix[2, 'value'] = -sales_transfer_ownlabel['value_transfer'].iloc[0]
            sales_waterfall = sales_waterfall.to_dict(orient='records')
            if initial_sales == 0:
                sales_tot_transfer = 0
            else:
                sales_tot_transfer = (product_dataset['value_transfer'].sum() / initial_sales) * 100
                sales_tot_transfer = (format(sales_tot_transfer, '.1f'))
                sales_tot_transfer = float(sales_tot_transfer)
            # In[46]:
            #cgm chart
            initial_cgm = product_dataset[['productcode', 'predicted_cgm']].drop_duplicates().reset_index(drop=True).groupby(['productcode'], as_index = False).agg({'predicted_cgm':max})
            initial_cgm = initial_cgm['predicted_cgm'].sum()
            cgm_transfer_brand = product_dataset[product_dataset['brand_indicator'] == "B"].groupby(['brand_indicator'],
                                                                                                    as_index=False).agg(
                {'cgm_transfer': sum})
            cgm_transfer_ownlabel = product_dataset[product_dataset['brand_indicator'] == "T"].groupby(
                ['brand_indicator'], as_index=False).agg({'cgm_transfer': sum})
            cgm_final_loss = initial_cgm - product_dataset['cgm_transfer'].sum()

            cgm_waterfall = pd.DataFrame()
            cgm_waterfall['name'] = []
            cgm_waterfall['value'] = []
            cgm_waterfall.ix[0, 'name'] = 'CGM lost from delist products'
            cgm_waterfall.ix[1, 'name'] = 'CGM transfer to substitute brand'
            cgm_waterfall.ix[2, 'name'] = 'CGM transfer to substitute OL'

            cgm_waterfall.ix[0, 'value'] = initial_cgm
            if cgm_transfer_brand.empty:
                cgm_waterfall.ix[1, 'value'] = 0
            else:
                cgm_waterfall.ix[1, 'value'] = -cgm_transfer_brand['cgm_transfer'].iloc[0]
            if cgm_transfer_ownlabel.empty:
                cgm_waterfall.ix[2, 'value'] = 0
            else:
                cgm_waterfall.ix[2, 'value'] = -cgm_transfer_ownlabel['cgm_transfer'].iloc[0]
            cgm_waterfall = cgm_waterfall.to_dict(orient='records')
            if initial_cgm == 0:
                cgm_tot_transfer =0
            else:
                cgm_tot_transfer = (product_dataset['cgm_transfer'].sum() / initial_cgm) * 100
                cgm_tot_transfer = (format(cgm_tot_transfer, '.1f'))
                cgm_tot_transfer = float(cgm_tot_transfer)

            # In[47]:
            #cts chart
            initial_cts = product_dataset[['productcode', 'predicted_cts']].drop_duplicates().reset_index(drop=True).groupby(['productcode'], as_index = False).agg({'predicted_cts':max})
            initial_cts = initial_cts['predicted_cts'].sum()
            cts_transfer_brand = product_dataset[product_dataset['brand_indicator'] == "B"].groupby(['brand_indicator'],
                                                                                                    as_index=False).agg(
                {'cts_transfer': sum})
            cts_transfer_ownlabel = product_dataset[product_dataset['brand_indicator'] == "T"].groupby(
                ['brand_indicator'], as_index=False).agg({'cts_transfer': sum})
            cts_final_loss = initial_cts - product_dataset['cts_transfer'].sum()

            cts_waterfall = pd.DataFrame()
            cts_waterfall['name'] = []
            cts_waterfall['value'] = []
            cts_waterfall.ix[0, 'name'] = 'CTS gain from delist products'
            cts_waterfall.ix[1, 'name'] = 'CTS transfer to substitute brand'
            cts_waterfall.ix[2, 'name'] = 'CTS transfer to substitute OL'

            cts_waterfall.ix[0, 'value'] = initial_cts
            if cts_transfer_brand.empty:
                cts_waterfall.ix[1, 'value'] = 0
            else:
                cts_waterfall.ix[1, 'value'] = -cts_transfer_brand['cts_transfer'].iloc[0]
            if cts_transfer_ownlabel.empty:
                cts_waterfall.ix[2, 'value'] = 0
            else:
                cts_waterfall.ix[2, 'value'] = -cts_transfer_ownlabel['cts_transfer'].iloc[0]
            cts_waterfall = cts_waterfall.to_dict(orient='records')
            if initial_cts == 0:
                cts_tot_transfer = 0
            else:
                cts_tot_transfer = (product_dataset['cts_transfer'].sum() / initial_cts) * 100
                cts_tot_transfer = (format(cts_tot_transfer, '.1f'))
                cts_tot_transfer = float(cts_tot_transfer)

            # In[14]:

            # PSGs of delisted products
            psg = read_frame(product_hierarchy.objects.all().filter(buying_controller__in=bc,
                                                                    base_product_number__in=delist_main).values(
                'product_sub_group_code').distinct())
            psg = list(psg['product_sub_group_code'])

            psg_impact = read_frame(product_hierarchy.objects.all().filter(product_sub_group_code__in=psg).values(
                'base_product_number').distinct())

            prod_price_data = read_frame(
                product_price.objects.filter(buying_controller__in=bc, store_type__in=['Main Estate']).values(
                    'base_product_number', 'asp', 'acp'))
            prod_price_data = prod_price_data.drop_duplicates().fillna(0).reset_index(drop=True)

            cts_main = read_frame(
                cts_data.objects.filter(buying_controller__in=bc, store_type__in=['Main Estate']).all())

            contribution_main = read_frame(
                product_contri.objects.all().filter(buying_controller__in=bc, store_type__in=['Main Estate'],
                                                    time_period__in=future).values_list())

            contribution_main = contribution_main.rename(columns={'base_product_number': 'productcode'})

            bc_predict_main = pd.merge(contribution_main, prod_price_data, left_on=['productcode'],
                                       right_on=['base_product_number'], how='left')
            bc_predict_main = pd.merge(bc_predict_main, cts_main, left_on=['base_product_number'],
                                       right_on=['base_product_number'], how='left')
            bc_predict_main = bc_predict_main.drop_duplicates().fillna(0).reset_index(drop=True)

            bc_predict_main['predicted_volume'] = bc_predict_main['predicted_volume'].astype('float')
            bc_predict_main['asp'] = bc_predict_main['asp'].astype('float')
            bc_predict_main['acp'] = bc_predict_main['acp'].astype('float')
            bc_predict_main['cts_per_unit'] = bc_predict_main['cts_per_unit'].astype('float')

            bc_predict_main['predicted_sales'] = bc_predict_main['predicted_volume'] * bc_predict_main['asp']
            bc_predict_main['predicted_cgm'] = bc_predict_main['predicted_volume'] * (
            bc_predict_main['asp'] - bc_predict_main['acp'])
            bc_predict_main['predicted_cts'] = bc_predict_main['predicted_volume'] * bc_predict_main['cts_per_unit']
            bc_predict_main = bc_predict_main[
                ['base_product_number', 'predicted_volume', 'predicted_sales', 'predicted_cgm', 'predicted_cts', 'asp',
                 'acp', 'cts_per_unit']]

            psg_predict_main = pd.merge(bc_predict_main, psg_impact, left_on=['base_product_number'],
                                        right_on=['base_product_number'], how='inner')
            psg_predict_main = psg_predict_main.drop_duplicates().fillna(0).reset_index(drop=True)
            psg_predict_main['predicted_volume'] = psg_predict_main['predicted_volume'].astype('float')

            psg_predict_main['predicted_sales'] = psg_predict_main['predicted_volume'] * psg_predict_main['asp']
            psg_predict_main['predicted_cgm'] = psg_predict_main['predicted_volume'] * (
            psg_predict_main['asp'] - psg_predict_main['acp'])
            psg_predict_main['predicted_cts'] = psg_predict_main['predicted_volume'] * psg_predict_main['cts_per_unit']
            psg_predict_main = psg_predict_main[
                ['base_product_number', 'predicted_volume', 'predicted_sales', 'predicted_cgm', 'predicted_cts']]

            # In[15]:

            psg_predict_main.head()

            # In[16]:

            # PSGs of delisted products
            psg = read_frame(product_hierarchy.objects.all().filter(buying_controller__in=bc,
                                                                    base_product_number__in=delist_exp).values(
                'product_sub_group_code').distinct())
            psg = list(psg['product_sub_group_code'])

            psg_impact = read_frame(product_hierarchy.objects.all().filter(product_sub_group_code__in=psg).values(
                'base_product_number').distinct())

            prod_price_data = read_frame(
                product_price.objects.filter(buying_controller__in=bc, store_type__in=['Express']).values(
                    'base_product_number', 'asp', 'acp'))
            prod_price_data = prod_price_data.drop_duplicates().fillna(0).reset_index(drop=True)

            cts_exp = read_frame(cts_data.objects.filter(buying_controller__in=bc, store_type__in=['Express']).all())

            contribution_exp = read_frame(
                product_contri.objects.all().filter(buying_controller__in=bc, store_type__in=['Express'],
                                                    time_period__in=future).values_list())

            contribution_exp = contribution_exp.rename(columns={'base_product_number': 'productcode'})

            bc_predict_exp = pd.merge(contribution_exp, prod_price_data, left_on=['productcode'],
                                      right_on=['base_product_number'], how='left')
            bc_predict_exp = pd.merge(bc_predict_exp, cts_exp, left_on=['base_product_number'],
                                      right_on=['base_product_number'], how='left')
            bc_predict_exp = bc_predict_exp.drop_duplicates().fillna(0).reset_index(drop=True)

            bc_predict_exp['predicted_volume'] = bc_predict_exp['predicted_volume'].astype('float')
            bc_predict_exp['asp'] = bc_predict_exp['asp'].astype('float')
            bc_predict_exp['acp'] = bc_predict_exp['acp'].astype('float')
            bc_predict_exp['cts_per_unit'] = bc_predict_exp['cts_per_unit'].astype('float')

            bc_predict_exp['predicted_sales'] = bc_predict_exp['predicted_volume'] * bc_predict_exp['asp']
            bc_predict_exp['predicted_cgm'] = bc_predict_exp['predicted_volume'] * (
            bc_predict_exp['asp'] - bc_predict_exp['acp'])
            bc_predict_exp['predicted_cts'] = bc_predict_exp['predicted_volume'] * bc_predict_exp['cts_per_unit']
            bc_predict_exp = bc_predict_exp[
                ['base_product_number', 'predicted_volume', 'predicted_sales', 'predicted_cgm', 'predicted_cts', 'asp',
                 'acp', 'cts_per_unit']]

            psg_predict_exp = pd.merge(bc_predict_exp, psg_impact, left_on=['base_product_number'],
                                       right_on=['base_product_number'], how='inner')
            psg_predict_exp = psg_predict_exp.drop_duplicates().fillna(0).reset_index(drop=True)
            psg_predict_exp['predicted_volume'] = psg_predict_exp['predicted_volume'].astype('float')

            psg_predict_exp['predicted_sales'] = psg_predict_exp['predicted_volume'] * psg_predict_exp['asp']
            psg_predict_exp['predicted_cgm'] = psg_predict_exp['predicted_volume'] * (
            psg_predict_exp['asp'] - psg_predict_exp['acp'])
            psg_predict_exp['predicted_cts'] = psg_predict_exp['predicted_volume'] * psg_predict_exp['cts_per_unit']
            psg_predict_exp = psg_predict_exp[
                ['base_product_number', 'predicted_volume', 'predicted_sales', 'predicted_cgm', 'predicted_cts']]

            # In[17]:

            bc_predict = pd.merge(bc_predict_main, bc_predict_exp, left_on=['base_product_number'],
                                  right_on=['base_product_number'], how='outer')
            bc_predict = bc_predict.drop_duplicates().fillna(0).reset_index(drop=True)
            bc_predict['predicted_volume'] = bc_predict['predicted_volume_x'] + bc_predict['predicted_volume_y']
            bc_predict['predicted_sales'] = bc_predict['predicted_sales_x'] + bc_predict['predicted_sales_y']
            bc_predict['predicted_cgm'] = bc_predict['predicted_cgm_x'] + bc_predict['predicted_cgm_y']
            bc_predict['predicted_cts'] = bc_predict['predicted_cts_x'] + bc_predict['predicted_cts_y']
            bc_predict = bc_predict[
                ['base_product_number', 'predicted_volume', 'predicted_sales', 'predicted_cgm', 'predicted_cts']]

            # In[18]:

            psg_predict = pd.merge(psg_predict_main, psg_predict_exp, left_on=['base_product_number'],
                                   right_on=['base_product_number'], how='outer')
            psg_predict = psg_predict.drop_duplicates().fillna(0).reset_index(drop=True)
            psg_predict['predicted_volume'] = psg_predict['predicted_volume_x'] + psg_predict['predicted_volume_y']
            psg_predict['predicted_sales'] = psg_predict['predicted_sales_x'] + psg_predict['predicted_sales_y']
            psg_predict['predicted_cgm'] = psg_predict['predicted_cgm_x'] + psg_predict['predicted_cgm_y']
            psg_predict['predicted_cts'] = psg_predict['predicted_cts_x'] + psg_predict['predicted_cts_y']
            psg_predict = psg_predict[
                ['base_product_number', 'predicted_volume', 'predicted_sales', 'predicted_cgm', 'predicted_cts']]

            # In[19]:

            bc_cgm = bc_predict['predicted_cgm'].sum()
            if bc_cgm == 0:
                bc_cgm_contri = 0
                bc_cgm_contri=float(bc_cgm_contri)
            else:
                bc_cgm_contri = (float(cgm_final_loss) / float(bc_cgm)) * (-100)
                bc_cgm_contri = (format(bc_cgm_contri, '.1f'))
                bc_cgm_contri=float(bc_cgm_contri)

            psg_cgm = psg_predict['predicted_cgm'].sum()
            if psg_cgm == 0:
                psg_cgm_contri = 0
                psg_cgm_contri = float(psg_cgm_contri)
            else:
                psg_cgm_contri = (float(cgm_final_loss) / float(psg_cgm)) * (-100)
                psg_cgm_contri = (format(psg_cgm_contri, '.1f'))
                psg_cgm_contri = float(psg_cgm_contri)

            bc_cts = bc_predict['predicted_cts'].sum()
            if bc_cts == 0:
                bc_cts_contri = 0
                bc_cts_contri = float(bc_cts_contri)
            else:
                bc_cts_contri = (float(cts_final_loss) / float(bc_cts)) * (100)
                bc_cts_contri = (format(bc_cts_contri, '.1f'))
                bc_cts_contri = float(bc_cts_contri)

            psg_cts = psg_predict['predicted_cts'].sum()
            if psg_cts == 0:
                psg_cts_contri = 0
                psg_cts_contri = float(psg_cts_contri)
            else:
                psg_cts_contri = (float(cts_final_loss) / float(psg_cts)) * (100)
                psg_cts_contri = (format(psg_cts_contri, '.1f'))
                psg_cts_contri = float(psg_cts_contri)

            bc_sales = bc_predict['predicted_sales'].sum()
            if bc_sales == 0:
                bc_sales_contri = 0
                bc_sales_contri = float(bc_sales_contri)
            else:
                bc_sales_contri = (float(sales_final_loss) / float(bc_sales)) * (-100)
                bc_sales_contri = (format(bc_sales_contri, '.1f'))
                bc_sales_contri = float(bc_sales_contri)

            psg_sales = psg_predict['predicted_sales'].sum()
            if psg_sales == 0:
                psg_sales_contri = 0
                psg_sales_contri = float(psg_sales_contri)
            else:
                psg_sales_contri = (float(sales_final_loss) / float(psg_sales)) * (-100)
                psg_sales_contri = (format(psg_sales_contri, '.1f'))
                psg_sales_contri = float(psg_sales_contri)

            bc_vols = bc_predict['predicted_volume'].sum()
            if bc_vols == 0:
                bc_vols_contri = 0
                bc_vols_contri =float(bc_vols_contri)
            else:
                bc_vols_contri = (float(volume_final_loss) / float(bc_vols)) * (-100)
                bc_vols_contri = (format(bc_vols_contri, '.1f'))
                bc_vols_contri =float(bc_vols_contri)

            psg_vols = psg_predict['predicted_volume'].sum()
            if psg_vols == 0:
                psg_vols_contri = 0
                psg_vols_contri = float(psg_vols_contri)
            else:
                psg_vols_contri = (float(volume_final_loss) / float(psg_vols)) * (-100)
                psg_vols_contri = (format(psg_vols_contri, '.1f'))
                psg_vols_contri = float(psg_vols_contri)

        else:

            if input_tpns==0:
                input_tpns = read_frame(nego_ads_drf.objects.all().filter(buying_controller__in=bc, store_type__in=store,
                                                      performance_quartile__in=['Low CPS/Low Profit'],
                                                      time_period__in=['Last 52 Weeks']).values('base_product_number').distinct())

                delist = list(input_tpns['base_product_number'])
            else:
                delist = input_tpns
                input_tpns = pd.DataFrame(input_tpns).reset_index(drop=True)
                input_tpns['base_product_number'] = input_tpns[0].copy()
                #input_tpns['base_product_number'] = input_tpns['base_product_number'].str[-8:]
                #input_tpns['base_product_number'] = input_tpns['base_product_number'].astype('int')
                delist = input_tpns['base_product_number'].drop_duplicates().values.tolist()

            # In[4]:
            product_dataset = vol_logic.volume_transfer_logic(bc, store, future, input_tpns, delist)


            # In[44]:
            prod_hrchy = read_frame(
                product_desc.objects.all().filter(buying_controller__in=bc).values('base_product_number',
                                                                                   'brand_indicator',
                                                                                   'long_description').distinct())

            initial_volume = product_dataset[['productcode', 'predicted_volume']].drop_duplicates().reset_index(
                drop=True).groupby(['productcode'], as_index = False).agg({'predicted_volume':max})
            initial_volume = initial_volume['predicted_volume'].sum()
            volume_transfer_brand = product_dataset[product_dataset['brand_indicator'] == "B"].groupby(
                ['brand_indicator'], as_index=False).agg({'volume_transfer': sum})
            volume_transfer_ownlabel = product_dataset[product_dataset['brand_indicator'] == "T"].groupby(
                ['brand_indicator'], as_index=False).agg({'volume_transfer': sum})
            volume_final_loss = initial_volume - product_dataset['volume_transfer'].sum()

            vols_waterfall = pd.DataFrame()
            vols_waterfall['name'] = []
            vols_waterfall['value'] = []
            vols_waterfall.ix[0, 'name'] = 'Volume lost from delist products'
            vols_waterfall.ix[1, 'name'] = 'Volume transfer to substitute brand'
            vols_waterfall.ix[2, 'name'] = 'Volume transfer to substitute OL'

            vols_waterfall.ix[0, 'value'] = initial_volume

            if volume_transfer_brand.empty:
                vols_waterfall.ix[1, 'value'] = 0
            else:
                vols_waterfall.ix[1, 'value'] = -volume_transfer_brand['volume_transfer'].iloc[0]

            if volume_transfer_ownlabel.empty:
                vols_waterfall.ix[2, 'value'] = 0
            else:
                vols_waterfall.ix[2, 'value'] = -volume_transfer_ownlabel['volume_transfer'].iloc[0]

            vols_waterfall = vols_waterfall.to_dict(orient='records')

            #print('#printing for volume transfer')
            #print(product_dataset['volume_transfer'].sum())
            #print(initial_volume)
            if initial_volume == 0:
                vol_tot_transfer =0
            else:
                vol_tot_transfer = (product_dataset['volume_transfer'].sum() / initial_volume) * 100
                vol_tot_transfer = (format(vol_tot_transfer, '.1f'))

            # In[45]:

            initial_sales = product_dataset[['productcode', 'predicted_value']].drop_duplicates().reset_index(drop=True).groupby(['productcode'], as_index = False).agg({'predicted_value':max})
            initial_sales = initial_sales['predicted_value'].sum()
            sales_transfer_brand = product_dataset[product_dataset['brand_indicator'] == "B"].groupby(
                ['brand_indicator'], as_index=False).agg({'value_transfer': sum})
            sales_transfer_ownlabel = product_dataset[product_dataset['brand_indicator'] == "T"].groupby(
                ['brand_indicator'], as_index=False).agg({'value_transfer': sum})
            sales_final_loss = initial_sales - product_dataset['value_transfer'].sum()

            sales_waterfall = pd.DataFrame()
            sales_waterfall['name'] = []
            sales_waterfall['value'] = []
            sales_waterfall.ix[0, 'name'] = 'Value lost from delist products'
            sales_waterfall.ix[1, 'name'] = 'Value transfer to substitute brand'
            sales_waterfall.ix[2, 'name'] = 'Value transfer to substitute OL'

            sales_waterfall.ix[0, 'value'] = initial_sales
            if sales_transfer_brand.empty:
                sales_waterfall.ix[1, 'value'] = 0
            else:
                sales_waterfall.ix[1, 'value'] = -sales_transfer_brand['value_transfer'].iloc[0]

            if sales_transfer_ownlabel.empty:
                sales_waterfall.ix[2, 'value'] = 0
            else:
                sales_waterfall.ix[2, 'value'] = -sales_transfer_ownlabel['value_transfer'].iloc[0]
            sales_waterfall = sales_waterfall.to_dict(orient='records')
            if initial_sales == 0:
                sales_tot_transfer = 0
            else:
                sales_tot_transfer = (product_dataset['value_transfer'].sum() / initial_sales) * 100
                sales_tot_transfer = (format(sales_tot_transfer, '.1f'))

            # In[46]:

            initial_cgm = product_dataset[['productcode', 'predicted_cgm']].drop_duplicates().reset_index(drop=True).groupby(['productcode'], as_index = False).agg({'predicted_cgm':max})
            initial_cgm = initial_cgm['predicted_cgm'].sum()
            cgm_transfer_brand = product_dataset[product_dataset['brand_indicator'] == "B"].groupby(['brand_indicator'],
                                                                                                    as_index=False).agg(
                {'cgm_transfer': sum})
            cgm_transfer_ownlabel = product_dataset[product_dataset['brand_indicator'] == "T"].groupby(
                ['brand_indicator'], as_index=False).agg({'cgm_transfer': sum})
            cgm_final_loss = initial_cgm - product_dataset['cgm_transfer'].sum()

            cgm_waterfall = pd.DataFrame()
            cgm_waterfall['name'] = []
            cgm_waterfall['value'] = []
            cgm_waterfall.ix[0, 'name'] = 'CGM lost from delist products'
            cgm_waterfall.ix[1, 'name'] = 'CGM transfer to substitute brand'
            cgm_waterfall.ix[2, 'name'] = 'CGM transfer to substitute OL'

            cgm_waterfall.ix[0, 'value'] = initial_cgm
            if cgm_transfer_brand.empty:
                cgm_waterfall.ix[1, 'value'] = 0
            else:
                cgm_waterfall.ix[1, 'value'] = -cgm_transfer_brand['cgm_transfer'].iloc[0]
            if cgm_transfer_ownlabel.empty:
                cgm_waterfall.ix[2, 'value'] = 0
            else:
                cgm_waterfall.ix[2, 'value'] = -cgm_transfer_ownlabel['cgm_transfer'].iloc[0]
            cgm_waterfall = cgm_waterfall.to_dict(orient='records')
            if initial_cgm == 0:
                cgm_tot_transfer = 0
            else:
                cgm_tot_transfer = (product_dataset['cgm_transfer'].sum() / initial_cgm) * 100
                cgm_tot_transfer = (format(cgm_tot_transfer, '.1f'))

            # In[47]:

            initial_cts = product_dataset[['productcode', 'predicted_cts']].drop_duplicates().reset_index(drop=True).groupby(['productcode'], as_index = False).agg({'predicted_cts':max})
            initial_cts = initial_cts['predicted_cts'].sum()
            cts_transfer_brand = product_dataset[product_dataset['brand_indicator'] == "B"].groupby(['brand_indicator'],
                                                                                                    as_index=False).agg(
                {'cts_transfer': sum})
            cts_transfer_ownlabel = product_dataset[product_dataset['brand_indicator'] == "T"].groupby(
                ['brand_indicator'], as_index=False).agg({'cts_transfer': sum})
            cts_final_loss = initial_cts - product_dataset['cts_transfer'].sum()

            cts_waterfall = pd.DataFrame()
            cts_waterfall['name'] = []
            cts_waterfall['value'] = []
            cts_waterfall.ix[0, 'name'] = 'CTS gain from delist products'
            cts_waterfall.ix[1, 'name'] = 'CTS transfer to substitute brand'
            cts_waterfall.ix[2, 'name'] = 'CTS transfer to substitute OL'

            cts_waterfall.ix[0, 'value'] = initial_cts
            if cts_transfer_brand.empty:
                cts_waterfall.ix[1, 'value'] = 0
            else:
                cts_waterfall.ix[1, 'value'] = -cts_transfer_brand['cts_transfer'].iloc[0]
            if cts_transfer_ownlabel.empty:
                cts_waterfall.ix[2, 'value'] = 0
            else:
                cts_waterfall.ix[2, 'value'] = -cts_transfer_ownlabel['cts_transfer'].iloc[0]
            cts_waterfall = cts_waterfall.to_dict(orient='records')
            if initial_cts == 0:
                cts_tot_transfer = 0
            else:
                cts_tot_transfer = (product_dataset['cts_transfer'].sum() / initial_cts) * 100
                cts_tot_transfer = (format(cts_tot_transfer, '.1f'))

            # In[48]:

            # PSGs of delisted products
            psg = read_frame(product_hierarchy.objects.all().filter(buying_controller__in=bc,
                                                                    base_product_number__in=delist).values(
                'product_sub_group_code').distinct())
            psg = list(psg['product_sub_group_code'])

            psg_impact = read_frame(product_hierarchy.objects.all().filter(product_sub_group_code__in=psg).values(
                'base_product_number').distinct())

            prod_price_data = read_frame(
                product_price.objects.filter(buying_controller__in=bc, store_type__in=store).values(
                    'base_product_number', 'asp', 'acp'))
            prod_price_data = prod_price_data.drop_duplicates().fillna(0).reset_index(drop=True)

            cts = read_frame(
                cts_data.objects.filter(buying_controller__in=bc, store_type__in=store).all())

            contribution = read_frame(
                product_contri.objects.all().filter(buying_controller__in=bc, store_type__in=store,
                                                    time_period__in=future).values_list())

            contribution = contribution.rename(columns={'base_product_number': 'productcode'})

            bc_predict = pd.merge(contribution, prod_price_data, left_on=['productcode'],
                                       right_on=['base_product_number'], how='left')
            bc_predict = pd.merge(bc_predict, cts, left_on=['base_product_number'],
                                       right_on=['base_product_number'], how='left')
            bc_predict = bc_predict.drop_duplicates().fillna(0).reset_index(drop=True)

            bc_predict['predicted_volume'] = bc_predict['predicted_volume'].astype('float')
            bc_predict['asp'] = bc_predict['asp'].astype('float')
            bc_predict['acp'] = bc_predict['acp'].astype('float')
            bc_predict['cts_per_unit'] = bc_predict['cts_per_unit'].astype('float')

            bc_predict['predicted_sales'] = bc_predict['predicted_volume'] * bc_predict['asp']
            bc_predict['predicted_cgm'] = bc_predict['predicted_volume'] * (bc_predict['asp'] - bc_predict['acp'])
            bc_predict['predicted_cts'] = bc_predict['predicted_volume'] * bc_predict['cts_per_unit']
            bc_predict = bc_predict[['base_product_number', 'predicted_volume', 'predicted_sales', 'predicted_cgm', 'predicted_cts', 'asp',
                 'acp', 'cts_per_unit']]

            psg_predict = pd.merge(bc_predict, psg_impact, left_on=['base_product_number'],
                                        right_on=['base_product_number'], how='inner')
            psg_predict = psg_predict.drop_duplicates().fillna(0).reset_index(drop=True)
            psg_predict['predicted_volume'] = psg_predict['predicted_volume'].astype('float')

            psg_predict['predicted_sales'] = psg_predict['predicted_volume'] * psg_predict['asp']
            psg_predict['predicted_cgm'] = psg_predict['predicted_volume'] * (psg_predict['asp'] - psg_predict['acp'])
            psg_predict['predicted_cts'] = psg_predict['predicted_volume'] * psg_predict['cts_per_unit']
            psg_predict_main = psg_predict[
                ['base_product_number', 'predicted_volume', 'predicted_sales', 'predicted_cgm', 'predicted_cts']]

            # In[49]:

            bc_cgm = bc_predict['predicted_cgm'].sum()
            if bc_cgm == 0:
                bc_cgm_contri = 0
            else:
                bc_cgm_contri = (float(cgm_final_loss) / float(bc_cgm)) * (-100)
                bc_cgm_contri = (format(bc_cgm_contri, '.1f'))

            psg_cgm = psg_predict['predicted_cgm'].sum()
            if psg_cgm == 0:
                psg_cgm_contri = 0
            else:
                psg_cgm_contri = (float(cgm_final_loss) / float(psg_cgm)) * (-100)
                psg_cgm_contri = (format(psg_cgm_contri, '.1f'))

            bc_cts = bc_predict['predicted_cts'].sum()
            if bc_cts == 0:
                bc_cts_contri = 0
            else:
                bc_cts_contri = (float(cts_final_loss) / float(bc_cts)) * (100)
                bc_cts_contri = (format(bc_cts_contri, '.1f'))

            psg_cts = psg_predict['predicted_cts'].sum()
            if psg_cts == 0:
                psg_cts_contri = 0
            else:
                psg_cts_contri = (float(cts_final_loss) / float(psg_cts)) * (100)
                psg_cts_contri = (format(psg_cts_contri, '.1f'))

            bc_sales = bc_predict['predicted_sales'].sum()
            if bc_sales == 0:
                bc_sales_contri = 0
            else:
                bc_sales_contri = (float(sales_final_loss) / float(bc_sales)) * (-100)
                bc_sales_contri = (format(bc_sales_contri, '.1f'))

            psg_sales = psg_predict['predicted_sales'].sum()
            if psg_sales == 0:
                psg_sales_contri = 0
            else:
                psg_sales_contri = (float(sales_final_loss) / float(psg_sales)) * (-100)
                psg_sales_contri = (format(psg_sales_contri, '.1f'))

            bc_vols = bc_predict['predicted_volume'].sum()
            if bc_vols == 0:
                bc_vols_contri = 0
            else:
                bc_vols_contri = (float(volume_final_loss) / float(bc_vols)) * (-100)
                bc_vols_contri = (format(bc_vols_contri, '.1f'))

            psg_vols = psg_predict['predicted_volume'].sum()
            if psg_vols == 0:
                psg_vols_contri = 0
            else:
                psg_vols_contri = (float(volume_final_loss) / float(psg_vols)) * (-100)
                psg_vols_contri = (format(psg_vols_contri, '.1f'))

        if vol_tot_transfer == 0:
            input_tpns = pd.DataFrame(input_tpns)
            data = {}
        else:
            data = {
                'cgm_chart': cgm_waterfall,
                'cts_chart': cts_waterfall,
                'sales_chart': sales_waterfall,
                'vols_chart': vols_waterfall,
                'bc_vols_contri': bc_vols_contri,
                'bc_sales_contri': bc_sales_contri,
                'bc_cgm_contri': bc_cgm_contri,
                'bc_cts_contri': bc_cts_contri,
                'psg_vols_contri': psg_vols_contri,
                'psg_sales_contri': psg_sales_contri,
                'psg_cgm_contri': psg_cgm_contri,
                'psg_cts_contri': psg_cts_contri,
                'vol_tot_transfer' : vol_tot_transfer,
                'sales_tot_transfer': sales_tot_transfer,
                'cgm_tot_transfer': cgm_tot_transfer,
                'cts_tot_transfer': cts_tot_transfer
            }
        return JsonResponse(data, safe=False)

# supplier table
class product_impact_supplier_table(vol_transfer_logic,APIView):
    def get(self, request, *args):

        # reading all the user selected filter values from the table


        all_filter = read_frame(product_impact_filter.objects.all().distinct())
        input_tpns = all_filter['input_tpns']
        input_tpns = list(input_tpns)
        bc=all_filter['bc'][0]
        bc=[bc]
        #print(type(bc))
        store = all_filter['store'][0]
        store=[store]
        #print(type(store))
        future = all_filter['future'][0]
        future=[future]
        vol_logic = vol_transfer_logic()
        #Logic for overview
        if store == ['Overview']:

            if input_tpns[0] == 0:
                input_tpns_main = read_frame(
                    nego_ads_drf.objects.all().filter(buying_controller__in=bc, store_type__in=['Main Estate'],
                                                      performance_quartile__in=['Low CPS/Low Profit'],
                                                      time_period__in=['Last 52 Weeks']).values(
                        'base_product_number').distinct())


                delist_main = list(input_tpns_main['base_product_number'])
            else:
                # delist_main = input_tpns
                input_tpns_main = pd.DataFrame(input_tpns).reset_index(drop=True)
                input_tpns_main['base_product_number'] = input_tpns_main[0].copy()
                #input_tpns_main['base_product_number'] = input_tpns_main['base_product_number'].str[-8:]
                #input_tpns_main['base_product_number'] = input_tpns_main['base_product_number'].astype('int')
                delist_main = input_tpns_main['base_product_number'].drop_duplicates().values.tolist()

            #call transfer logic for main estate
            product_dataset_main = vol_logic.volume_transfer_logic(bc, ['Main Estate'], future, input_tpns_main, delist_main)
            product_dataset_main.head()

            # In[7]:

            if input_tpns[0] == 0:
                input_tpns_exp = read_frame(
                    nego_ads_drf.objects.all().filter(buying_controller__in=bc, store_type__in=['Express'],
                                                      performance_quartile__in=['Low CPS/Low Profit'],
                                                      time_period__in=['Last 52 Weeks']).values(
                        'base_product_number').distinct())

                delist_exp = list(input_tpns_exp['base_product_number'])
            else:
                # delist_exp = input_tpns
                input_tpns_exp = pd.DataFrame(input_tpns).reset_index(drop=True)
                input_tpns_exp['base_product_number'] = input_tpns_exp[0].copy()
                #input_tpns_exp['base_product_number'] = input_tpns_exp['base_product_number'].str[-8:]
                #input_tpns_exp['base_product_number'] = input_tpns_exp['base_product_number'].astype('int')
                delist_exp = input_tpns_exp['base_product_number'].drop_duplicates().values.tolist()
            #call volume transfer logic for express
            product_dataset_exp = vol_logic.volume_transfer_logic(bc, ['Express'], future, input_tpns_exp, delist_exp)

            # In[9]:

            product_dataset = pd.merge(product_dataset_main, product_dataset_exp,
                                       left_on=['productcode', 'substituteproductcode'],
                                       right_on=['productcode', 'substituteproductcode'], how='outer')

            # In[10]:

            product_dataset = product_dataset.drop_duplicates().fillna(0).reset_index(drop=True)

            # In[11]:

            product_dataset['predicted_volume'] = product_dataset['predicted_volume_x'] + product_dataset[
                'predicted_volume_y']
            product_dataset['predicted_value'] = product_dataset['predicted_value_x'] + product_dataset[
                'predicted_value_y']
            product_dataset['predicted_cgm'] = product_dataset['predicted_cgm_x'] + product_dataset['predicted_cgm_y']
            product_dataset['predicted_cts'] = product_dataset['predicted_cts_x'] + product_dataset['predicted_cts_y']
            product_dataset['volume_transfer'] = product_dataset['volume_transfer_x'] + product_dataset[
                'volume_transfer_y']
            product_dataset['value_transfer'] = product_dataset['value_transfer_x'] + product_dataset[
                'value_transfer_y']
            product_dataset['cgm_transfer'] = product_dataset['cgm_transfer_x'] + product_dataset['cgm_transfer_y']
            product_dataset['cts_transfer'] = product_dataset['cts_transfer_x'] + product_dataset['cts_transfer_y']
            product_dataset['brand_indicator_x'] = product_dataset['brand_indicator_x'].fillna(
                product_dataset['brand_indicator_y'])
            product_dataset['brand_indicator_y'] = product_dataset['brand_indicator_x'].fillna(
                product_dataset['brand_indicator_y'])
            product_dataset['brand_indicator'] = product_dataset.brand_indicator_x.combine_first(
                product_dataset.brand_indicator_y)
            del product_dataset["brand_indicator_x"]
            del product_dataset["brand_indicator_y"]

            product_dataset = product_dataset[
                ['productcode', 'substituteproductcode', 'brand_indicator', 'predicted_volume', 'predicted_value',
                 'predicted_cgm', 'predicted_cts', 'volume_transfer', 'value_transfer', 'cgm_transfer', 'cts_transfer']]

            # supplier sales impact - table in UI
            #For overview logic
            sup_table_main, sup_sales_table_main = vol_logic.supplier_table(product_dataset_main, ['Main Estate'],
                                                                            future, bc)
            sup_table_exp, sup_sales_table_exp = vol_logic.supplier_table(product_dataset_exp, ['Express'], future, bc)

            #aggregation of tables start here
            sup_sales_table = pd.merge(sup_sales_table_main, sup_sales_table_exp, left_on=['parent_supplier'],
                                       right_on=['parent_supplier'], how='outer')
            sup_sales_table.head(2)

            # In[30]:
            sup_sales_table = sup_sales_table.drop_duplicates().reset_index(drop=True).fillna(0)
            sup_sales_table['predicted_volume_share'] = sup_sales_table['predicted_volume_share_x'] + sup_sales_table[
                'predicted_volume_share_y']
            sup_sales_table['vols_gain_share'] = sup_sales_table['vols_gain_share_x'] + sup_sales_table[
                'vols_gain_share_y']
            sup_sales_table['vols_loss_share'] = sup_sales_table['vols_loss_share_x'] + sup_sales_table[
                'vols_loss_share_y']

            sup_sales_table['predicted_value_share'] = sup_sales_table['predicted_value_share_x'] + sup_sales_table[
                'predicted_value_share_y']
            sup_sales_table['value_gain_share'] = sup_sales_table['value_gain_share_x'] + sup_sales_table[
                'value_gain_share_y']
            sup_sales_table['value_loss_share'] = sup_sales_table['value_loss_share_x'] + sup_sales_table[
                'value_loss_share_y']

            sup_sales_table = sup_sales_table[
                ['parent_supplier', 'predicted_volume_share', 'vols_gain_share', 'vols_loss_share',
                 'predicted_value_share', 'value_gain_share', 'value_loss_share']]

            # In[31]:

            sup_sales_table['vol_impact'] = sup_sales_table['vols_gain_share'] - sup_sales_table['vols_loss_share']
            sup_sales_table['value_impact'] = sup_sales_table['value_gain_share'] - sup_sales_table['value_loss_share']

            sup_sales_table['predicted_volume_share'] = sup_sales_table['predicted_volume_share'].replace(0, 1)
            sup_sales_table['predicted_value_share'] = sup_sales_table['predicted_value_share'].replace(0, 1)

            try:
                sup_sales_table['vol_impact_per'] = (sup_sales_table['vol_impact'] * 100) / sup_sales_table[
                    'predicted_volume_share']
            except:
                sup_sales_table['vol_impact_per'] = 0

            try:
                sup_sales_table['value_impact_per'] = (sup_sales_table['value_impact'] * 100) / sup_sales_table[
                    'predicted_value_share']
            except:
                sup_sales_table['value_impact_per'] = 0

            sup_sales_table = sup_sales_table[sup_sales_table['vol_impact'] != 0]

            #print('final sup_sales table at 4571, inside product_impact_chart')

        else:

            if input_tpns[0]== 0 :
                input_tpns = read_frame(nego_ads_drf.objects.all().filter(buying_controller__in=bc, store_type__in=store,
                                                      performance_quartile__in=['Low CPS/Low Profit'],
                                                      time_period__in=['Last 52 Weeks']).values('base_product_number').distinct())

                delist = list(input_tpns['base_product_number'])
            else:
                delist = input_tpns
                input_tpns = pd.DataFrame(input_tpns).reset_index(drop=True)
                input_tpns['base_product_number'] = input_tpns[0].copy()
                #input_tpns['base_product_number'] = input_tpns['base_product_number'].str[-8:]
                #input_tpns['base_product_number'] = input_tpns['base_product_number'].astype('int')
                delist = input_tpns['base_product_number'].drop_duplicates().values.tolist()

            #print('below values are passed')
            #print(args, bc, store, future, input_tpns)

            # In[4]:
            product_dataset = vol_logic.volume_transfer_logic(bc, store, future, input_tpns, delist)

            # In[44]:
            prod_hrchy = read_frame(
                product_desc.objects.all().filter(buying_controller__in=bc).values('base_product_number',
                                                                                   'brand_indicator',
                                                                                   'long_description').distinct())
            sup_table, sup_sales_table= vol_logic.supplier_table(product_dataset,store,
                                                                            future, bc)

        ## Original supplier table starts here -- keyword

        sup_sales_table = sup_sales_table

        sup_sales_table['vol_impact_per'] =  sup_sales_table['vol_impact_per'].round(decimals=1)
        sup_sales_table['value_impact_per'] =  sup_sales_table['value_impact_per'].round(decimals=1)
        #args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        #args.pop('format__iexact', None)

        #supplier_search = args.pop('supplier_search__iexact', '')

        #if supplier_search is not None:
        #   supplier_search_table = sup_sales_table
        #.str.contains(supplier_search, case=False)]

        ## set default delist page as 1
        #supplier_page = 1

        ## take page from args if entered
        # try:
        #   supplier_page = int(args.get('supplier_page__iexact'))
        # ## else 1
        # except:
        #   supplier_page = 1
        #
        # ## remove page number from args
        # args.pop('supplier_page__iexact', None)
        #
        # ## assign start and end points for subsetting data frame
        # start_row = (supplier_page - 1) * 8  ## example: for page 2 => 9
        # end_row = start_row + 8
        #
        # ## calculate total number of pages
        # num_pages = math.ceil((len(supplier_search_table) / 8))
        # ## calculate start index for data frame
        # start_index = (supplier_page - 1) * 8 + 1
        # #print(start_index)
        # # calculate total number of rows
        # count = len(supplier_search_table)
        # # calculate end index
        # end_index = supplier_page * 8
        ## subset the queryset to display required data
        #supplier_search_table = supplier_search_table.loc[start_row:end_row,]
        data = {
            'sup_sales_table': sup_sales_table.to_dict(orient='records')
        }
        ## passing data in required format
        return JsonResponse({#'pagination_count': num_pages,
                             #'supplier_page': supplier_page,
                             #'start_index': start_index,
                             #'count': count,
                             #'end_index': end_index,
                             'sup_sales_table': data['sup_sales_table']}, safe=False)

# supplier popup
class supplier_popup(vol_transfer_logic,APIView):

    def get(self, request, *args):

        all_filter = read_frame(product_impact_filter.objects.all())
        input_tpns = all_filter['input_tpns']
        input_tpns = list(input_tpns)
        #print('inside supplier pop up')
        #print(input_tpns)
        bc = all_filter['bc'][0]
        bc=[bc]
        store = all_filter['store'][0]
        store=[store]
        future = all_filter['future'][0]
        future=[future]
        vol_logic = vol_transfer_logic()
        # Logic for overview
        if store == ['Overview']:

            if input_tpns[0]==0:
                input_tpns_main = read_frame(
                    nego_ads_drf.objects.all().filter(buying_controller__in=bc, store_type__in=['Main Estate'],
                                                      performance_quartile__in=['Low CPS/Low Profit'],
                                                      time_period__in=['Last 52 Weeks']).values(
                        'base_product_number').distinct())

                delist_main = list(input_tpns_main['base_product_number'])
            else:
                # delist_main = input_tpns
                input_tpns_main = pd.DataFrame(input_tpns).reset_index(drop=True)
                input_tpns_main['base_product_number'] = input_tpns_main[0].copy()
                #input_tpns_main['base_product_number'] = input_tpns_main['base_product_number'].str[-8:]
                #input_tpns_main['base_product_number'] = input_tpns_main['base_product_number'].astype('int')
                delist_main = input_tpns_main['base_product_number'].drop_duplicates().values.tolist()

            product_dataset_main = vol_logic.volume_transfer_logic(bc, ['Main Estate'], future, input_tpns_main, delist_main)

            # In[7]:

            if input_tpns[0]==0:
                input_tpns_exp = read_frame(
                    nego_ads_drf.objects.all().filter(buying_controller__in=bc, store_type__in=['Express'],
                                                      performance_quartile__in=['Low CPS/Low Profit'],
                                                      time_period__in=['Last 52 Weeks']).values(
                        'base_product_number').distinct())

                delist_exp = list(input_tpns_exp['base_product_number'])
            else:
                # delist_exp = input_tpns
                input_tpns_exp = pd.DataFrame(input_tpns).reset_index(drop=True)
                input_tpns_exp['base_product_number'] = input_tpns_exp[0].copy()
                #input_tpns_exp['base_product_number'] = input_tpns_exp['base_product_number'].str[-8:]
                #input_tpns_exp['base_product_number'] = input_tpns_exp['base_product_number'].astype('int')
                delist_exp = input_tpns_exp['base_product_number'].drop_duplicates().values.tolist()

            #print(args, bc, store, future, input_tpns_exp)

            product_dataset_exp = vol_logic.volume_transfer_logic(bc, ['Express'], future, input_tpns_exp, delist_exp)
            product_dataset_exp.head()

            product_dataset = pd.merge(product_dataset_main, product_dataset_exp,
                                       left_on=['productcode', 'substituteproductcode'],
                                       right_on=['productcode', 'substituteproductcode'], how='outer')

            product_dataset = product_dataset.drop_duplicates().fillna(0).reset_index(drop=True)

            # In[11]:

            product_dataset['predicted_volume'] = product_dataset['predicted_volume_x'] + product_dataset[
                'predicted_volume_y']
            product_dataset['predicted_value'] = product_dataset['predicted_value_x'] + product_dataset[
                'predicted_value_y']
            product_dataset['predicted_cgm'] = product_dataset['predicted_cgm_x'] + product_dataset['predicted_cgm_y']
            product_dataset['predicted_cts'] = product_dataset['predicted_cts_x'] + product_dataset['predicted_cts_y']
            product_dataset['volume_transfer'] = product_dataset['volume_transfer_x'] + product_dataset[
                'volume_transfer_y']
            product_dataset['value_transfer'] = product_dataset['value_transfer_x'] + product_dataset[
                'value_transfer_y']
            product_dataset['cgm_transfer'] = product_dataset['cgm_transfer_x'] + product_dataset['cgm_transfer_y']
            product_dataset['cts_transfer'] = product_dataset['cts_transfer_x'] + product_dataset['cts_transfer_y']
            product_dataset['brand_indicator_x'] = product_dataset['brand_indicator_x'].fillna(
                product_dataset['brand_indicator_y'])
            product_dataset['brand_indicator_y'] = product_dataset['brand_indicator_x'].fillna(
                product_dataset['brand_indicator_y'])
            product_dataset['brand_indicator'] = product_dataset.brand_indicator_x.combine_first(
                product_dataset.brand_indicator_y)
            del product_dataset["brand_indicator_x"]
            del product_dataset["brand_indicator_y"]

            product_dataset = product_dataset[
                ['productcode', 'substituteproductcode', 'brand_indicator', 'predicted_volume', 'predicted_value',
                 'predicted_cgm', 'predicted_cts', 'volume_transfer', 'value_transfer', 'cgm_transfer', 'cts_transfer']]

            # supplier sales impact - table in UI
            # For overview logic
            sup_table_main, sup_sales_table_main = vol_logic.supplier_table(product_dataset_main, ['Main Estate'],
                                                                            future, bc)
            sup_table_exp, sup_sales_table_exp = vol_logic.supplier_table(product_dataset_exp, ['Express'], future, bc)

            # aggregation of tables start here
            sup_sales_table = pd.merge(sup_sales_table_main, sup_sales_table_exp, left_on=['parent_supplier'],
                                       right_on=['parent_supplier'], how='outer')
            sup_sales_table.head(2)

            # In[30]:
            sup_sales_table = sup_sales_table.drop_duplicates().reset_index(drop=True).fillna(0)
            sup_sales_table['predicted_volume_share'] = sup_sales_table['predicted_volume_share_x'] + sup_sales_table[
                'predicted_volume_share_y']
            sup_sales_table['vols_gain_share'] = sup_sales_table['vols_gain_share_x'] + sup_sales_table[
                'vols_gain_share_y']
            sup_sales_table['vols_loss_share'] = sup_sales_table['vols_loss_share_x'] + sup_sales_table[
                'vols_loss_share_y']

            sup_sales_table['predicted_value_share'] = sup_sales_table['predicted_value_share_x'] + sup_sales_table[
                'predicted_value_share_y']
            sup_sales_table['value_gain_share'] = sup_sales_table['value_gain_share_x'] + sup_sales_table[
                'value_gain_share_y']
            sup_sales_table['value_loss_share'] = sup_sales_table['value_loss_share_x'] + sup_sales_table[
                'value_loss_share_y']

            sup_sales_table = sup_sales_table[
                ['parent_supplier', 'predicted_volume_share', 'vols_gain_share', 'vols_loss_share',
                 'predicted_value_share', 'value_gain_share', 'value_loss_share']]

            # In[31]:

            sup_sales_table['vol_impact'] = sup_sales_table['vols_gain_share'] - sup_sales_table['vols_loss_share']
            sup_sales_table['value_impact'] = sup_sales_table['value_gain_share'] - sup_sales_table['value_loss_share']

            sup_sales_table['predicted_volume_share'] = sup_sales_table['predicted_volume_share'].replace(0, 1)
            sup_sales_table['predicted_value_share'] = sup_sales_table['predicted_value_share'].replace(0, 1)

            try:
                sup_sales_table['vol_impact_per'] = (sup_sales_table['vol_impact'] * 100) / sup_sales_table[
                    'predicted_volume_share']
            except:
                sup_sales_table['vol_impact_per'] = 0

            try:
                sup_sales_table['value_impact_per'] = (sup_sales_table['value_impact'] * 100) / sup_sales_table[
                    'predicted_value_share']
            except:
                sup_sales_table['value_impact_per'] = 0

            sup_sales_table = sup_sales_table[sup_sales_table['vol_impact'] != 0]

            #popup supplier table
            sup_table = pd.merge(sup_table_main, sup_table_exp, on=['parent_supplier', 'base_product_number'],
                                 how="outer")

            sup_table = sup_table.drop_duplicates().reset_index(drop=True).fillna(0)

            sup_table['predicted_volume_share'] = sup_table['predicted_volume_share_x'] + sup_table[
                'predicted_volume_share_y']
            sup_table['vols_gain_share'] = sup_table['vols_gain_share_x'] + sup_table['vols_gain_share_y']
            sup_table['vols_loss_share'] = sup_table['vols_loss_share_x'] + sup_table['vols_loss_share_y']

            sup_table['predicted_value_share'] = sup_table['predicted_value_share_x'] + sup_table[
                'predicted_value_share_y']
            sup_table['value_gain_share'] = sup_table['value_gain_share_x'] + sup_table['value_gain_share_y']
            sup_table['value_loss_share'] = sup_table['value_loss_share_x'] + sup_table['value_loss_share_y']

            sup_table = sup_table[
                ['parent_supplier', 'base_product_number', 'predicted_volume_share', 'vols_gain_share',
                 'vols_loss_share', 'predicted_value_share', 'value_gain_share', 'value_loss_share']]

            # In[32]:

            # ##------- supplier product level impact - pop up in UI------##

            #print('at 4583 - product-dataset')
            # #print(product_dataset)
            # #print(sup_table)

            data_pop = pd.merge(product_dataset_main[['productcode', 'substituteproductcode']], sup_table[
                ['parent_supplier', 'base_product_number', 'predicted_volume_share', 'vols_gain_share',
                 'vols_loss_share', 'predicted_value_share', 'value_gain_share', 'value_loss_share']],
                                left_on=['productcode'], right_on=['base_product_number'], how='left')

            data_pop = data_pop.rename(
                columns={'parent_supplier': 'delist_supplier', 'predicted_volume_share': 'delist_pred_vol',
                         'vols_gain_share': 'delist_vol_gain', 'vols_loss_share': 'delist_vol_loss',
                         'predicted_value_share': 'delist_pred_value', 'value_gain_share': 'delist_value_gain',
                         'value_loss_share': 'delist_value_loss'})

            data_pop = pd.merge(data_pop, sup_table[
                ['parent_supplier', 'base_product_number', 'predicted_volume_share', 'vols_gain_share',
                 'vols_loss_share', 'predicted_value_share', 'value_gain_share', 'value_loss_share']],
                                left_on=['substituteproductcode'], right_on=['base_product_number'], how='left')

            data_pop = data_pop.rename(
                columns={'parent_supplier': 'substitute_supplier', 'predicted_volume_share': 'substitute_pred_vol',
                         'vols_gain_share': 'substitute_vol_gain', 'vols_loss_share': 'substitute_vol_loss',
                         'predicted_value_share': 'substitute_pred_value', 'value_gain_share': 'substitute_value_gain',
                         'value_loss_share': 'substitute_value_loss'})
            data_pop = data_pop.drop_duplicates().fillna(0)

            del data_pop['base_product_number_x']
            del data_pop['base_product_number_y']

            data_pop = data_pop[['delist_supplier', 'productcode', 'delist_pred_vol',
                                 'delist_vol_gain', 'delist_vol_loss', 'delist_pred_value',
                                 'delist_value_gain', 'delist_value_loss', 'substitute_supplier',
                                 'substituteproductcode',
                                 'substitute_pred_vol', 'substitute_vol_gain', 'substitute_vol_loss',
                                 'substitute_pred_value', 'substitute_value_gain',
                                 'substitute_value_loss']]

            data_pop = data_pop[data_pop['substitute_supplier'] != 0]
            # sup_prod_pop_sub['delist_supplier'] = sup_prod_pop_sub['substitute_supplier']

            sup_product_pop = pd.DataFrame()
            sup_product_pop = sup_product_pop.append(data_pop)

            sup_prod_pop_sub = data_pop
            sup_prod_pop_sub['delist_pred_vol'] = 0
            sup_prod_pop_sub['delist_vol_gain'] = 0
            sup_prod_pop_sub['delist_vol_loss'] = 0
            sup_prod_pop_sub['delist_pred_value'] = 0
            sup_prod_pop_sub['delist_value_gain'] = 0
            sup_prod_pop_sub['delist_value_loss'] = 0
            sup_prod_pop_sub['delist_supplier'] = sup_prod_pop_sub['substitute_supplier']
            sup_product_pop = sup_product_pop.append(sup_prod_pop_sub)

            prod_hrchy = read_frame(
                product_desc.objects.all().filter(buying_controller__in=bc).values('base_product_number',
                                                                                   'brand_indicator',
                                                                                   'long_description').distinct())
            sup_product_pop = pd.merge(sup_product_pop, prod_hrchy, left_on=['productcode'],
                                       right_on=['base_product_number'], how='left')
            del sup_product_pop['base_product_number']
            sup_product_pop = sup_product_pop.rename(columns={'long_description': 'productdescription'})
            sup_product_pop = pd.merge(sup_product_pop, prod_hrchy, left_on=['substituteproductcode'],
                                       right_on=['base_product_number'], how='left')
            del sup_product_pop['base_product_number']
            sup_product_pop = sup_product_pop.rename(columns={'long_description': 'substituteproductdescription'})
            sup_product_pop = sup_product_pop.drop_duplicates().fillna(0).reset_index(drop=True)

            sup_product_pop['substitute_value_loss'] = sup_product_pop['substitute_value_loss'].astype('int')
            sup_product_pop['substitute_vol_loss'] = sup_product_pop['substitute_vol_loss'].astype('int')

            sup_product_pop['substitute_value_gain'] = sup_product_pop['substitute_value_gain'].astype('int')
            sup_product_pop['substitute_vol_gain'] = sup_product_pop['substitute_vol_gain'].astype('int')

            sup_product_pop['substitute_pred_value'] = sup_product_pop['substitute_pred_value'].astype('int')
            sup_product_pop['substitute_pred_vol'] = sup_product_pop['substitute_pred_vol'].astype('int')

            sup_product_pop['delist_value_gain'] = sup_product_pop['delist_value_gain'].astype('int')
            sup_product_pop['delist_vol_gain'] = sup_product_pop['delist_vol_gain'].astype('int')

            sup_product_pop['delist_value_loss'] = sup_product_pop['delist_value_loss'].astype('int')
            sup_product_pop['delist_vol_loss'] = sup_product_pop['delist_vol_loss'].astype('int')

            sup_product_pop['delist_pred_value'] = sup_product_pop['delist_pred_value'].astype('int')
            sup_product_pop['delist_pred_vol'] = sup_product_pop['delist_pred_vol'].astype('int')

            sup_product_pop = sup_product_pop[sup_product_pop['substitute_pred_vol'] != 0]
            supplier_table_popup = pd.DataFrame(sup_product_pop)
            #delist_table_popup = pd.DataFrame(delist_prod_subs)

            #print('supplier pop up before pop up condition')
            #print(supplier_table_popup.shape)


        else:

            if input_tpns[0] ==0:
                input_tpns = read_frame(
                    nego_ads_drf.objects.all().filter(buying_controller__in=bc, store_type__in=store,
                                                      performance_quartile__in=['Low CPS/Low Profit'],
                                                      time_period__in=['Last 52 Weeks']).values(
                        'base_product_number').distinct())

                delist = list(input_tpns['base_product_number'])
            else:
                delist = input_tpns
                input_tpns = pd.DataFrame(input_tpns).reset_index(drop=True)
                input_tpns['base_product_number'] = input_tpns[0].copy()
                #input_tpns['base_product_number'] = input_tpns['base_product_number'].str[-8:]
                #input_tpns['base_product_number'] = input_tpns['base_product_number'].astype('int')
                delist = input_tpns['base_product_number'].drop_duplicates().values.tolist()



            product_dataset = vol_logic.volume_transfer_logic(bc, store, future, input_tpns, delist)

            prod_hrchy = read_frame(
                product_desc.objects.all().filter(buying_controller__in=bc).values('base_product_number',
                                                                                   'brand_indicator',
                                                                                   'long_description').distinct())

            sup_table, sup_sales_table = vol_logic.supplier_table(product_dataset, store,
                                                      future, bc)
            ##------- supplier product level impact - pop up in UI------##
            #print("#printing sup table data")
            #print(sup_table.shape)
            data_pop = pd.merge(product_dataset[['productcode', 'substituteproductcode']], sup_table[
                ['parent_supplier', 'base_product_number', 'predicted_volume_share', 'vols_gain_share',
                 'vols_loss_share', 'predicted_value_share', 'value_gain_share', 'value_loss_share']],
                                left_on=['productcode'], right_on=['base_product_number'], how='left')

            data_pop = data_pop.rename(
                columns={'parent_supplier': 'delist_supplier', 'predicted_volume_share': 'delist_pred_vol',
                         'vols_gain_share': 'delist_vol_gain', 'vols_loss_share': 'delist_vol_loss',
                         'predicted_value_share': 'delist_pred_value', 'value_gain_share': 'delist_value_gain',
                         'value_loss_share': 'delist_value_loss'})

            data_pop = pd.merge(data_pop, sup_table[
                ['parent_supplier', 'base_product_number', 'predicted_volume_share', 'vols_gain_share',
                 'vols_loss_share', 'predicted_value_share', 'value_gain_share', 'value_loss_share']],
                                left_on=['substituteproductcode'], right_on=['base_product_number'], how='left')

            data_pop = data_pop.rename(
                columns={'parent_supplier': 'substitute_supplier', 'predicted_volume_share': 'substitute_pred_vol',
                         'vols_gain_share': 'substitute_vol_gain', 'vols_loss_share': 'substitute_vol_loss',
                         'predicted_value_share': 'substitute_pred_value', 'value_gain_share': 'substitute_value_gain',
                         'value_loss_share': 'substitute_value_loss'})
            data_pop = data_pop.drop_duplicates().fillna(0)

            del data_pop['base_product_number_x']
            del data_pop['base_product_number_y']

            data_pop = data_pop[['delist_supplier', 'productcode', 'delist_pred_vol',
                                 'delist_vol_gain', 'delist_vol_loss', 'delist_pred_value',
                                 'delist_value_gain', 'delist_value_loss', 'substitute_supplier',
                                 'substituteproductcode',
                                 'substitute_pred_vol', 'substitute_vol_gain', 'substitute_vol_loss',
                                 'substitute_pred_value', 'substitute_value_gain',
                                 'substitute_value_loss']]

            data_pop = data_pop[data_pop['substitute_supplier'] != 0]

            sup_product_pop = pd.DataFrame()
            sup_product_pop = sup_product_pop.append(data_pop)

            sup_prod_pop_sub = data_pop
            sup_prod_pop_sub['delist_pred_vol'] = 0
            sup_prod_pop_sub['delist_vol_gain'] = 0
            sup_prod_pop_sub['delist_vol_loss'] = 0
            sup_prod_pop_sub['delist_pred_value'] = 0
            sup_prod_pop_sub['delist_value_gain'] = 0
            sup_prod_pop_sub['delist_value_loss'] = 0
            sup_prod_pop_sub['delist_supplier'] = sup_prod_pop_sub['substitute_supplier']
            sup_product_pop = sup_product_pop.append(sup_prod_pop_sub)

            sup_product_pop = pd.merge(sup_product_pop, prod_hrchy, left_on=['productcode'],
                                       right_on=['base_product_number'], how='left')
            del sup_product_pop['base_product_number']
            sup_product_pop = sup_product_pop.rename(columns={'long_description': 'productdescription'})
            sup_product_pop = pd.merge(sup_product_pop, prod_hrchy, left_on=['substituteproductcode'],
                                       right_on=['base_product_number'], how='left')
            del sup_product_pop['base_product_number']
            sup_product_pop = sup_product_pop.rename(columns={'long_description': 'substituteproductdescription'})
            sup_product_pop = sup_product_pop.drop_duplicates().fillna(0).reset_index(drop=True)

            sup_product_pop['substitute_value_loss'] = sup_product_pop['substitute_value_loss'].astype('int')
            sup_product_pop['substitute_vol_loss'] = sup_product_pop['substitute_vol_loss'].astype('int')

            sup_product_pop['substitute_value_gain'] = sup_product_pop['substitute_value_gain'].astype('int')
            sup_product_pop['substitute_vol_gain'] = sup_product_pop['substitute_vol_gain'].astype('int')

            sup_product_pop['substitute_pred_value'] = sup_product_pop['substitute_pred_value'].astype('int')
            sup_product_pop['substitute_pred_vol'] = sup_product_pop['substitute_pred_vol'].astype('int')

            sup_product_pop['delist_value_gain'] = sup_product_pop['delist_value_gain'].astype('int')
            sup_product_pop['delist_vol_gain'] = sup_product_pop['delist_vol_gain'].astype('int')

            sup_product_pop['delist_value_loss'] = sup_product_pop['delist_value_loss'].astype('int')
            sup_product_pop['delist_vol_loss'] = sup_product_pop['delist_vol_loss'].astype('int')

            sup_product_pop['delist_pred_value'] = sup_product_pop['delist_pred_value'].astype('int')
            sup_product_pop['delist_pred_vol'] = sup_product_pop['delist_pred_vol'].astype('int')

            sup_product_pop = sup_product_pop[sup_product_pop['substitute_pred_vol'] != 0]
            supplier_table_popup = pd.DataFrame(sup_product_pop)
            #print(supplier_table_popup.shape)
            #delist_table_popup = pd.DataFrame(delist_prod_subs)


        ## original supplier pop up starts here

        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        args.pop('format__iexact', None)
        supplier = args.get('supplier__iexact')
        supplier = [supplier]
        sup_pop = pd.DataFrame(supplier)
        sup_pop['supplier'] = sup_pop[0]
        popup_data1 = pd.merge(supplier_table_popup, sup_pop, left_on=['delist_supplier'], right_on=['supplier'], how='inner')
        #print('\n supplier from arguments after converting to list and popup_data1 _____', type(supplier_table_popup['delist_supplier']),type(sup_pop['supplier']))
        popup_data1 = popup_data1.drop_duplicates().fillna(0).reset_index(drop=True)

        # ## set default page as 1
        #
        # supplier_popup_page = 1
        #
        # ## take page from args if entered
        # try:
        #   supplier_popup_page = int(args.get('supplier_popup_page__iexact'))
        # ## else 1
        # except:
        #   supplier_popup_page = 1
        # ## remove page number from args
        # args.pop('supplier_popup_page__iexact', None)
        #
        # ## assign start and end points for subsetting data frame
        # start_row = (supplier_popup_page - 1) * 8  ## example: for page 2 => 9
        # end_row = start_row + 7
        #
        # ## calculate total number of pages
        # num_pages = math.ceil((len(popup_data1) / 8))
        # ## calculate start index for data frame
        # start_index = (supplier_popup_page - 1) * 8 + 1
        # #print(start_index)
        # # calculate total number of rows
        # count = len(popup_data1)
        # # calculate end index
        # end_index = supplier_popup_page * 8
        # ## subset the queryset to display required data
        # popup_data1 = popup_data1.loc[start_row:end_row, ]
        #
        # #print('\n final output of supplier pop up after everything',popup_data1)

        data = {
            'supplier_table_popup': popup_data1.to_dict(orient='records')
        }
        ## passing data in required format
        return JsonResponse({#'pagination_count': num_pages,
                             #'supplier_popup_page': supplier_popup_page,
                             #'start_index': start_index,
                             #'count': count,
                             #'end_index': end_index,
                             'table': data['supplier_table_popup']}, safe=False)

# delist table
class product_impact_delist_table(vol_transfer_logic,APIView):
    def get(self, request, *args):
        all_filter = read_frame(product_impact_filter.objects.all())
        input_tpns = all_filter['input_tpns']
        input_tpns = list(input_tpns)
        bc = all_filter['bc'][0]
        bc= [bc]
        store = all_filter['store'][0]
        store=[store]
        future = all_filter['future'][0]
        future =[future]
        vol_logic = vol_transfer_logic()
        # Logic for overview10358
        if store == ['Overview']:
            if input_tpns[0]==0:
                input_tpns_main = read_frame(
                nego_ads_drf.objects.all().filter(buying_controller__in=bc, store_type__in=['Main Estate'],
                                                  performance_quartile__in=['Low CPS/Low Profit'],
                                                  time_period__in=['Last 52 Weeks']).values(
                    'base_product_number').distinct())

                delist_main = list(input_tpns_main['base_product_number'])
            else:
                # delist_main = input_tpns
                input_tpns_main = pd.DataFrame(input_tpns).reset_index(drop=True)
                input_tpns_main['base_product_number'] = input_tpns_main[0].copy()
                #input_tpns_main['base_product_number'] = input_tpns_main['base_product_number'].str[-8:]
                #input_tpns_main['base_product_number'] = input_tpns_main['base_product_number'].astype('int')
                delist_main = input_tpns_main['base_product_number'].drop_duplicates().values.tolist()

            # In[6]:

            product_dataset_main = vol_logic.volume_transfer_logic(bc, ['Main Estate'], future, input_tpns_main, delist_main)

            if input_tpns[0]==0:
                input_tpns_exp = read_frame(
                    nego_ads_drf.objects.all().filter(buying_controller__in=bc, store_type__in=['Express'],
                                                      performance_quartile__in=['Low CPS/Low Profit'],
                                                      time_period__in=['Last 52 Weeks']).values(
                        'base_product_number').distinct())

                delist_exp = list(input_tpns_exp['base_product_number'])
            else:
                # delist_exp = input_tpns
                input_tpns_exp = pd.DataFrame(input_tpns).reset_index(drop=True)
                input_tpns_exp['base_product_number'] = input_tpns_exp[0].copy()
                #input_tpns_exp['base_product_number'] = input_tpns_exp['base_product_number'].str[-8:]
                #input_tpns_exp['base_product_number'] = input_tpns_exp['base_product_number'].astype('int')
                delist_exp = input_tpns_exp['base_product_number'].drop_duplicates().values.tolist()

            #print('below values are passed - express')
            #print(args, bc, store, future, input_tpns_exp)

            # In[8]:

            product_dataset_exp = vol_logic.volume_transfer_logic(bc, ['Express'], future, input_tpns_exp, delist_exp)

            product_dataset = pd.merge(product_dataset_main, product_dataset_exp,
                                       left_on=['productcode', 'substituteproductcode'],
                                       right_on=['productcode', 'substituteproductcode'], how='outer')

            # In[10]:

            product_dataset = product_dataset.drop_duplicates().fillna(0).reset_index(drop=True)

            # In[11]:

            product_dataset['predicted_volume'] = product_dataset['predicted_volume_x'] + product_dataset[
                'predicted_volume_y']
            product_dataset['predicted_value'] = product_dataset['predicted_value_x'] + product_dataset[
                'predicted_value_y']
            product_dataset['predicted_cgm'] = product_dataset['predicted_cgm_x'] + product_dataset['predicted_cgm_y']
            product_dataset['predicted_cts'] = product_dataset['predicted_cts_x'] + product_dataset['predicted_cts_y']
            product_dataset['volume_transfer'] = product_dataset['volume_transfer_x'] + product_dataset[
                'volume_transfer_y']
            product_dataset['value_transfer'] = product_dataset['value_transfer_x'] + product_dataset[
                'value_transfer_y']
            product_dataset['cgm_transfer'] = product_dataset['cgm_transfer_x'] + product_dataset['cgm_transfer_y']
            product_dataset['cts_transfer'] = product_dataset['cts_transfer_x'] + product_dataset['cts_transfer_y']
            product_dataset['brand_indicator_x'] = product_dataset['brand_indicator_x'].fillna(
                product_dataset['brand_indicator_y'])
            product_dataset['brand_indicator_y'] = product_dataset['brand_indicator_x'].fillna(
                product_dataset['brand_indicator_y'])
            product_dataset['brand_indicator'] = product_dataset.brand_indicator_x.combine_first(
                product_dataset.brand_indicator_y)
            del product_dataset["brand_indicator_x"]
            del product_dataset["brand_indicator_y"]

            product_dataset = product_dataset[
                ['productcode', 'substituteproductcode', 'brand_indicator', 'predicted_volume', 'predicted_value',
                 'predicted_cgm', 'predicted_cts', 'volume_transfer', 'value_transfer', 'cgm_transfer', 'cts_transfer']]

            # delist product table for UI
            delist_prod_table = product_dataset[['productcode','substituteproductcode', 'predicted_volume', 'predicted_value', 'predicted_cgm','volume_transfer','value_transfer']]
            delist_prod_table = delist_prod_table.drop_duplicates().fillna(0).reset_index(drop=True).groupby(['productcode'], as_index = False).agg({'predicted_volume':max, 'predicted_value': max, 'predicted_cgm':max,'volume_transfer':sum,'value_transfer':sum})
            prod_hrchy = read_frame(product_desc.objects.all().values('base_product_number', 'brand_indicator',
                                                                      'long_description').distinct())

            delist_prod_table = pd.merge(delist_prod_table, prod_hrchy, left_on=['productcode'],
                                         right_on=['base_product_number'], how='left')
            del delist_prod_table['base_product_number']
            delist_prod_table = delist_prod_table.drop_duplicates().fillna(0).reset_index(drop=True)

            #delist_prod_table = delist_prod_table.groupby(['productcode', 'long_description'], as_index=False).agg(
            #   {'predicted_volume': sum, 'predicted_value': sum, 'predicted_cgm': sum})

            # In[22]:
            contribution_main = read_frame(
                product_contri.objects.all().filter(buying_controller__in=bc, store_type__in=['Main Estate'],
                                                    time_period__in=future).values_list())

            contribution_main = contribution_main.rename(columns={'base_product_number': 'productcode'})

            contribution_exp = read_frame(
                product_contri.objects.all().filter(buying_controller__in=bc, store_type__in=['Express'],
                                                    time_period__in=future).values_list())

            contribution_exp = contribution_exp.rename(columns={'base_product_number': 'productcode'})

            overview_store = pd.merge(contribution_main[['productcode', 'no_of_stores']],
                                      contribution_exp[['productcode', 'no_of_stores']], left_on=['productcode'],
                                      right_on=['productcode'], how='outer')
            overview_store = overview_store.drop_duplicates().fillna(0).reset_index(drop=True)
            overview_store['no_of_stores'] = overview_store['no_of_stores_x'] + overview_store['no_of_stores_y']

            # In[23]:

            delist_prod_table = pd.merge(delist_prod_table, overview_store[['productcode', 'no_of_stores']],
                                         left_on=['productcode'], right_on=['productcode'], how='left')
            delist_prod_table = delist_prod_table[delist_prod_table['no_of_stores'] > 0]
            delist_prod_table = delist_prod_table[delist_prod_table['predicted_volume'] > 0]
            delist_prod_table['per_vol_transfer'] = ((delist_prod_table['volume_transfer'] / delist_prod_table[
                'predicted_volume']) * 100).round(decimals=1)
            delist_prod_table['per_value_transfer'] = ((delist_prod_table['value_transfer'] / delist_prod_table[
                'predicted_value']) * 100).round(decimals=1)


            delist_prod_table = delist_prod_table.drop_duplicates().fillna(0).reset_index(drop=True)

            #adding psg impact column
            psg_predict_main = vol_logic.psg_impact(bc, delist_main, ['Main Estate'], future)
            psg_predict_exp = vol_logic.psg_impact(bc, delist_exp, ['Express'], future)

            psg_predict = pd.merge(psg_predict_main, psg_predict_exp, on="product_sub_group_description", how='outer')
            psg_predict = psg_predict.drop_duplicates().reset_index(drop=True).fillna(0)

            psg_predict['psg_predicted_sales'] = psg_predict['psg_predicted_sales_x'] + psg_predict[
                'psg_predicted_sales_y']

            del psg_predict['psg_predicted_sales_x']
            del psg_predict['psg_predicted_sales_y']

            psg = read_frame(product_hierarchy.objects.all().filter(buying_controller__in=bc,
                                                                    base_product_number__in=delist_exp).values(
                'product_sub_group_code', 'base_product_number', 'product_sub_group_description').distinct())
            delist_prod_table = pd.merge(delist_prod_table,
                                         psg[['base_product_number', 'product_sub_group_description']],
                                         left_on="productcode", right_on="base_product_number", how="left")
            del delist_prod_table['base_product_number']

            delist_prod_table = pd.merge(delist_prod_table, psg_predict, on="product_sub_group_description", how="left")
            delist_prod_table['psg_value_impact'] = (
            delist_prod_table['predicted_value'] / delist_prod_table['psg_predicted_sales']).round(decimals=1)
            delist_prod_table = delist_prod_table[['productcode', 'long_description', 'predicted_value',
                                                   'predicted_volume', 'predicted_cgm', 'no_of_stores',
                                                   'per_vol_transfer','per_value_transfer',
                                                   'product_sub_group_description', 'psg_value_impact']]


        else:

            if input_tpns[0]==0:
                input_tpns = read_frame(
                    nego_ads_drf.objects.all().filter(buying_controller__in=bc, store_type__in=store,
                                                      performance_quartile__in=['Low CPS/Low Profit'],
                                                      time_period__in=['Last 52 Weeks']).values(
                        'base_product_number').distinct())

                delist = list(input_tpns['base_product_number'])
            else:
                delist = input_tpns
                input_tpns = pd.DataFrame(input_tpns).reset_index(drop=True)
                input_tpns['base_product_number'] = input_tpns[0].copy()
                #input_tpns['base_product_number'] = input_tpns['base_product_number'].str[-8:]
                #input_tpns['base_product_number'] = input_tpns['base_product_number'].astype('int')
                delist = input_tpns['base_product_number'].drop_duplicates().values.tolist()

            # In[4]:
            product_dataset = vol_logic.volume_transfer_logic(bc, store, future, input_tpns, delist)

            # In[44]:
            prod_hrchy = read_frame(
                product_desc.objects.all().filter(buying_controller__in=bc).values('base_product_number',
                                                                                   'brand_indicator',
                                                                                   'long_description').distinct())

            # delist product table for UI
            delist_prod_table = product_dataset[['productcode','substituteproductcode', 'predicted_volume', 'predicted_value', 'predicted_cgm','volume_transfer','value_transfer']]
            delist_prod_table = delist_prod_table.drop_duplicates().fillna(0).reset_index(drop=True).groupby(['productcode'], as_index = False).agg({'predicted_volume':max, 'predicted_value': max, 'predicted_cgm':max,'volume_transfer':sum,'value_transfer':sum})
            delist_prod_table = pd.merge(delist_prod_table, prod_hrchy, left_on=['productcode'],
                                         right_on=['base_product_number'], how='left')
            del delist_prod_table['base_product_number']

            #delist_prod_table = delist_prod_table.groupby(['productcode', 'long_description'], as_index=False).agg(
            #   {'predicted_volume': sum, 'predicted_value': sum, 'predicted_cgm': sum})
            delist_prod_table = delist_prod_table.drop_duplicates().fillna(0)
            contribution = read_frame(
                product_contri.objects.all().filter(buying_controller__in=bc, store_type__in=store,
                                                    time_period__in=future).values_list())

            contribution = contribution.rename(columns={'base_product_number': 'productcode'})
            delist_prod_table = pd.merge(delist_prod_table, contribution[['productcode', 'no_of_stores']],
                                         left_on=['productcode'], right_on=['productcode'], how='left')
            delist_prod_table = delist_prod_table[delist_prod_table['no_of_stores'] > 0]
            delist_prod_table = delist_prod_table[delist_prod_table['predicted_volume'] > 0]

            delist_prod_table['per_vol_transfer'] = ((delist_prod_table['volume_transfer'] / delist_prod_table[
                'predicted_volume']) * 100).round(decimals=1)
            delist_prod_table['per_value_transfer'] = ((delist_prod_table['value_transfer'] / delist_prod_table[
                'predicted_value']) * 100).round(decimals=1)

            delist_prod_table = delist_prod_table.drop_duplicates().fillna(0).reset_index(drop=True)

            #add psg impact column
            psg = read_frame(product_hierarchy.objects.all().filter(buying_controller__in=bc,
                                                                    base_product_number__in=delist).values(
                'product_sub_group_code', 'base_product_number', 'product_sub_group_description').distinct())
            delist_prod_table = pd.merge(delist_prod_table,
                                         psg[['base_product_number', 'product_sub_group_description']],
                                         left_on="productcode", right_on="base_product_number", how="left")
            del delist_prod_table['base_product_number']

            psg_predict = vol_logic.psg_impact(bc, delist, store, future)
            delist_prod_table = pd.merge(delist_prod_table, psg_predict, on="product_sub_group_description", how="left")
            delist_prod_table['psg_value_impact'] = (
            delist_prod_table['predicted_value'] / delist_prod_table['psg_predicted_sales']).round(decimals=1)

            delist_prod_table = delist_prod_table[['productcode', 'long_description', 'predicted_value',
                                                   'predicted_volume', 'predicted_cgm', 'no_of_stores',
                                                    'product_sub_group_description','per_vol_transfer','per_value_transfer',
                                                   'psg_value_impact']]



        # args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        # args.pop('format__iexact', None)
        #
        # delist_search = args.pop('delist_search__iexact', '')
        #
        # if delist_search is not None:
        #   delist_search_table = delist_prod_table[delist_prod_table['long_description'].str.contains(delist_search, case=False)]
        #
        # ## set default delist page as 1
        # delist_page = 1
        #
        # ## take page from args if entered
        # try:
        #   delist_page = int(args.get('delist_page__iexact'))
        # ## else 1
        # except:
        #   delist_page = 1
        #
        # ## remove page number from args
        # args.pop('delist_page__iexact', None)
        #
        # ## assign start and end points for subsetting data frame
        # start_row = (delist_page - 1) * 8  ## example: for page 2 => 9
        # end_row = start_row + 8
        #
        # ## calculate total number of pages
        # num_pages = math.ceil((len(delist_search_table) / 8))
        # ## calculate start index for data frame
        # start_index = (delist_page - 1) * 8 + 1
        # #print(start_index)
        # # calculate total number of rows
        # count = len(delist_search_table)
        # # calculate end index
        # end_index = delist_page * 8
        # ## subset the queryset to display required data
        # delist_search_table = delist_search_table.loc[start_row:end_row, ]
        #
        # #print('inside delist_table-- at 5312')
        # #print(delist_search_table)

        data = {
            'delist_prod_table': delist_prod_table.to_dict(orient='records')
        }
        ## passing data in required format

        return JsonResponse({#'pagination_count': num_pages,
                             #'delist_page': delist_page,
                             #'start_index': start_index,
                             #'count': count,
                             #'end_index': end_index,
                             'delist_prod_table': data['delist_prod_table']}, safe=False)

# delist popup
class delist_popup(vol_transfer_logic,APIView):

    def get(self, request, *args):

        all_filter = read_frame(product_impact_filter.objects.all())
        input_tpns = all_filter['input_tpns']
        input_tpns =list(input_tpns)
        bc = all_filter['bc'][0]
        bc= [bc]
        store = all_filter['store'][0]
        store =[store]
        future = all_filter['future'][0]
        future=[future]
        vol_logic =vol_transfer_logic()
        # Logic for overview
        if store == ['Overview']:

            if input_tpns[0]==0:
                input_tpns_main = read_frame(
                    nego_ads_drf.objects.all().filter(buying_controller__in=bc, store_type__in=['Main Estate'],
                                                      performance_quartile__in=['Low CPS/Low Profit'],
                                                      time_period__in=['Last 52 Weeks']).values(
                        'base_product_number').distinct())

                delist_main = list(input_tpns_main['base_product_number'])
            else:
                # delist_main = input_tpns
                input_tpns_main = pd.DataFrame(input_tpns).reset_index(drop=True)
                input_tpns_main['base_product_number'] = input_tpns_main[0].copy()
                #input_tpns_main['base_product_number'] = input_tpns_main['base_product_number'].str[-8:]
                #input_tpns_main['base_product_number'] = input_tpns_main['base_product_number'].astype('int')
                delist_main = input_tpns_main['base_product_number'].drop_duplicates().values.tolist()

            # #print('below values are passed - main estate')
            # #print(args, bc, store, future, input_tpns_main)

            # In[6]:

            product_dataset_main = vol_logic.volume_transfer_logic(bc, ['Main Estate'], future, input_tpns_main, delist_main)
            if input_tpns[0]==0:
                input_tpns_exp = read_frame(
                    nego_ads_drf.objects.all().filter(buying_controller__in=bc, store_type__in=['Express'],
                                                      performance_quartile__in=['Low CPS/Low Profit'],
                                                      time_period__in=['Last 52 Weeks']).values(
                        'base_product_number').distinct())

                delist_exp = list(input_tpns_exp['base_product_number'])
            else:
                # delist_exp = input_tpns
                input_tpns_exp = pd.DataFrame(input_tpns).reset_index(drop=True)
                input_tpns_exp['base_product_number'] = input_tpns_exp[0].copy()
                #input_tpns_exp['base_product_number'] = input_tpns_exp['base_product_number'].str[-8:]
                #input_tpns_exp['base_product_number'] = input_tpns_exp['base_product_number'].astype('int')
                delist_exp = input_tpns_exp['base_product_number'].drop_duplicates().values.tolist()

            product_dataset_exp = vol_logic.volume_transfer_logic(bc, ['Express'], future, input_tpns_exp, delist_exp)
            product_dataset = pd.merge(product_dataset_main, product_dataset_exp,
                                       left_on=['productcode', 'substituteproductcode'],
                                       right_on=['productcode', 'substituteproductcode'], how='outer')

            # In[10]:

            product_dataset = product_dataset.drop_duplicates().fillna(0).reset_index(drop=True)

            # In[11]:

            product_dataset['predicted_volume'] = product_dataset['predicted_volume_x'] + product_dataset[
                'predicted_volume_y']
            product_dataset['predicted_value'] = product_dataset['predicted_value_x'] + product_dataset[
                'predicted_value_y']
            product_dataset['predicted_cgm'] = product_dataset['predicted_cgm_x'] + product_dataset['predicted_cgm_y']
            product_dataset['predicted_cts'] = product_dataset['predicted_cts_x'] + product_dataset['predicted_cts_y']
            product_dataset['volume_transfer'] = product_dataset['volume_transfer_x'] + product_dataset[
                'volume_transfer_y']
            product_dataset['value_transfer'] = product_dataset['value_transfer_x'] + product_dataset[
                'value_transfer_y']
            product_dataset['cgm_transfer'] = product_dataset['cgm_transfer_x'] + product_dataset['cgm_transfer_y']
            product_dataset['cts_transfer'] = product_dataset['cts_transfer_x'] + product_dataset['cts_transfer_y']
            product_dataset['brand_indicator_x'] = product_dataset['brand_indicator_x'].fillna(
                product_dataset['brand_indicator_y'])
            product_dataset['brand_indicator_y'] = product_dataset['brand_indicator_x'].fillna(
                product_dataset['brand_indicator_y'])
            product_dataset['brand_indicator'] = product_dataset.brand_indicator_x.combine_first(
                product_dataset.brand_indicator_y)
            del product_dataset["brand_indicator_x"]
            del product_dataset["brand_indicator_y"]

            product_dataset = product_dataset[
                ['productcode', 'substituteproductcode', 'brand_indicator', 'predicted_volume', 'predicted_value',
                 'predicted_cgm', 'predicted_cts', 'volume_transfer', 'value_transfer', 'cgm_transfer', 'cts_transfer']]


            # delist product table for UI
            delist_prod_table = product_dataset[['productcode', 'predicted_volume', 'predicted_value', 'predicted_cgm']]
            delist_prod_table = delist_prod_table.drop_duplicates().fillna(0).reset_index(drop=True).groupby(['productcode'], as_index = False).agg({'predicted_volume':max, 'predicted_value': max, 'predicted_cgm':max})
            prod_hrchy = read_frame(product_desc.objects.all().values('base_product_number', 'brand_indicator',
                                                                      'long_description').distinct())

            delist_prod_table = pd.merge(delist_prod_table, prod_hrchy, left_on=['productcode'],
                                         right_on=['base_product_number'], how='left')
            del delist_prod_table['base_product_number']
            delist_prod_table = delist_prod_table.drop_duplicates().fillna(0).reset_index(drop=True)

            delist_prod_table = delist_prod_table.groupby(['productcode', 'long_description'], as_index=False).agg(
                {'predicted_volume': sum, 'predicted_value': sum, 'predicted_cgm': sum})

            # In[22]:
            contribution_main = read_frame(
                product_contri.objects.all().filter(buying_controller__in=bc, store_type__in=['Main Estate'],
                                                    time_period__in=future).values_list())

            contribution_main = contribution_main.rename(columns={'base_product_number': 'productcode'})

            contribution_exp = read_frame(
                product_contri.objects.all().filter(buying_controller__in=bc, store_type__in=['Express'],
                                                    time_period__in=future).values_list())

            contribution_exp = contribution_exp.rename(columns={'base_product_number': 'productcode'})
            overview_store = pd.merge(contribution_main[['productcode', 'no_of_stores']],
                                      contribution_exp[['productcode', 'no_of_stores']], left_on=['productcode'],
                                      right_on=['productcode'], how='outer')
            overview_store = overview_store.drop_duplicates().fillna(0).reset_index(drop=True)
            overview_store['no_of_stores'] = overview_store['no_of_stores_x'] + overview_store['no_of_stores_y']

            # In[23]:

            delist_prod_table = pd.merge(delist_prod_table, overview_store[['productcode', 'no_of_stores']],
                                         left_on=['productcode'], right_on=['productcode'], how='left')
            delist_prod_table = delist_prod_table[delist_prod_table['no_of_stores'] > 0]
            delist_prod_table = delist_prod_table[delist_prod_table['predicted_volume'] > 0]
            delist_prod_table = delist_prod_table.drop_duplicates().fillna(0).reset_index(drop=True)

            delist_prod_subs_main = vol_logic.delist_subs(bc, ['Main Estate'], future, delist_main)
            delist_prod_subs_exp = vol_logic.delist_subs(bc, ['Express'], future, delist_exp)

            delist_prod_subs = pd.DataFrame()
            delist_prod_subs = delist_prod_subs.append(delist_prod_subs_main)
            delist_prod_subs = delist_prod_subs.append(delist_prod_subs_exp)
            delist_prod_subs = delist_prod_subs.drop_duplicates().fillna(0).reset_index(drop=True)
            #print("overview delist popup"
                  # )
            #print(delist_prod_subs.shape)
            delist_table_popup = pd.DataFrame(delist_prod_subs)

        else:

            if input_tpns[0]==0:
                input_tpns = read_frame(
                    nego_ads_drf.objects.all().filter(buying_controller__in=bc, store_type__in=store,
                                                      performance_quartile__in=['Low CPS/Low Profit'],
                                                      time_period__in=['Last 52 Weeks']).values(
                        'base_product_number').distinct())

                delist = list(input_tpns['base_product_number'])
            else:
                delist = input_tpns
                input_tpns = pd.DataFrame(input_tpns).reset_index(drop=True)
                input_tpns['base_product_number'] = input_tpns[0].copy()
                #input_tpns['base_product_number'] = input_tpns['base_product_number'].str[-8:]
                #input_tpns['base_product_number'] = input_tpns['base_product_number'].astype('int')
                delist = input_tpns['base_product_number'].drop_duplicates().values.tolist()

            #print('below values are passed')
            #print(args, bc, store, future, input_tpns)

            # In[4]:
            product_dataset = vol_logic.volume_transfer_logic(bc, store, future, input_tpns, delist)

            # In[44]:
            prod_hrchy = read_frame(
                product_desc.objects.all().filter(buying_controller__in=bc).values('base_product_number',
                                                                                   'brand_indicator',
                                                                                   'long_description').distinct())
            # delist product table for UI
            delist_prod_table = product_dataset[['productcode', 'predicted_volume', 'predicted_value', 'predicted_cgm']]
            delist_prod_table = delist_prod_table.drop_duplicates().fillna(0).reset_index(drop=True).groupby(['productcode'], as_index = False).agg({'predicted_volume':max, 'predicted_value': max, 'predicted_cgm':max})
            delist_prod_table = pd.merge(delist_prod_table, prod_hrchy, left_on=['productcode'],
                                         right_on=['base_product_number'], how='left')
            del delist_prod_table['base_product_number']

            delist_prod_table = delist_prod_table.groupby(['productcode', 'long_description'], as_index=False).agg(
                {'predicted_volume': sum, 'predicted_value': sum, 'predicted_cgm': sum})
            delist_prod_table = delist_prod_table.drop_duplicates().fillna(0)
            contribution = read_frame(
                product_contri.objects.all().filter(buying_controller__in=bc, store_type__in=store,
                                                    time_period__in=future).values_list())

            contribution = contribution.rename(columns={'base_product_number': 'productcode'})
            delist_prod_table = pd.merge(delist_prod_table, contribution[['productcode', 'no_of_stores']],
                                         left_on=['productcode'], right_on=['productcode'], how='left')
            delist_prod_table = delist_prod_table[delist_prod_table['no_of_stores'] > 0]
            delist_prod_table = delist_prod_table[delist_prod_table['predicted_volume'] > 0]
            delist_prod_table = delist_prod_table.drop_duplicates().fillna(0).reset_index(drop=True)

            # list of prods & their substitutes
            delist_prod_subs_main = vol_logic.delist_subs(bc, ['Main Estate'], future, delist)
            delist_prod_subs = pd.DataFrame()
            delist_prod_subs = delist_prod_subs.append(delist_prod_subs_main)
            delist_table_popup = pd.DataFrame(delist_prod_subs)

        # Original Delist popup starts here
        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        args.pop('format__iexact', None)
        delist = args.get('delist_product__iexact')
        # delist = kwargs.get['product', -1]
        delist = [delist]
        delist_pop = pd.DataFrame(delist)
        delist_pop['product'] = delist_pop[0].astype('float')
        popup_data2 = pd.merge(delist_table_popup, delist_pop, left_on=['productcode'], right_on=['product'],
                               how='inner')
        popup_data2 = popup_data2.drop_duplicates().fillna(0).reset_index(drop=True)


        # ## set default page as 1
        # delist_popup_page = 1
        #
        # ## take page from args if entered
        # try:
        #   delist_popup_page = int(args.get('delist_popup_page__iexact'))
        # ## else 1
        # except:
        #   delist_popup_page = 1
        #
        # ## remove page number from args
        # args.pop('delist_popup_page__iexact', None)
        #
        # ## assign start and end points for subsetting data frame
        # start_row = (delist_popup_page - 1) * 8  ## example: for page 2 => 9
        # end_row = start_row + 8
        #
        # ## calculate total number of pages
        # num_pages = math.ceil((len(popup_data2) / 8))
        # ## calculate start index for data frame
        # start_index = (delist_popup_page - 1) * 8 + 1
        # #print(start_index)
        # # calculate total number of rows
        # count = len(popup_data2)
        # # calculate end index
        # end_index = delist_popup_page * 8
        # ## subset the queryset to display required data
        # popup_data2 = popup_data2.loc[start_row:end_row, ]

        data = {
            'delist_table_popup': popup_data2.to_dict(orient='records')
        }
        ## passing data in required format
        return JsonResponse({#'pagination_count': num_pages,
                             #'delist_popup_page': delist_popup_page,
                             #'start_index': start_index,
                             #'count': count,
                             #'end_index': end_index,
                             'table': data['delist_table_popup']}, safe=False)

#Save scenarios
class delist_scenario_final(vol_transfer_logic,APIView):

    def get(self, request, *args):
        args = {reqobj: request.GET.get(reqobj) for reqobj in request.GET.keys()}

        args.pop('format', None)


        designation = args.pop('designation', None)
        user_id = args.pop('user_id')
        session_id = args.pop('session_id')
        user_name = args.pop('user_name', None)
        buying_controller_header = args.pop('buying_controller_header', None)
        buyer_header = args.pop('buyer_header', None)
        #print("#printing args.................")
        #print(designation,buying_controller_header,buyer_header)
        user_attributes_args = args.copy()
        user_attributes = user_attributes_args
        scenario_name = args.pop('scenario_name', None)
        #print("scenario_name")
        #print(scenario_name)
        #event_name = args.pop('event_name', None)
        #user_id = args.pop('user_id', None)
        Buying_controller = args.pop('buying_controller',None)

        #bc = Buying_controller
        #par_supp = args.pop('parent_supplier',None)
        #Buyer = args.pop('buyer',None)

        #designation = args_list.pop('designation__in', None)
            #user_id = args_list.pop('user_id__in', None)
        #session_id = args_list.pop('session_id__in', None)
        #user_name = args_list.pop('user_name__in', None)
        #buying_controller_header = args_list.pop('buying_controller_header__in', None)
        #buyer_header = args_list.pop('buyer_header__in', None)
        args_list = {reqobj + '__in': request.GET.getlist(reqobj) for reqobj in request.GET.keys()}
        input_tpns = args_list.pop('long_description__in', 0)
        # #print(input_tpns)
        input_tpns_main = pd.DataFrame(input_tpns).reset_index(drop=True)
        input_tpns_main['base_product_number'] = input_tpns_main[0].copy()
        input_tpns_main['base_product_number'] = input_tpns_main['base_product_number'].str[-8:]
        input_tpns_main['base_product_number'] = input_tpns_main['base_product_number'].astype('int')
        input_tpns = input_tpns_main['base_product_number'].drop_duplicates().values.tolist()

        if Buying_controller is not None: #or Buyer is None:
            view_mine = "True"
        else:
            view_mine = "False"
        today = datetime.date.today()
        curr_time = today.ctime()
        # %H:%M"
        system_time = strftime("%Y-%m-%d", gmtime())
        #print("system time")
        #print(system_time)
        # to check if the scenario name exists already

        scenario = scenario_name
        check_value = str(user_id) + '_' + scenario

        ##print("check_value")
        ##print(check_value)

        x = list(delist_scenario.objects.values_list('user_id', 'scenario_name').distinct())
        x_df = pd.DataFrame(x, columns=["user_id", "scenario_name"])
        check_list = []
        x_df['check_list'] = x_df['user_id'] + '_' + x_df['scenario_name']
        ##print("xx")
        ##print(x_df)

        ##print(x_df['check_list'])
        check_list_data = list(x_df['check_list'])
        if check_value in check_list_data:
            result = "FAILURE"
        else:
            result = "SUCCESS"

        if result == "SUCCESS":
            #print("inside success")
            # read all files
            #print(".........inside_class..............")

            # all_filter = read_frame(product_impact_filter.objects.all())
            # input_tpns = all_filter['input_tpns']
            # input_tpns = list(input_tpns)
            bc = Buying_controller
           # bc = all_filter['bc'][0]
            bc = [bc]
                #store = all_filter['store'][0]
            store = ['Overview']
                #future = all_filter['future'][0]
                #future = [future]
            months = ['3_months', '6_months', '12_months']

            #print(".........inside_class..............")

            for i in range(0, len(months)):
                future = [months[i]]
                week_tab=str(future[0])
                if input_tpns[0] == 0:

                    input_tpns_main = read_frame(
                        nego_ads_drf.objects.all().filter(buying_controller__in=bc, store_type__in=['Main Estate'],
                                                          performance_quartile__in=['Low CPS/Low Profit'],
                                                          time_period__in=['Last 52 Weeks']).values(
                            'base_product_number').distinct())
                    delist_main = list(input_tpns_main['base_product_number'])
                    #print(delist_main)
                    #print(input_tpns_main)
                else:
                    # delist_main = input_tpns
                    input_tpns_main = pd.DataFrame(input_tpns).reset_index(drop=True)
                    input_tpns_main['base_product_number'] = input_tpns_main[0].copy()
                    #input_tpns_main['base_product_number'] = input_tpns_main['base_product_number'].str[-8:]
                    #input_tpns_main['base_product_number'] = input_tpns_main['base_product_number'].astype('int')
                    delist_main = input_tpns_main['base_product_number'].drop_duplicates().values.tolist()
                    #print(delist_main)
                # #print('below values are passed - main estate')
                # #print(args, bc, store, future, input_tpns_main)

                # In[6]:
                ##print(bc,future, input_tpns_main, delist_main)
                vol_logic = vol_transfer_logic()
                #print("Following values are passed...")
                #print(bc,future,input_tpns_main,delist_main)
                product_dataset_main = vol_logic.volume_transfer_logic(bc,['Main Estate'],future,input_tpns_main,delist_main)
         #       #print(product_dataset_main)

                # In[7]:

                if input_tpns[0] == 0:
                    input_tpns_exp = read_frame(
                        nego_ads_drf.objects.all().filter(buying_controller__in=bc, store_type__in=['Express'],
                                                          performance_quartile__in=['Low CPS/Low Profit'],
                                                          time_period__in=['Last 52 Weeks']).values(
                            'base_product_number').distinct())

                    delist_exp = list(input_tpns_exp['base_product_number'])
                else:
                    # delist_exp = input_tpns
                    input_tpns_exp = pd.DataFrame(input_tpns).reset_index(drop=True)
                    input_tpns_exp['base_product_number'] = input_tpns_exp[0].copy()
                    #input_tpns_exp['base_product_number'] = input_tpns_exp['base_product_number'].str[-8:]
                    #input_tpns_exp['base_product_number'] = input_tpns_exp['base_product_number'].astype('int')
                    delist_exp = input_tpns_exp['base_product_number'].drop_duplicates().values.tolist()

                #print('below values are passed - express')
                ##print(args, bc, store, future, input_tpns_exp)

                # In[8]:

                product_dataset_exp = vol_logic.volume_transfer_logic(bc, ['Express'], future, input_tpns_exp, delist_exp)


                # In[9]:

                product_dataset = pd.merge(product_dataset_main, product_dataset_exp,
                                           left_on=['productcode', 'substituteproductcode'],
                                           right_on=['productcode', 'substituteproductcode'], how='outer')

                # In[10]:

                product_dataset = product_dataset.drop_duplicates().fillna(0).reset_index(drop=True)

                # In[11]:

                product_dataset['predicted_volume'] = product_dataset['predicted_volume_x'] + product_dataset[
                    'predicted_volume_y']
                product_dataset['predicted_value'] = product_dataset['predicted_value_x'] + product_dataset[
                    'predicted_value_y']
                product_dataset['predicted_cgm'] = product_dataset['predicted_cgm_x'] + product_dataset['predicted_cgm_y']
                product_dataset['predicted_cts'] = product_dataset['predicted_cts_x'] + product_dataset['predicted_cts_y']
                product_dataset['volume_transfer'] = product_dataset['volume_transfer_x'] + product_dataset[
                    'volume_transfer_y']
                product_dataset['value_transfer'] = product_dataset['value_transfer_x'] + product_dataset[
                    'value_transfer_y']
                product_dataset['cgm_transfer'] = product_dataset['cgm_transfer_x'] + product_dataset['cgm_transfer_y']
                product_dataset['cts_transfer'] = product_dataset['cts_transfer_x'] + product_dataset['cts_transfer_y']
                product_dataset['brand_indicator_x'] = product_dataset['brand_indicator_x'].fillna(
                    product_dataset['brand_indicator_y'])
                product_dataset['brand_indicator_y'] = product_dataset['brand_indicator_x'].fillna(
                    product_dataset['brand_indicator_y'])
                product_dataset['brand_indicator'] = product_dataset.brand_indicator_x.combine_first(
                    product_dataset.brand_indicator_y)
                del product_dataset["brand_indicator_x"]
                del product_dataset["brand_indicator_y"]

                product_dataset = product_dataset[
                    ['productcode', 'substituteproductcode', 'brand_indicator', 'predicted_volume', 'predicted_value',
                     'predicted_cgm', 'predicted_cts', 'volume_transfer', 'value_transfer', 'cgm_transfer', 'cts_transfer']]

                chart_data = vol_logic.waterfall_chart(product_dataset,bc,future)

                #supplier table for overview
                sup_table_main,sup_sales_table_main = vol_logic.supplier_table(product_dataset_main, ['Main Estate'], future, bc)
                sup_table_exp,sup_sales_table_exp = vol_logic.supplier_table(product_dataset_exp, ['Express'], future, bc)

                sup_sales_table = pd.merge(sup_sales_table_main, sup_sales_table_exp, left_on=['parent_supplier'],
                                           right_on=['parent_supplier'], how='outer')

                # In[30]:
                sup_sales_table = sup_sales_table.drop_duplicates().reset_index(drop=True).fillna(0)
                sup_sales_table['predicted_volume_share'] = sup_sales_table['predicted_volume_share_x'] + sup_sales_table[
                    'predicted_volume_share_y']
                sup_sales_table['vols_gain_share'] = sup_sales_table['vols_gain_share_x'] + sup_sales_table[
                    'vols_gain_share_y']
                sup_sales_table['vols_loss_share'] = sup_sales_table['vols_loss_share_x'] + sup_sales_table[
                    'vols_loss_share_y']

                sup_sales_table['predicted_value_share'] = sup_sales_table['predicted_value_share_x'] + sup_sales_table[
                    'predicted_value_share_y']
                sup_sales_table['value_gain_share'] = sup_sales_table['value_gain_share_x'] + sup_sales_table[
                    'value_gain_share_y']
                sup_sales_table['value_loss_share'] = sup_sales_table['value_loss_share_x'] + sup_sales_table[
                    'value_loss_share_y']

                sup_sales_table = sup_sales_table[
                    ['parent_supplier', 'predicted_volume_share', 'vols_gain_share', 'vols_loss_share',
                     'predicted_value_share', 'value_gain_share', 'value_loss_share']]

                # In[31]:

                sup_sales_table['vol_impact'] = sup_sales_table['vols_gain_share'] - sup_sales_table['vols_loss_share']
                sup_sales_table['value_impact'] = sup_sales_table['value_gain_share'] - sup_sales_table['value_loss_share']

                sup_sales_table['predicted_volume_share'] = sup_sales_table['predicted_volume_share'].replace(0, 1)
                sup_sales_table['predicted_value_share'] = sup_sales_table['predicted_value_share'].replace(0, 1)

                try:
                    sup_sales_table['vol_impact_per'] = (sup_sales_table['vol_impact'] * 100) / sup_sales_table[
                        'predicted_volume_share']
                except:
                    sup_sales_table['vol_impact_per'] = 0

                try:
                    sup_sales_table['value_impact_per'] = (sup_sales_table['value_impact'] * 100) / sup_sales_table[
                        'predicted_value_share']
                except:
                    sup_sales_table['value_impact_per'] = 0

                sup_sales_table = sup_sales_table[sup_sales_table['vol_impact'] != 0]
                #df.round({'A': 1, 'C': 2})
                sup_sales_table['vol_impact_per'] = sup_sales_table['vol_impact_per'].round(decimals=1)
                sup_sales_table['value_impact_per'] = sup_sales_table['value_impact_per'].round(decimals=1)

                # delist product table for UI
                delist_prod_table = product_dataset[
                    ['productcode', 'substituteproductcode', 'predicted_volume', 'predicted_value', 'predicted_cgm',
                     'volume_transfer', 'value_transfer']]

                delist_prod_table = delist_prod_table.drop_duplicates().fillna(0).reset_index(drop=True).groupby(
                    ['productcode'], as_index=False).agg(
                    {'predicted_volume': max, 'predicted_value': max, 'predicted_cgm': max,'volume_transfer':sum,'value_transfer':sum})
                prod_hrchy = read_frame(product_desc.objects.all().values('base_product_number', 'brand_indicator',
                                                                          'long_description').distinct())

                delist_prod_table = pd.merge(delist_prod_table, prod_hrchy, left_on=['productcode'],
                                             right_on=['base_product_number'], how='left')
                del delist_prod_table['base_product_number']
                delist_prod_table = delist_prod_table.drop_duplicates().fillna(0).reset_index(drop=True)

               # delist_prod_table = delist_prod_table.groupby(['productcode', 'long_description'], as_index=False).agg(
                #    {'predicted_volume': sum, 'predicted_value': sum, 'predicted_cgm': sum})

                # In[22]:
                contribution_main = read_frame(
                    product_contri.objects.all().filter(buying_controller__in=bc, store_type__in=['Main Estate'],
                                                        time_period__in=future).values_list())

                contribution_main = contribution_main.rename(columns={'base_product_number': 'productcode'})

                contribution_exp = read_frame(
                    product_contri.objects.all().filter(buying_controller__in=bc, store_type__in=['Express'],
                                                        time_period__in=future).values_list())

                contribution_exp = contribution_exp.rename(columns={'base_product_number': 'productcode'})
                overview_store = pd.merge(contribution_main[['productcode', 'no_of_stores']],
                                          contribution_exp[['productcode', 'no_of_stores']], left_on=['productcode'],
                                          right_on=['productcode'], how='outer')
                overview_store = overview_store.drop_duplicates().fillna(0).reset_index(drop=True)
                overview_store['no_of_stores'] = overview_store['no_of_stores_x'] + overview_store['no_of_stores_y']

                # In[23]:

                delist_prod_table = pd.merge(delist_prod_table, overview_store[['productcode', 'no_of_stores']],
                                             left_on=['productcode'], right_on=['productcode'], how='left')
                delist_prod_table = delist_prod_table[delist_prod_table['no_of_stores'] > 0]
                delist_prod_table = delist_prod_table[delist_prod_table['predicted_volume'] > 0]
                delist_prod_table['per_vol_transfer'] = ((delist_prod_table['volume_transfer'] / delist_prod_table[
                    'predicted_volume']) * 100).round(decimals=1)
                delist_prod_table['per_value_transfer'] = ((delist_prod_table['value_transfer'] / delist_prod_table[
                    'predicted_value']) * 100).round(decimals=1)

                delist_prod_table = delist_prod_table.drop_duplicates().fillna(0).reset_index(drop=True)
                delist_prod_table['productcode'] =delist_prod_table['productcode'].astype('int')

                # adding psg impact column
                psg_predict_main = vol_logic.psg_impact(bc, delist_main, ['Main Estate'], future)
                psg_predict_exp = vol_logic.psg_impact(bc, delist_exp, ['Express'], future)

                psg_predict = pd.merge(psg_predict_main, psg_predict_exp, on="product_sub_group_description",
                                       how='outer')
                psg_predict = psg_predict.drop_duplicates().reset_index(drop=True).fillna(0)

                psg_predict['psg_predicted_sales'] = psg_predict['psg_predicted_sales_x'] + psg_predict[
                    'psg_predicted_sales_y']

                del psg_predict['psg_predicted_sales_x']
                del psg_predict['psg_predicted_sales_y']

                psg = read_frame(product_hierarchy.objects.all().filter(buying_controller__in=bc,
                                                                        base_product_number__in=delist_main).values(
                    'product_sub_group_code', 'base_product_number', 'product_sub_group_description').distinct())
                delist_prod_table = pd.merge(delist_prod_table,
                                             psg[['base_product_number', 'product_sub_group_description']],
                                             left_on="productcode", right_on="base_product_number", how="left")
                del delist_prod_table['base_product_number']
                delist_prod_table = pd.merge(delist_prod_table, psg_predict, on="product_sub_group_description",
                                             how="left")

                delist_prod_table['psg_value_impact'] = (
                    delist_prod_table['predicted_value'] / delist_prod_table['psg_predicted_sales']).round(decimals=1)
                delist_prod_table = delist_prod_table[['productcode', 'long_description', 'predicted_value',
                                                       'predicted_volume', 'predicted_cgm', 'no_of_stores',
                                                       'per_vol_transfer','per_value_transfer',
                                                       'product_sub_group_description', 'psg_value_impact']]

                chart_attr = chart_data


                #delist_prods = {'delist_prods':}
                supp_attr = {'sup_attr': sup_sales_table.to_dict(orient='records')}
                delist_attr = {'delist_attr': delist_prod_table.to_dict(orient='records')}
                #print("for loop running...")
                #print(i)
                save_scenario = delist_scenario(scenario_name = scenario_name,
                                                session_id = session_id,
                                                user_id = user_id,
                                                user_name= user_name,
                                                buying_controller=buying_controller_header,
                                                designation=designation,
                                                buyer=buyer_header,
                                                time_period = week_tab,
                                                user_attributes = user_attributes,
                                                chart_attr = chart_attr,
                                                supp_attr = supp_attr,
                                                delist_attr = delist_attr,
                                                system_time=system_time,
                                                page="delist",
                                                view_mine = view_mine,
                                                input_tpns = delist_prod_table['productcode'].tolist())
                save_scenario.save()
        #result= "SUCCESS"

        return JsonResponse({"save_scenario" : result}, safe=False)

# View scenarios
class display_delist_scenario(vol_transfer_logic,APIView):
    def get(self,request,format=None):
        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        args.pop('format__iexact', None)
        user_id = args.get('user_id__iexact', None)
        scenario_name = args.get('scenario_name__iexact', None)
        #event_name = args.get('event_name__iexact', None)

        queryset_13 = read_frame(delist_scenario.objects.all().filter(**args).filter(time_period="3_months").values('chart_attr','supp_attr','delist_attr'))
        queryset_13 = {'queryset_13': queryset_13.to_dict(orient='records')}
        #print(type(queryset_13))
        queryset_26 = read_frame(delist_scenario.objects.all().filter(**args).filter(time_period="6_months").values('chart_attr','supp_attr','delist_attr'))
        queryset_26 = {'queryset_26': queryset_26.to_dict(orient='records')}
        #print(type(queryset_26))
        queryset_52 = read_frame(delist_scenario.objects.all().filter(**args).filter(time_period="12_months").values('chart_attr','supp_attr','delist_attr'))
        queryset_52 = {'queryset_52': queryset_52.to_dict(orient='records')}
        #print(type(queryset_52))

        return JsonResponse({
                "user_id":user_id,
                "scenario_name": scenario_name,
                #"event_name": event_name,
                "week_13" : queryset_13,
                "week_26" : queryset_26,
                "week_52" : queryset_52})

# List scenarios
class delist_scenario_list(APIView):
    def get(self, request, format=None):
        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        user_id = args.pop('user_id__iexact', None)
        queryset = delist_scenario.objects.filter(user_id=user_id).values('system_time','scenario_name').distinct().order_by('-system_time')
        serializer_class = delist_savescenarioserializer(queryset, many=True)
        return JsonResponse(serializer_class.data, safe=False)



