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
from django.conf import settings
import pandas as pd
from django_pandas.io import read_frame
from django.utils import six
import json
import numpy as np
import re

# models for product impact and negotiation filters
from .models import product_hierarchy, nego_ads_drf
import numpy as np
# for cache
from rest_framework_extensions.cache.decorators import cache_response

# logs
import os
import datetime

import environ
import logging

ROOT_DIR = environ.Path(__file__) - 1

env = environ.Env()
env_file = str(ROOT_DIR.path('.env'))
print('Loading : {}'.format(env_file))
env.read_env(env_file)
print('The .env file has been loaded. See base.py for more information')
if not os.path.exists('logs'):
    os.makedirs('logs')
if not os.path.exists('logs/' + os.path.basename(__file__)[:-3]):
    os.makedirs('logs/' + os.path.basename(__file__)[:-3])
# defaults
# logging.basicConfig(filename='logs/' + os.path.basename(__file__)[:-3] + '/' + os.path.basename(__file__)[:-3] + '_' +
#                             str(datetime.datetime.utcnow())[:-7] + '.log',
#                     level=logging.DEBUG,
#                     format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')



#### Negotiation View Filters
def col_distinct(kwargs, col_name, kwargs_header):
    queryset = nego_ads_drf.objects.filter(**kwargs_header).filter(**kwargs).values(col_name).order_by(
        col_name).distinct()
    base_product_number_list = [k.get(col_name) for k in queryset]
    return base_product_number_list


class negotiation_filters(APIView):
    def get(self, request):
        # args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}

        obj = {}
        get_keys = request.GET.keys()

        for i in get_keys:
            obj[i] = request.GET.getlist(i)

        sent_req = obj
        user_id = sent_req.pop('user_id', None)
        designation = sent_req.pop('designation', None)
        session_id = sent_req.pop('session_id', None)
        user_name = sent_req.pop('user_name', None)
        buying_controller_header = sent_req.pop('buying_controller_header', None)
        buyer_header = sent_req.pop('buyer_header', None)

        if 'admin' in designation:
            kwargs_header = {}
        else:
            if buyer_header is None:
                kwargs_header = {
                    'buying_controller__in': buying_controller_header
                }
            else:
                kwargs_header = {
                    'buying_controller__in': buying_controller_header,
                    'buyer__in': buyer_header
                }

        cols = ['buying_controller', 'buyer', 'junior_buyer', 'product_sub_group_description', 'need_state',
                'brand_name']

        # find lowest element of cols
        lowest = 0
        second_lowest = 0

        element_list = []
        for i in sent_req.keys():
            if i in cols:
                element_list.append(cols.index(i))

        element_list.sort()

        try:
            lowest = element_list[-1]
        except:
            pass

        try:
            second_lowest = element_list[-2]
        except:
            pass

        lowest_key = cols[lowest]
        second_lowest_key = cols[lowest]

        final_list = []  # final list to send

        col_unique_list_name = []  # rename
        col_unique_list_name_obj = {}  # rename
        for col_name in cols:
            col_unique_list = col_distinct({}, col_name, kwargs_header)
            col_unique_list_name.append({'name': col_name,
                                         'unique_elements': col_unique_list})
            col_unique_list_name_obj[col_name] = col_unique_list
            # args sent as url params
            kwargs2 = {reqobj + '__in': sent_req.get(reqobj) for reqobj in sent_req.keys()}

            category_of_sent_obj_list = col_distinct(kwargs2, col_name, kwargs_header)

            sent_obj_category_list = []

            # get unique elements for `col_name`
            for i in category_of_sent_obj_list:
                sent_obj_category_list.append(i)

            def highlight_check(category_unique):

                if len(sent_req.keys()) > 0:
                    highlighted = False
                    if col_name in sent_req.keys():
                        if col_name == cols[lowest]:
                            queryset = nego_ads_drf.objects.filter(**{col_name: category_unique})[:1].get()
                            y = getattr(queryset, cols[second_lowest])
                            for i in sent_req.keys():

                                if y in sent_req.get(i) and cols[second_lowest] == i:
                                    highlighted = True

                            return highlighted
                        else:
                            return False
                    else:
                        if category_unique in sent_obj_category_list:
                            highlighted = True
                        return highlighted
                else:
                    return True

            # assign props to send as json response

            y = []
            for title in col_unique_list:
                selected = True if type(sent_req.get(col_name)) == list and title in sent_req.get(col_name) else False
                y.append({'title': title,
                          'resource': {'params': col_name + '=' + title,
                                       'selected': selected},
                          'highlighted': selected if selected else highlight_check(title)})

            final_list.append({'items': y,
                               'input_type': 'Checkbox',
                               'title': col_name,
                               'buying_controller': 'Beers, Wines and Spirits',
                               'id': col_name,
                               'required': True if col_name == 'buying_controller' else False
                               })

        def get_element_type(title):
            if title == 'buying_controller':
                return 'Checkbox'
            else:
                return 'Checkbox'

        # sort list with checked at top

        final_list2 = []
        for i in final_list:
            m = []
            for j in i.get('items'):

                if j['resource']['selected']:
                    m.append(j)

            for j in i.get('items'):
                if not j['resource']['selected']:
                    m.append(j)

            final_list2.append({'items': m,
                                'input_type': get_element_type(i['title']),
                                'title': i['title'],
                                'required': i['required'],
                                'category_director': 'Beers, Wines and Spirits',
                                'id': i['id']})
        return JsonResponse({'cols': cols, 'checkbox_list': final_list2}, safe=False)


#### Product Impact Filters
def col_distinct_product(kwargs, col_name, kwargs_header):
    queryset = product_hierarchy.objects.filter(**kwargs_header).filter(**kwargs).values(col_name).order_by(
        col_name).distinct()
    base_product_number_list = [k.get(col_name) for k in queryset]
    return base_product_number_list


class product_impact_filters(APIView):
    def get(self, request):

        obj = {}
        get_keys = request.GET.keys()
        for i in get_keys:
            obj[i] = request.GET.getlist(i)

        sent_req = obj
        user_id = sent_req.pop('user_id', None)
        designation = sent_req.pop('designation', None)
        session_id = sent_req.pop('session_id', None)
        user_name = sent_req.pop('user_name', None)
        buying_controller_header = sent_req.pop('buying_controller_header', None)
        buyer_header = sent_req.pop('buyer_header', None)

        if 'admin' in designation:
            kwargs_header = {}
        else:
            if buyer_header is None:
                kwargs_header = {
                    'buying_controller__in': buying_controller_header
                }
            else:
                kwargs_header = {
                    'buying_controller__in': buying_controller_header,
                    'buyer__in': buyer_header
                }

        cols = ['buying_controller', 'parent_supplier', 'buyer', 'junior_buyer', 'brand_indicator',
                'brand_name', 'product_sub_group_description', 'long_description']

        # find lowest element of cols
        lowest = 0
        second_lowest = 0

        element_list = []
        for i in sent_req.keys():
            if i in cols:
                element_list.append(cols.index(i))

        element_list.sort()

        try:
            lowest = element_list[-1]
        except:
            pass

        try:
            second_lowest = element_list[-2]
        except:
            pass

        lowest_key = cols[lowest]
        second_lowest_key = cols[lowest]

        final_list = []  # final list to send

        col_unique_list_name = []  # rename
        col_unique_list_name_obj = {}  # rename
        for col_name in cols:
            col_unique_list = col_distinct_product({}, col_name, kwargs_header)
            col_unique_list_name.append({'name': col_name,
                                         'unique_elements': col_unique_list})
            col_unique_list_name_obj[col_name] = col_unique_list
            # args sent as url params
            kwargs2 = {reqobj + '__in': sent_req.get(reqobj) for reqobj in sent_req.keys()}

            category_of_sent_obj_list = col_distinct_product(kwargs2, col_name, kwargs_header)

            sent_obj_category_list = []

            # get unique elements for `col_name`
            for i in category_of_sent_obj_list:
                sent_obj_category_list.append(i)

            def highlight_check(category_unique):

                if len(sent_req.keys()) > 0:
                    highlighted = False
                    if col_name in sent_req.keys():
                        if col_name == cols[lowest]:
                            queryset = product_hierarchy.objects.filter(**{col_name: category_unique})[:1].get()
                            y = getattr(queryset, cols[second_lowest])
                            for i in sent_req.keys():

                                if y in sent_req.get(i) and cols[second_lowest] == i:
                                    highlighted = True

                            return highlighted
                        else:
                            return False
                    else:
                        if category_unique in sent_obj_category_list:
                            highlighted = True
                        return highlighted
                else:
                    return True

            # assign props to send as json response

            y = []
            for title in col_unique_list:
                selected = True if type(sent_req.get(col_name)) == list and title in sent_req.get(col_name) else False
                y.append({'title': title,
                          'resource': {'params': col_name + '=' + title,
                                       'selected': selected},
                          'highlighted': selected if selected else highlight_check(title)})

            final_list.append({'items': y,
                               'input_type': 'Checkbox',
                               'title': col_name,
                               'buying_controller': 'Beers, Wines and Spirits',
                               'id': col_name,
                               'required': True if col_name in ['buying_controller', 'long_description'] else False
                               })

        def get_element_type(title):
            if title == 'buying_controller':
                return 'Checkbox'
            else:
                return 'Checkbox'

        # sort list with checked at top

        final_list2 = []
        for i in final_list:
            m = []
            for j in i.get('items'):

                if j['resource']['selected']:
                    m.append(j)

            for j in i.get('items'):
                if not j['resource']['selected']:
                    m.append(j)

            final_list2.append({'items': m,
                                'input_type': get_element_type(i['title']),
                                'title': i['title'],
                                'required': i['required'],
                                'category_director': 'Beers, Wines and Spirits',
                                'id': i['id']})
        return JsonResponse({'cols': cols, 'checkbox_list': final_list2}, safe=False)


class negotiation_filters_new(APIView):
    def get(self, request, format=None):
        # input from header
        args = {reqobj + '__in': request.GET.getlist(reqobj) for reqobj in request.GET.keys()}

        designation = args.pop('designation__in', None)
        user_id = args.pop('user_id__in', None)
        session_id = args.pop('session_id__in', None)
        user_name = args.pop('user_name__in', None)
        buying_controller_header = args.pop('buying_controller_header__in', None)
        buyer_header = args.pop('buyer_header__in', None)

        # header over
        cols = ['buying_controller', 'buyer', 'junior_buyer', 'product_sub_group_description', 'need_state',
                'brand_name']

        if 'admin' in designation:
            kwargs_header = {}
        else:
            if buyer_header is None:
                kwargs_header = {
                    'buying_controller__in': buying_controller_header
                }
            else:
                kwargs_header = {
                    'buying_controller__in': buying_controller_header,
                    'buyer__in': buyer_header
                }
        # input from args
        default = args.pop('default__in', None)
        if default is None:
            if not args:

                df = read_frame(nego_ads_drf.objects.filter(**kwargs_header).filter(**args))
                heirarchy = read_frame(
                    nego_ads_drf.objects.filter(**kwargs_header).values('buying_controller', 'buyer', 'junior_buyer',
                                                                        'product_sub_group_description', 'need_state',
                                                                        'brand_name'))

                data = {'buying_controller': df.buying_controller.unique()}
                bc = pd.DataFrame(data)
                if len(bc) == 1:
                    bc['selected'] = True  ### One change here for default selection of bc logging in
                    bc['highlighted'] = False
                else:
                    bc['selected'] = False  ### One change here for default selection of bc logging in
                    bc['highlighted'] = False

                data = {'buyer': df.buyer.unique()}
                buyer = pd.DataFrame(data)

                ### Changes here to
                if len(buyer) == 1:
                    buyer['selected'] = True
                    buyer['highlighted'] = False
                else:
                    buyer['selected'] = False
                    buyer['highlighted'] = False

                data = {'junior_buyer': df.junior_buyer.unique()}
                jr_buyer = pd.DataFrame(data)
                jr_buyer['selected'] = False
                jr_buyer['highlighted'] = False

                data = {'product_sub_group_description': df.product_sub_group_description.unique()}
                psg = pd.DataFrame(data)
                psg['selected'] = False
                psg['highlighted'] = False

                data = {'need_state': df.need_state.unique()}
                need_state = pd.DataFrame(data)
                need_state['selected'] = False
                need_state['highlighted'] = False

                data = {'brand_name': df.brand_name.unique()}
                brand_name = pd.DataFrame(data)
                brand_name['selected'] = False
                brand_name['highlighted'] = False

                bc_df = heirarchy[['buying_controller']].drop_duplicates()

                buyer_df = heirarchy[['buyer']].drop_duplicates()

                jr_buyer_df = heirarchy[['junior_buyer']].drop_duplicates()

                psg_df = heirarchy[['product_sub_group_description']].drop_duplicates()

                need_state_df = heirarchy[['need_state']].drop_duplicates()

                brand_name_df = heirarchy[['brand_name']].drop_duplicates()

                bc_df = pd.merge(bc_df, bc, how='left')
                bc_df['selected'] = bc_df['selected'].fillna(False)
                bc_df['highlighted'] = bc_df['highlighted'].fillna(False)

                bc_df = bc_df.rename(columns={'buying_controller': 'title'})

                buyer_df = pd.merge(buyer_df, buyer, how='left')
                buyer_df['selected'] = buyer_df['selected'].fillna(False)
                buyer_df['highlighted'] = buyer_df['highlighted'].fillna(False)
                buyer_df = buyer_df.rename(columns={'buyer': 'title'})

                jr_buyer_df = pd.merge(jr_buyer_df, jr_buyer, how='left')
                jr_buyer_df['selected'] = jr_buyer_df['selected'].fillna(False)
                jr_buyer_df['highlighted'] = jr_buyer_df['highlighted'].fillna(False)
                jr_buyer_df = jr_buyer_df.rename(columns={'junior_buyer': 'title'})

                psg_df = pd.merge(psg_df, psg, how='left')
                psg_df['selected'] = psg_df['selected'].fillna(False)
                psg_df['highlighted'] = psg_df['highlighted'].fillna(False)
                psg_df = psg_df.rename(columns={'product_sub_group_description': 'title'})

                need_state_df = pd.merge(need_state_df, need_state, how='left')
                need_state_df['selected'] = need_state_df['selected'].fillna(False)
                need_state_df['highlighted'] = need_state_df['highlighted'].fillna(False)
                need_state_df = need_state_df.rename(columns={'need_state': 'title'})

                brand_name_df = pd.merge(brand_name_df, brand_name, how='left')
                brand_name_df['selected'] = brand_name_df['selected'].fillna(False)
                brand_name_df['highlighted'] = brand_name_df['highlighted'].fillna(False)
                brand_name_df = brand_name_df.rename(columns={'brand_name': 'title'})

                bc_df = bc_df.sort_values(by='title', ascending=True)
                bc_df['highlighted'] = ~bc_df['highlighted']
                bc_df_sel = bc_df[['selected']]
                bc_df['resource'] = bc_df_sel.to_dict(orient='records')
                del bc_df['selected']
                bc_final = bc_df.to_json(orient='records')
                bc_final = json.loads(bc_final)

                a = {}
                a['id'] = 'buying_controller'
                a['title'] = 'buying_controller'
                a['required'] = True
                a['items'] = bc_final
                a['category_director'] = "Beers, Wines and Spirits"

                buyer_df = buyer_df.sort_values(by='title', ascending=True)
                buyer_df['highlighted'] = ~buyer_df['highlighted']
                buyer_df_sel = buyer_df[['selected']]
                buyer_df['resource'] = buyer_df_sel.to_dict(orient='records')
                del buyer_df['selected']

                buyer_final = buyer_df.to_json(orient='records')
                buyer_final = json.loads(buyer_final)

                b = {}
                b['id'] = 'buyer'
                b['title'] = 'buyer'
                b['required'] = False
                b['items'] = buyer_final
                b['category_director'] = "Beers, Wines and Spirits"

                jr_buyer_df = jr_buyer_df.sort_values(by='title', ascending=True)
                jr_buyer_df['highlighted'] = ~jr_buyer_df['highlighted']
                jr_buyer_df_sel = jr_buyer_df[['selected']]
                jr_buyer_df['resource'] = jr_buyer_df_sel.to_dict(orient='records')
                del jr_buyer_df['selected']

                jr_buyer_final = jr_buyer_df.to_json(orient='records')
                jr_buyer_final = json.loads(jr_buyer_final)

                c = {}
                c['id'] = 'junior_buyer'
                c['title'] = 'junior_buyer'
                c['required'] = False
                c['items'] = jr_buyer_final
                c['category_director'] = "Beers, Wines and Spirits"

                psg_df = psg_df.sort_values(by='title', ascending=True)
                psg_df['highlighted'] = ~psg_df['highlighted']
                psg_df_sel = psg_df[['selected']]
                psg_df['resource'] = psg_df_sel.to_dict(orient='records')
                del psg_df['selected']

                psg_final = psg_df.to_json(orient='records')
                psg_final = json.loads(psg_final)

                d = {}
                d['id'] = 'product_sub_group_description'
                d['title'] = 'product_sub_group_description'
                d['required'] = False
                d['items'] = psg_final
                d['category_director'] = "Beers, Wines and Spirits"

                need_state_df = need_state_df.sort_values(by='title', ascending=True)
                need_state_df['highlighted'] = ~need_state_df['highlighted']
                need_state_df_sel = need_state_df[['selected']]
                need_state_df['resource'] = need_state_df_sel.to_dict(orient='records')
                del need_state_df['selected']

                need_state_final = need_state_df.to_json(orient='records')
                need_state_final = json.loads(need_state_final)

                e = {}
                e['id'] = 'need_state'
                e['title'] = 'need_state'
                e['required'] = False
                e['items'] = need_state_final
                e['category_director'] = "Beers, Wines and Spirits"

                brand_name_df = brand_name_df.sort_values(by='title', ascending=True)
                brand_name_df['highlighted'] = ~brand_name_df['highlighted']
                brand_name_df_sel = brand_name_df[['selected']]
                brand_name_df['resource'] = brand_name_df_sel.to_dict(orient='records')
                del brand_name_df['selected']

                brand_name_final = brand_name_df.to_json(orient='records')
                brand_name_final = json.loads(brand_name_final)

                f = {}
                f['title'] = 'brand_name'
                f['id'] = 'brand_name'
                f['required'] = False
                f['items'] = brand_name_final
                f['category_director'] = "Beers, Wines and Spirits"
                final = []
                final.append(a)
                final.append(b)
                final.append(c)
                final.append(d)
                final.append(e)
                final.append(f)
            else:



                if 'admin' in designation:
                    heirarchy = read_frame(
                        nego_ads_drf.objects.values('buying_controller', 'buyer',
                                                                            'junior_buyer',
                                                                            'product_sub_group_description',
                                                                            'need_state',
                                                                            'brand_name'))
                else:
                    heirarchy = read_frame(nego_ads_drf.objects.filter(**kwargs_header).values('buying_controller', 'buyer', 'junior_buyer',
                                                                       'product_sub_group_description', 'need_state',
                                                                       'brand_name'))

                bc_df = heirarchy[['buying_controller']].drop_duplicates()
                buyer_df_heirarchy = heirarchy[['buyer']].drop_duplicates()
                jr_buyer_df_heirarhcy = heirarchy[['junior_buyer']].drop_duplicates()
                psg_df_heirarchy = heirarchy[['product_sub_group_description']].drop_duplicates()
                need_state_df_heirarchy = heirarchy[['need_state']].drop_duplicates()
                brand_name_df_heirarchy = heirarchy[['brand_name']].drop_duplicates()

                args_list = {reqobj + '__in': request.GET.getlist(reqobj) for reqobj in request.GET.keys()}
                bc_list = args_list.pop('buying_controller_header__in', None)
                buyer_list = args_list.pop('buyer_header__in', None)
                jr_buyer_list = args_list.pop('junior_buyer__in', None)
                psg_list = args_list.pop('product_sub_group_description__in', None)
                need_state_list = args_list.pop('need_state__in', None)
                brand_name_list = args_list.pop('brand_name__in', None)
                df = read_frame(nego_ads_drf.objects.filter(**args))

                data = {'buying_controller': df.buying_controller.unique()}
                bc = pd.DataFrame(data)
                bc = pd.DataFrame(data)

                data = {'buyer': df.buyer.unique()}
                buyer = pd.DataFrame(data)
                print(buyer)

                data = {'junior_buyer': df.junior_buyer.unique()}
                jr_buyer = pd.DataFrame(data)
                print(jr_buyer)

                data = {'product_sub_group_description': df.product_sub_group_description.unique()}
                psg = pd.DataFrame(data)

                data = {'need_state': df.need_state.unique()}
                need_state = pd.DataFrame(data)

                data = {'brand_name': df.brand_name.unique()}
                brand_name = pd.DataFrame(data)

                bc_df =bc
                bc_df['selected'] = True
                bc_df['highlighted'] = False
                #bc_df = pd.merge(bc_df, bc, how='left')
                #print(bc_df.columns)
                #bc_df['selected'] = bc_df['selected'].fillna(False)
                #bc_df['highlighted'] = bc_df['highlighted'].fillna(True)

                bc_df = bc_df.rename(columns={'buying_controller': 'title'})

                heirarchy_check = read_frame(
                    nego_ads_drf.objects.filter(buying_controller__in=bc_list).values('buying_controller', 'buyer',
                                                                                      'junior_buyer',
                                                                                      'product_sub_group_description',
                                                                                      'need_state',
                                                                                      'brand_name'))
                if buyer_list is not None:
                    print("inside buyerrr..")
                    buyer['selected'] = True
                    buyer['highlighted'] = False
                    buyer_df_check = pd.merge(buyer_df_heirarchy, heirarchy_check[['buyer']].drop_duplicates(),
                                              on="buyer", how='right')
                    print("after merge_1...")
                    print(buyer_df_check)
                    print("printing buyer")
                    print(buyer)
                    buyer_df_selected = pd.merge(buyer_df_check[['buyer']], buyer, on="buyer", how='left')
                    print("after mergeeee...")
                    buyer_df_selected['selected'] = buyer_df_selected['selected'].fillna(False)
                    buyer_df_selected['highlighted'] = buyer_df_selected['highlighted'].fillna(False)
                    print(buyer_df_selected)

                    buyer_df = pd.merge(buyer_df_heirarchy, buyer_df_selected, on="buyer", how='left')
                    buyer_df['selected'] = buyer_df['selected'].fillna(False)
                    buyer_df['highlighted'] = buyer_df['highlighted'].fillna(True)
                    print(buyer_df)
                    buyer_df = buyer_df.rename(columns={'buyer': 'title'})

                    if jr_buyer_list is not None:
                        print("inside junior buyer")
                        buyer_df = buyer_df.rename(columns={'title': 'buyer'})
                        buyer_list_df = pd.DataFrame(buyer_list, columns={'buyer'})
                        data = {'buyer': buyer_list_df.buyer.unique()}
                        print(data)
                        buyer = pd.DataFrame(data)
                        buyer['selected'] = True
                        buyer['highlighted'] = False
                        print(buyer)
                        buyer_df = pd.merge(buyer_df_heirarchy, buyer, how='left')
                        buyer_df['selected'] = buyer_df['selected'].fillna(False)
                        buyer_df['highlighted'] = buyer_df['highlighted'].fillna(True)
                        buyer_df = buyer_df[['buyer', 'selected', 'highlighted']]
                        buyer_df = buyer_df.rename(columns={'buyer': 'title'})
                        print(buyer_df)
                else:
                    buyer['selected'] = False
                    buyer['highlighted'] = False
                    buyer_df = pd.merge(buyer_df_heirarchy, buyer, how='left')
                    buyer_df['selected'] = buyer_df['selected'].fillna(False)
                    buyer_df['highlighted'] = buyer_df['highlighted'].fillna(True)
                    buyer_df = buyer_df.rename(columns={'buyer': 'title'})

                if jr_buyer_list is not None:
                    jr_buyer['selected'] = True
                    jr_buyer['highlighted'] = False
                    if buyer_list is not None:
                        print("buyer list is present")
                        kwargs_jr_buyer= {
                            'buyer__in': buyer_list,
                            'product_sub_group_description__in': psg_list,
                            'brand_name__in': brand_name_list,
                            'need_state__in':need_state_list
                        }
                        kwargs_jr_buyer = dict(filter(lambda item: item[1] is not None, kwargs_jr_buyer.items()))
                        print(kwargs_jr_buyer)
                        heirarchy_check = read_frame(
                            nego_ads_drf.objects.filter(**kwargs_jr_buyer))

                    jr_buyer_df_check = pd.merge(jr_buyer_df_heirarhcy,
                                                 heirarchy_check[['junior_buyer']].drop_duplicates(), on="junior_buyer",
                                                 how='right')

                    print("after merge_1...")
                    print(jr_buyer_df_check)
                    print(jr_buyer)
                    jr_buyer_df_selected = pd.merge(jr_buyer_df_check[['junior_buyer']], jr_buyer, on="junior_buyer",
                                                    how='left')
                    print("after mergeeee...")
                    jr_buyer_df_selected['selected'] = jr_buyer_df_selected['selected'].fillna(False)
                    jr_buyer_df_selected['highlighted'] = jr_buyer_df_selected['highlighted'].fillna(False)
                    print(jr_buyer_df_selected)
                    jr_buyer_df = pd.merge(jr_buyer_df_heirarhcy, jr_buyer_df_selected, how='left')
                    jr_buyer_df['selected'] = jr_buyer_df['selected'].fillna(False)
                    jr_buyer_df['highlighted'] = jr_buyer_df['highlighted'].fillna(True)
                    jr_buyer_df = jr_buyer_df.rename(columns={'junior_buyer': 'title'})
                    print(jr_buyer_df)
                    if psg_list is not None:
                        print("psg selected")
                        jr_buyer_df = jr_buyer_df.rename(columns={'title': 'junior_buyer'})
                        jr_buyer_list_df = pd.DataFrame(jr_buyer_list, columns={'junior_buyer'})
                        data = {'junior_buyer': jr_buyer_list_df.junior_buyer.unique()}
                        print(data)
                        jr_buyer = pd.DataFrame(data)
                        jr_buyer['selected'] = True
                        jr_buyer['highlighted'] = False
                        print(jr_buyer)
                        jr_buyer_df = pd.merge(jr_buyer_df_heirarhcy, jr_buyer, how='left')
                        jr_buyer_df['selected'] = jr_buyer_df['selected'].fillna(False)
                        jr_buyer_df['highlighted'] = jr_buyer_df['highlighted'].fillna(True)
                        jr_buyer_df = jr_buyer_df[['junior_buyer', 'selected', 'highlighted']]
                        jr_buyer_df = jr_buyer_df.rename(columns={'junior_buyer': 'title'})
                        print(jr_buyer_df)
                else:
                    jr_buyer['selected'] = False
                    jr_buyer['highlighted'] = False
                    jr_buyer_df = pd.merge(jr_buyer_df_heirarhcy, jr_buyer, how='left')
                    jr_buyer_df['selected'] = jr_buyer_df['selected'].fillna(False)
                    jr_buyer_df['highlighted'] = jr_buyer_df['highlighted'].fillna(True)
                    jr_buyer_df = jr_buyer_df.rename(columns={'junior_buyer': 'title'})

                if psg_list is not None:
                    psg['selected'] = True
                    psg['highlighted'] = False
                    if jr_buyer_list is not None:
                        print("junior list is present")
                        kwargs_psg = {
                            'buyer__in': buyer_list,
                            'need_state__in': need_state_list,
                            'junior_buyer__in': jr_buyer_list,
                            'brand_name__in': brand_name_list
                        }
                        kwargs_psg = dict(filter(lambda item: item[1] is not None, kwargs_psg.items()))
                        print(kwargs_psg)
                        heirarchy_check = read_frame(
                            nego_ads_drf.objects.filter(**kwargs_psg))


                    psg_df_check = pd.merge(psg_df_heirarchy,
                                            heirarchy_check[['product_sub_group_description']].drop_duplicates(),
                                            on="product_sub_group_description",
                                            how='right')

                    print("after merge_1...")
                    print(psg_df_check)
                    psg_df_selected = pd.merge(psg_df_check[['product_sub_group_description']], psg,
                                               on="product_sub_group_description", how='left')
                    print("after mergeeee...")
                    psg_df_selected['selected'] = psg_df_selected['selected'].fillna(False)
                    psg_df_selected['highlighted'] = psg_df_selected['highlighted'].fillna(False)
                    print(psg_df_selected)
                    psg_df = pd.merge(psg_df_heirarchy, psg_df_selected, how='left')
                    psg_df['selected'] = psg_df['selected'].fillna(False)
                    psg_df['highlighted'] = psg_df['highlighted'].fillna(True)
                    print(psg_df)
                    psg_df = psg_df.rename(columns={'product_sub_group_description': 'title'})
                    if need_state_list is not None:
                        print("psg selected")
                        psg_df = psg_df.rename(columns={'title': 'product_sub_group_description'})
                        psg_list_df = pd.DataFrame(psg_list, columns={'product_sub_group_description'})
                        data = {'product_sub_group_description': psg_list_df.product_sub_group_description.unique()}
                        print(data)
                        psg = pd.DataFrame(data)
                        psg['selected'] = True
                        psg['highlighted'] = False
                        print(psg)
                        psg_df = pd.merge(psg_df_heirarchy, psg, how='left')
                        psg_df['selected'] = psg_df['selected'].fillna(False)
                        psg_df['highlighted'] = psg_df['highlighted'].fillna(True)
                        psg_df = psg_df[['product_sub_group_description', 'selected', 'highlighted']]
                        psg_df = psg_df.rename(columns={'product_sub_group_description': 'title'})
                        print(psg_df)
                else:
                    psg['selected'] = False
                    psg['highlighted'] = False
                    psg_df = pd.merge(psg_df_heirarchy, psg, how='left')
                    psg_df['selected'] = psg_df['selected'].fillna(False)
                    psg_df['highlighted'] = psg_df['highlighted'].fillna(True)
                    psg_df = psg_df.rename(columns={'product_sub_group_description': 'title'})

                if need_state_list is not None:
                    need_state['selected'] = True
                    need_state['highlighted'] = False
                    # if psg_list is not None:
                    #     print("psg is present")
                    kwargs_need_state = {
                        'buyer__in': buyer_list,
                        'product_sub_group_description__in': psg_list,
                        'junior_buyer__in': jr_buyer_list,
                        'brand_name__in': brand_name_list
                    }
                    kwargs_need_state = dict(filter(lambda item: item[1] is not None, kwargs_need_state.items()))
                    print(kwargs_need_state)
                    heirarchy_check = read_frame(
                        nego_ads_drf.objects.filter(**kwargs_need_state))


                    need_state_df_check = pd.merge(need_state_df_heirarchy,
                                                   heirarchy_check[['need_state']].drop_duplicates(),
                                                   on="need_state",
                                                   how='right')

                    print("after merge_1...")
                    print(need_state_df_check)
                    need_state_df_selected = pd.merge(need_state_df_check[['need_state']], need_state,
                                                      on="need_state", how='left')
                    print("after mergeeee...")
                    need_state_df_selected['selected'] = need_state_df_selected['selected'].fillna(False)
                    need_state_df_selected['highlighted'] = need_state_df_selected['highlighted'].fillna(False)
                    print(need_state_df_selected)
                    need_state_df = pd.merge(need_state_df_heirarchy, need_state_df_selected, how='left')
                    need_state_df['selected'] = need_state_df['selected'].fillna(False)
                    need_state_df['highlighted'] = need_state_df['highlighted'].fillna(True)
                    print(need_state_df)
                    need_state_df = need_state_df.rename(columns={'need_state': 'title'})
                    # if brand_name_list is not None:
                    #     print("need_state selected")
                    #     need_state_df = need_state_df.rename(columns={'title': 'need_state'})
                    #     need_state_list_df = pd.DataFrame(need_state_list, columns={'need_state'})
                    #     data = {'need_state': need_state_list_df.need_state.unique()}
                    #     print(data)
                    #     need_state = pd.DataFrame(data)
                    #     need_state['selected'] = True
                    #     need_state['highlighted'] = False
                    #     print(need_state)
                    #     need_state_df = pd.merge(need_state_df_heirarchy, need_state, how='left')
                    #     need_state_df['selected'] = need_state_df['selected'].fillna(False)
                    #     need_state_df['highlighted'] = need_state_df['highlighted'].fillna(True)
                    #     need_state_df = need_state_df[['need_state', 'selected', 'highlighted']]
                    #     need_state_df = need_state_df.rename(columns={'need_state': 'title'})
                    #     print(need_state_df)
                else:
                    need_state['selected'] = False
                    need_state['highlighted'] = False
                    need_state_df = pd.merge(need_state_df_heirarchy, need_state, how='left')
                    need_state_df['selected'] = need_state_df['selected'].fillna(False)
                    need_state_df['highlighted'] = need_state_df['highlighted'].fillna(True)
                    need_state_df = need_state_df.rename(columns={'need_state': 'title'})

                if brand_name_list is not None:
                    brand_name['selected'] = True
                    brand_name['highlighted'] = False
                    #if need_state_list is not None:
                    #    print("need State is present")
                    kwargs_brand_name = {
                        'buyer__in': buyer_list,
                        'product_sub_group_description__in': psg_list,
                        'junior_buyer__in': jr_buyer_list,
                        'need_state__in': need_state_list
                    }
                    kwargs_brand_name = dict(filter(lambda item: item[1] is not None, kwargs_brand_name.items()))
                    print(kwargs_brand_name)
                    heirarchy_check = read_frame(
                        nego_ads_drf.objects.filter(**kwargs_brand_name))

                    print(heirarchy_check['brand_name'].drop_duplicates())
                    brand_name_df_check = pd.merge(brand_name_df_heirarchy,
                                                   heirarchy_check[['brand_name']].drop_duplicates(),
                                                   on="brand_name",
                                                   how='right')

                    print("after merge_1...")
                    print(brand_name_df_check)
                    brand_name_df_selected = pd.merge(brand_name_df_check[['brand_name']], brand_name,
                                                      on="brand_name", how='left')
                    print("after mergeeee...")
                    brand_name_df_selected['selected'] = brand_name_df_selected['selected'].fillna(False)
                    brand_name_df_selected['highlighted'] = brand_name_df_selected['highlighted'].fillna(False)
                    print(brand_name_df_selected)
                    brand_name_df = pd.merge(brand_name_df_heirarchy, brand_name_df_selected, how='left')
                    brand_name_df['selected'] = brand_name_df['selected'].fillna(False)
                    brand_name_df['highlighted'] = brand_name_df['highlighted'].fillna(True)
                    print(brand_name_df)
                    brand_name_df = brand_name_df.rename(columns={'brand_name': 'title'})
                else:
                    brand_name['selected'] = False
                    brand_name['highlighted'] = False
                    brand_name_df = pd.merge(brand_name_df_heirarchy, brand_name, how='left')
                    brand_name_df['selected'] = brand_name_df['selected'].fillna(False)
                    brand_name_df['highlighted'] = brand_name_df['highlighted'].fillna(True)
                    brand_name_df = brand_name_df.rename(columns={'brand_name': 'title'})

                bc_df = bc_df.sort_values(by='title', ascending=True)
                bc_df['highlighted'] = ~bc_df['highlighted']
                bc_df_sel = bc_df[['selected']]
                bc_df['resource'] = bc_df_sel.to_dict(orient='records')
                del bc_df['selected']
                print(bc_df)
                bc_final = bc_df.to_json(orient='records')
                bc_final = json.loads(bc_final)

                a = {}
                a['id'] = 'buying_controller'
                a['title'] = 'buying_controller'
                a['required'] = True
                a['items']=bc_final
                a['category_director'] = "Beers, Wines and Spirits"

                buyer_df = buyer_df.sort_values(by=['selected','title'], ascending=[False,True])
                buyer_df['highlighted'] = ~buyer_df['highlighted']
                buyer_df_sel = buyer_df[['selected']]
                buyer_df['resource'] = buyer_df_sel.to_dict(orient='records')
                del buyer_df['selected']

                buyer_final = buyer_df.to_json(orient='records')
                buyer_final = json.loads(buyer_final)

                b = {}
                b['id'] = 'buyer'
                b['title'] = 'buyer'
                b['required'] = False
                b['items'] = buyer_final
                b['category_director'] = "Beers, Wines and Spirits"

                jr_buyer_df = jr_buyer_df.sort_values(by=['selected','title'], ascending=[False,True])
                jr_buyer_df['highlighted'] = ~jr_buyer_df['highlighted']
                jr_buyer_df_sel = jr_buyer_df[['selected']]
                jr_buyer_df['resource'] = jr_buyer_df_sel.to_dict(orient='records')
                del jr_buyer_df['selected']

                jr_buyer_final = jr_buyer_df.to_json(orient='records')
                jr_buyer_final = json.loads(jr_buyer_final)

                c = {}
                c['id'] = 'junior_buyer'
                c['title'] = 'junior_buyer'
                c['required'] = False
                c['items'] = jr_buyer_final
                c['category_director'] = "Beers, Wines and Spirits"

                psg_df = psg_df.sort_values(by=['selected','title'], ascending=[False,True])
                psg_df['highlighted'] = ~psg_df['highlighted']
                psg_df_sel = psg_df[['selected']]
                psg_df['resource'] = psg_df_sel.to_dict(orient='records')
                del psg_df['selected']

                psg_final = psg_df.to_json(orient='records')
                psg_final = json.loads(psg_final)

                d = {}
                d['id'] = 'product_sub_group_description'
                d['title'] = 'product_sub_group_description'
                d['required'] = False
                d['items'] = psg_final
                d['category_director'] = "Beers, Wines and Spirits"

                need_state_df = need_state_df.sort_values(by=['selected','title'], ascending=[False,True])
                need_state_df['highlighted'] = ~need_state_df['highlighted']
                need_state_df_sel = need_state_df[['selected']]
                need_state_df['resource'] = need_state_df_sel.to_dict(orient='records')
                del need_state_df['selected']

                need_state_final = need_state_df.to_json(orient='records')
                need_state_final = json.loads(need_state_final)

                e = {}
                e['id'] = 'need_state'
                e['title'] = 'need_state'
                e['required'] = False
                e['items'] = need_state_final
                e['category_director'] = "Beers, Wines and Spirits"

                brand_name_df = brand_name_df.sort_values(by=['selected','title'], ascending=[False,True])
                brand_name_df['highlighted'] = ~brand_name_df['highlighted']
                brand_name_df_sel = brand_name_df[['selected']]
                brand_name_df['resource'] = brand_name_df_sel.to_dict(orient='records')
                del brand_name_df['selected']

                brand_name_final = brand_name_df.to_json(orient='records')
                brand_name_final = json.loads(brand_name_final)

                f = {}
                f['title'] = 'brand_name'
                f['id'] = 'brand_name'
                f['required'] = False
                f['items'] = brand_name_final
                f['category_director'] = "Beers, Wines and Spirits"

                final = []
                final.append(a)
                final.append(b)
                final.append(c)
                final.append(d)
                final.append(e)
                final.append(f)

        return JsonResponse({'cols': cols,'checkbox_list': final}, safe=False)


class product_impact_filters_new(APIView):
    def get(self, request, format=None):
        # input from header
        args = {reqobj + '__in': request.GET.getlist(reqobj) for reqobj in request.GET.keys()}

        designation = args.pop('designation__in', None)
        user_id = args.pop('user_id__in', None)
        session_id = args.pop('session_id__in', None)
        user_name = args.pop('user_name__in', None)
        buying_controller_header = args.pop('buying_controller_header__in', None)
        buyer_header = args.pop('buyer_header__in', None)
        # header over
        cols = ['buying_controller', 'parent_supplier', 'buyer', 'junior_buyer', 'brand_indicator',
                'brand_name', 'product_sub_group_description', 'long_description']

        if 'admin' in designation:
            kwargs_header = {}
        else:
            if buyer_header is None:
                kwargs_header = {
                    'buying_controller__in': buying_controller_header
                }
            else:
                kwargs_header = {
                    'buying_controller__in': buying_controller_header,
                    'buyer__in': buyer_header
                }
        # input from args
        default = args.pop('default__in', None)
        if default is None:
            if not args:

                df = read_frame(product_hierarchy.objects.filter(**kwargs_header).filter(**args))
                heirarchy = read_frame(
                    product_hierarchy.objects.filter(**kwargs_header).values('buying_controller', 'parent_supplier',
                                                                             'buyer',
                                                                             'junior_buyer', 'brand_indicator',
                                                                             'product_sub_group_description',
                                                                             'long_description', 'brand_name'))

                data = {'buying_controller': df.buying_controller.unique()}
                bc = pd.DataFrame(data)
                if len(bc) == 1:
                    bc['selected'] = True  ### One change here for default selection of bc logging in
                    bc['highlighted'] = False
                else:
                    bc['selected'] = False  ### One change here for default selection of bc logging in
                    bc['highlighted'] = False

                data = {'buyer': df.buyer.unique()}
                buyer = pd.DataFrame(data)
                if len(buyer) == 1:
                    buyer['selected'] = True
                    buyer['highlighted'] = False
                else:
                    buyer['selected'] = False
                    buyer['highlighted'] = False

                data = {'parent_supplier': df.parent_supplier.unique()}
                parent_supplier = pd.DataFrame(data)
                parent_supplier['selected'] = False
                parent_supplier['highlighted'] = False

                data = {'junior_buyer': df.junior_buyer.unique()}
                jr_buyer = pd.DataFrame(data)
                jr_buyer['selected'] = False
                jr_buyer['highlighted'] = False

                data = {'brand_indicator': df.brand_indicator.unique()}
                brand_indicator = pd.DataFrame(data)
                brand_indicator['selected'] = False
                brand_indicator['highlighted'] = False

                data = {'product_sub_group_description': df.product_sub_group_description.unique()}
                psg = pd.DataFrame(data)
                psg['selected'] = False
                psg['highlighted'] = False

                data = {'brand_name': df.brand_name.unique()}
                brand_name = pd.DataFrame(data)
                brand_name['selected'] = False
                brand_name['highlighted'] = False

                data = {'long_description': df.long_description.unique()}
                long_description = pd.DataFrame(data)
                long_description['selected'] = False
                long_description['highlighted'] = False

                bc_df = heirarchy[['buying_controller']].drop_duplicates()

                parent_supplier_df = heirarchy[['parent_supplier']].drop_duplicates()

                buyer_df = heirarchy[['buyer']].drop_duplicates()

                jr_buyer_df = heirarchy[['junior_buyer']].drop_duplicates()

                brand_indicator_df = heirarchy[['brand_indicator']].drop_duplicates()

                psg_df = heirarchy[['product_sub_group_description']].drop_duplicates()

                brand_name_df = heirarchy[['brand_name']].drop_duplicates()

                long_description_df = heirarchy[['long_description']].drop_duplicates()

                bc_df = pd.merge(bc_df, bc, how='left')
                bc_df['selected'] = bc_df['selected'].fillna(False)
                bc_df['highlighted'] = bc_df['highlighted'].fillna(False)
                bc_df = bc_df.rename(columns={'buying_controller': 'title'})

                parent_supplier_df = pd.merge(parent_supplier_df, parent_supplier, how='left')
                parent_supplier_df['selected'] = parent_supplier_df['selected'].fillna(False)
                parent_supplier_df['highlighted'] = parent_supplier_df['highlighted'].fillna(False)
                parent_supplier_df = parent_supplier_df.rename(columns={'parent_supplier': 'title'})

                buyer_df = pd.merge(buyer_df, buyer, how='left')
                buyer_df['selected'] = buyer_df['selected'].fillna(False)
                buyer_df['highlighted'] = buyer_df['highlighted'].fillna(False)
                buyer_df = buyer_df.rename(columns={'buyer': 'title'})

                jr_buyer_df = pd.merge(jr_buyer_df, jr_buyer, how='left')
                jr_buyer_df['selected'] = jr_buyer_df['selected'].fillna(False)
                jr_buyer_df['highlighted'] = jr_buyer_df['highlighted'].fillna(False)
                jr_buyer_df = jr_buyer_df.rename(columns={'junior_buyer': 'title'})

                brand_indicator_df = pd.merge(brand_indicator_df, brand_indicator, how='left')
                brand_indicator_df['selected'] = brand_indicator_df['selected'].fillna(False)
                brand_indicator_df['highlighted'] = brand_indicator_df['highlighted'].fillna(False)
                brand_indicator_df = brand_indicator_df.rename(columns={'brand_indicator': 'title'})

                psg_df = pd.merge(psg_df, psg, how='left')
                psg_df['selected'] = psg_df['selected'].fillna(False)
                psg_df['highlighted'] = psg_df['highlighted'].fillna(False)
                psg_df = psg_df.rename(columns={'product_sub_group_description': 'title'})

                brand_name_df = pd.merge(brand_name_df, brand_name, how='left')
                brand_name_df['selected'] = brand_name_df['selected'].fillna(False)
                brand_name_df['highlighted'] = brand_name_df['highlighted'].fillna(False)
                brand_name_df = brand_name_df.rename(columns={'brand_name': 'title'})

                long_description_df = pd.merge(long_description_df, long_description, how='left')
                long_description_df['selected'] = long_description_df['selected'].fillna(False)
                long_description_df['highlighted'] = long_description_df['highlighted'].fillna(False)
                long_description_df = long_description_df.rename(columns={'long_description': 'title'})

                bc_df = bc_df.sort_values(by='title', ascending=True)
                bc_df['highlighted'] = ~bc_df['highlighted']
                bc_df_sel = bc_df[['selected']]
                bc_df['resource'] = bc_df_sel.to_dict(orient='records')
                del bc_df['selected']
                bc_final = bc_df.to_json(orient='records')
                bc_final = json.loads(bc_final)

                a = {}
                a['id'] = 'buying_controller'
                a['title'] = 'buying_controller'
                a['required'] = True
                a['items'] = bc_final
                a['category_director'] = "Beers, Wines and Spirits"

                parent_supplier_df['name_supplier'] = parent_supplier_df['title'].str.split('-').str[1]
                parent_supplier_df = parent_supplier_df.sort_values(by=['selected','name_supplier'], ascending=[False,True])
                del parent_supplier_df['name_supplier']
                parent_supplier_df['highlighted'] = ~parent_supplier_df['highlighted']
                parent_supplier_df_sel = parent_supplier_df[['selected']]
                parent_supplier_df['resource'] = parent_supplier_df_sel.to_dict(orient='records')
                del parent_supplier_df_sel['selected']
                parent_supplier_final = parent_supplier_df.to_json(orient='records')
                parent_supplier_final = json.loads(parent_supplier_final)

                b = {}
                b['id'] = 'parent_supplier'
                b['title'] = 'parent_supplier'
                b['items'] = parent_supplier_final
                b['required'] = False
                b['category_director'] = "Beers, Wines and Spirits"

                buyer_df = buyer_df.sort_values(by='title', ascending=True)
                buyer_df['highlighted'] = ~buyer_df['highlighted']
                buyer_df_sel = buyer_df[['selected']]
                buyer_df['resource'] = buyer_df_sel.to_dict(orient='records')
                buyer_final = buyer_df.to_json(orient='records')
                buyer_final = json.loads(buyer_final)

                c = {}
                c['id'] = 'buyer'
                c['title'] = 'buyer'
                c['items'] = buyer_final
                c['required'] = False
                c['category_director'] = "Beers, Wines and Spirits"

                jr_buyer_df = jr_buyer_df.sort_values(by='title', ascending=True)
                jr_buyer_df['highlighted'] = ~jr_buyer_df['highlighted']
                jr_buyer_df_sel = jr_buyer_df[['selected']]
                jr_buyer_df['resource'] = jr_buyer_df_sel.to_dict(orient='records')
                jr_buyer_final = jr_buyer_df.to_json(orient='records')
                jr_buyer_final = json.loads(jr_buyer_final)

                d = {}
                d['id'] = 'junior_buyer'
                d['title'] = 'junior_buyer'
                d['items'] = jr_buyer_final
                d['required'] = False
                d['category_director'] = "Beers, Wines and Spirits"

                brand_indicator_df = brand_indicator_df.sort_values(by='title', ascending=True)
                brand_indicator_df['highlighted'] = ~brand_indicator_df['highlighted']
                brand_indicator_df_sel = brand_indicator_df[['selected']]
                brand_indicator_df['resource'] = brand_indicator_df_sel.to_dict(orient='records')
                brand_indicator_final = brand_indicator_df.to_json(orient='records')
                brand_indicator_final = json.loads(brand_indicator_final)

                e = {}
                e['id'] = 'brand_indicator'
                e['title'] = 'brand_indicator'
                e['required'] = False
                e['items'] = brand_indicator_final
                e['category_director'] = "Beers, Wines and Spirits"

                brand_name_df = brand_name_df.sort_values(by='title', ascending=True)
                brand_name_df['highlighted'] = ~brand_name_df['highlighted']
                brand_name_df_sel = brand_name_df[['selected']]
                brand_name_df['resource'] = brand_name_df_sel.to_dict(orient='records')
                brand_name_final = brand_name_df.to_json(orient='records')
                brand_name_final = json.loads(brand_name_final)

                f = {}
                f['id'] = 'brand_name'
                f['title'] = 'brand_name'
                f['required'] = False
                f['items'] = brand_name_final
                f['category_director'] = "Beers, Wines and Spirits"

                psg_df = psg_df.sort_values(by='title', ascending=True)
                psg_df['highlighted'] = ~psg_df['highlighted']
                psg_df_sel = psg_df[['selected']]
                psg_df['resource'] = psg_df_sel.to_dict(orient='records')
                psg_final = psg_df.to_json(orient='records')
                psg_final = json.loads(psg_final)

                g = {}
                g['id'] = 'product_sub_group_description'
                g['title'] = 'product_sub_group_description'
                g['required'] = False
                g['items'] = psg_final
                g['category_director'] = "Beers, Wines and Spirits"

                long_description_df = long_description_df.sort_values(by='title', ascending=True)
                long_description_df['highlighted'] = ~long_description_df['highlighted']
                long_description_df_sel = long_description_df[['selected']]
                long_description_df['resource'] = long_description_df_sel.to_dict(orient='records')
                long_description_final = long_description_df.to_json(orient='records')
                long_description_final = json.loads(long_description_final)

                h = {}
                h['id'] = 'long_description'
                h['title'] = 'long_description'
                h['required'] = True
                h['items'] = long_description_final
                h['category_director'] = "Beers, Wines and Spirits"

                final = []
                final.append(a)
                final.append(b)
                final.append(c)
                final.append(d)
                final.append(e)
                final.append(f)
                final.append(g)
                final.append(h)
            else:

                if 'admin' in designation:
                    heirarchy = read_frame(
                        product_hierarchy.objects.values('buying_controller','parent_supplier', 'buyer',
                                                                                           'junior_buyer',
                                                                                           'product_sub_group_description',
                                                                                           'brand_indicator',
                                                                                           'brand_name',
                                                                                           'long_description','base_product_number'))
                else:
                    heirarchy = read_frame(
                        product_hierarchy.objects.filter(**kwargs_header).values('buying_controller',
                                                                                           'parent_supplier', 'buyer',
                                                                                           'junior_buyer',
                                                                                           'product_sub_group_description',
                                                                                           'brand_indicator',
                                                                                           'brand_name',
                                                                                           'long_description','base_product_number'))
                bc_df = heirarchy[['buying_controller']].drop_duplicates()
                parent_supplier_df_heirarchy = heirarchy[['parent_supplier']].drop_duplicates()
                buyer_df_heirarchy = heirarchy[['buyer']].drop_duplicates()
                jr_buyer_df_heirarhcy = heirarchy[['junior_buyer']].drop_duplicates()
                psg_df_heirarchy = heirarchy[['product_sub_group_description']].drop_duplicates()
                brand_indicator_df_heirarchy = heirarchy[['brand_indicator']].drop_duplicates()
                brand_name_df_heirarchy = heirarchy[['brand_name']].drop_duplicates()
                long_description_df_heirarchy = heirarchy[['long_description']].drop_duplicates()

                args_list = {reqobj + '__in': request.GET.getlist(reqobj) for reqobj in request.GET.keys()}
                if buying_controller_header is not None:
                    bc_list = args_list.pop('buying_controller_header__in', None)
                else:
                    bc_list = args_list.pop('buying_controller__in', None)

                parent_supplier_list = args_list.pop('parent_supplier__in', None)
                # if buyer_header is not None:
                #     buyer_list = args_list.pop('buyer_header__in', None)
                # else:
                #     buyer_list = args_list.pop('buyer__in', None)

                buyer_list = args_list.pop('buyer__in', None)

                jr_buyer_list = args_list.pop('junior_buyer__in', None)
                psg_list = args_list.pop('product_sub_group_description__in', None)
                brand_name_list = args_list.pop('brand_name__in', None)
                brand_indicator_list = args_list.pop('brand_indicator__in', None)
                long_description_list = args_list.pop('base_product_number__in', None)


                print(args)
                df = read_frame(product_hierarchy.objects.filter(**args))
                #print(df)
                data = {'buying_controller': df.buying_controller.unique()}
                bc = pd.DataFrame(data)
                #print(bc)

                data = {'parent_supplier': df.parent_supplier.unique()}
                parent_supplier = pd.DataFrame(data)

                data = {'buyer': df.buyer.unique()}
                buyer = pd.DataFrame(data)
                # print(buyer)

                data = {'junior_buyer': df.junior_buyer.unique()}
                jr_buyer = pd.DataFrame(data)
                # print(jr_buyer)

                data = {'product_sub_group_description': df.product_sub_group_description.unique()}
                psg = pd.DataFrame(data)

                data = {'brand_indicator': df.brand_indicator.unique()}
                brand_indicator = pd.DataFrame(data)

                data = {'brand_name': df.brand_name.unique()}
                brand_name = pd.DataFrame(data)

                data = {'long_description': df.long_description.unique()}
                long_description = pd.DataFrame(data)

                bc['selected'] = True
                bc['highlighted'] = False
                bc_df = pd.merge(bc_df, bc, how='left')
                print(bc_df.columns)
                bc_df['selected'] = bc_df['selected'].fillna(False)
                bc_df['highlighted'] = bc_df['highlighted'].fillna(True)

                bc_df = bc_df.rename(columns={'buying_controller': 'title'})

                heirarchy_check = read_frame(
                    product_hierarchy.objects.filter(buying_controller__in=bc_list).values('buying_controller',
                                                                                           'parent_supplier', 'buyer',
                                                                                           'junior_buyer',
                                                                                           'product_sub_group_description',
                                                                                           'brand_indicator',
                                                                                           'brand_name',
                                                                                           'long_description','base_product_number'))

                if parent_supplier_list is not None:
                    #print("inside buyerrr..")
                    parent_supplier['selected'] = True
                    parent_supplier['highlighted'] = False
                    kwargs_parent_sup = {
                        'buyer__in': buyer_list,
                        'junior_buyer__in': jr_buyer_list,
                        'brand_name__in':brand_name_list,
                        'product_sub_group_description__in':psg_list,
                        'base_product_number__in':long_description_list
                    }
                    kwargs_parent_sup = dict(filter(lambda item: item[1] is not None, kwargs_parent_sup.items()))
                    print(kwargs_parent_sup)
                    heirarchy_check = read_frame(
                        product_hierarchy.objects.filter(**kwargs_parent_sup))
                    parent_supplier_df_check = pd.merge(parent_supplier_df_heirarchy,
                                                        heirarchy_check[['parent_supplier']].drop_duplicates(),
                                                        on="parent_supplier", how='right')
                    print("after merge_1...")
                    print(parent_supplier_df_check)
                    print("printing supplier")
                    print(parent_supplier)
                    parent_supplier_df_selected = pd.merge(parent_supplier_df_check[['parent_supplier']],
                                                           parent_supplier, on="parent_supplier", how='left')
                    # print("after mergeeee...")
                    parent_supplier_df_selected['selected'] = parent_supplier_df_selected['selected'].fillna(False)
                    parent_supplier_df_selected['highlighted'] = parent_supplier_df_selected['highlighted'].fillna(
                        False)
                    # print(parent_supplier_df_selected)

                    parent_supplier_df = pd.merge(parent_supplier_df_heirarchy, parent_supplier_df_selected,
                                                  on="parent_supplier", how='left')
                    parent_supplier_df['selected'] = parent_supplier_df['selected'].fillna(False)
                    parent_supplier_df['highlighted'] = parent_supplier_df['highlighted'].fillna(True)
                    # print(parent_supplier_df)
                    parent_supplier_df = parent_supplier_df.rename(columns={'parent_supplier': 'title'})

                    # if buyer_header is not None:
                    #     pass
                    # else:
                    #     if buyer_list is not None:
                    #         #print("inside buyer")
                    #         parent_supplier_df = parent_supplier_df.rename(columns={'title': 'parent_supplier'})
                    #         parent_supplier_list_df = pd.DataFrame(parent_supplier_list, columns={'parent_supplier'})
                    #         data = {'parent_supplier': parent_supplier_list_df.parent_supplier.unique()}
                    #         # print(data)
                    #         parent_supplier = pd.DataFrame(data)
                    #         parent_supplier['selected'] = True
                    #         parent_supplier['highlighted'] = False
                    #         # print(parent_supplier)
                    #         parent_supplier_df = pd.merge(parent_supplier_df_heirarchy, parent_supplier, how='left')
                    #         parent_supplier_df['selected'] = parent_supplier_df['selected'].fillna(False)
                    #         parent_supplier_df['highlighted'] = parent_supplier_df['highlighted'].fillna(True)
                    #         parent_supplier_df = parent_supplier_df[['parent_supplier', 'selected', 'highlighted']]
                    #         parent_supplier_df = parent_supplier_df.rename(columns={'parent_supplier': 'title'})
                    #         #print(parent_supplier_df)
                else:
                    parent_supplier['selected'] = False
                    parent_supplier['highlighted'] = False
                    parent_supplier_df = pd.merge(parent_supplier_df_heirarchy, parent_supplier, how='left')
                    parent_supplier_df['selected'] = parent_supplier_df['selected'].fillna(False)
                    parent_supplier_df['highlighted'] = parent_supplier_df['highlighted'].fillna(True)
                    parent_supplier_df = parent_supplier_df.rename(columns={'parent_supplier': 'title'})

                if buyer_list is not None:
                    print("inside buyerrr..")
                    buyer['selected'] = True
                    buyer['highlighted'] = False
                    if parent_supplier_list is not None:
                        print("buyer list is present")
                        heirarchy_check = read_frame(
                            product_hierarchy.objects.filter(parent_supplier__in=parent_supplier_list))

                    buyer_df_check = pd.merge(buyer_df_heirarchy, heirarchy_check[['buyer']].drop_duplicates(),
                                              on="buyer", how='right')
                    # print("after merge_1...")
                    # print(buyer_df_check)
                    # print("printing buyer")
                    # print(buyer)
                    buyer_df_selected = pd.merge(buyer_df_check[['buyer']], buyer, on="buyer", how='left')
                    # print("after mergeeee...")
                    buyer_df_selected['selected'] = buyer_df_selected['selected'].fillna(False)
                    buyer_df_selected['highlighted'] = buyer_df_selected['highlighted'].fillna(False)
                    # print(buyer_df_selected)

                    buyer_df = pd.merge(buyer_df_heirarchy, buyer_df_selected, on="buyer", how='left')
                    buyer_df['selected'] = buyer_df['selected'].fillna(False)
                    buyer_df['highlighted'] = buyer_df['highlighted'].fillna(True)
                    print(buyer_df)
                    buyer_df = buyer_df.rename(columns={'buyer': 'title'})

                    if jr_buyer_list is not None:
                        print("inside junior buyer")
                        buyer_df = buyer_df.rename(columns={'title': 'buyer'})
                        buyer_list_df = pd.DataFrame(buyer_list, columns={'buyer'})
                        data = {'buyer': buyer_list_df.buyer.unique()}
                        print(data)
                        buyer = pd.DataFrame(data)
                        buyer['selected'] = True
                        buyer['highlighted'] = False
                        print(buyer)
                        buyer_df = pd.merge(buyer_df_heirarchy, buyer, how='left')
                        buyer_df['selected'] = buyer_df['selected'].fillna(False)
                        buyer_df['highlighted'] = buyer_df['highlighted'].fillna(True)
                        buyer_df = buyer_df[['buyer', 'selected', 'highlighted']]
                        buyer_df = buyer_df.rename(columns={'buyer': 'title'})
                        print(buyer_df)
                else:
                    buyer['selected'] = False
                    buyer['highlighted'] = False
                    buyer_df = pd.merge(buyer_df_heirarchy, buyer, how='left')
                    buyer_df['selected'] = buyer_df['selected'].fillna(False)
                    buyer_df['highlighted'] = buyer_df['highlighted'].fillna(True)
                    buyer_df = buyer_df.rename(columns={'buyer': 'title'})

                if jr_buyer_list is not None:
                    jr_buyer['selected'] = True
                    jr_buyer['highlighted'] = False
                    if buyer_list is not None:
                        print("buyer list is present")
                        kwargs_jr_buyer = {
                                'parent_supplier__in': parent_supplier_list,
                                'buyer__in':buyer_list
                            }
                        kwargs_jr_buyer = dict(filter(lambda item: item[1] is not None, kwargs_jr_buyer.items()))
                        print(kwargs_jr_buyer)
                        heirarchy_check = read_frame(
                            product_hierarchy.objects.filter(**kwargs_jr_buyer))


                    print('heirarchy_check')
                    jr_buyer_df_check = pd.merge(jr_buyer_df_heirarhcy,
                                                 heirarchy_check[['junior_buyer']].drop_duplicates(),
                                                 on="junior_buyer",
                                                 how='right')

                    # print("after merge_1...")
                    print(jr_buyer_df_check)
                    print(jr_buyer)
                    jr_buyer_df_selected = pd.merge(jr_buyer_df_check[['junior_buyer']], jr_buyer,
                                                    on="junior_buyer", how='left')
                    print("after mergeeee...")
                    jr_buyer_df_selected['selected'] = jr_buyer_df_selected['selected'].fillna(False)
                    jr_buyer_df_selected['highlighted'] = jr_buyer_df_selected['highlighted'].fillna(False)
                    # print(jr_buyer_df_selected)
                    jr_buyer_df = pd.merge(jr_buyer_df_heirarhcy, jr_buyer_df_selected, how='left')
                    jr_buyer_df['selected'] = jr_buyer_df['selected'].fillna(False)
                    jr_buyer_df['highlighted'] = jr_buyer_df['highlighted'].fillna(True)
                    jr_buyer_df = jr_buyer_df.rename(columns={'junior_buyer': 'title'})
                    # print(jr_buyer_df)
                    if psg_list is not None:
                        print("psg selected")
                        jr_buyer_df = jr_buyer_df.rename(columns={'title': 'junior_buyer'})
                        jr_buyer_list_df = pd.DataFrame(jr_buyer_list, columns={'junior_buyer'})
                        data = {'junior_buyer': jr_buyer_list_df.junior_buyer.unique()}
                        # print(data)
                        jr_buyer = pd.DataFrame(data)
                        jr_buyer['selected'] = True
                        jr_buyer['highlighted'] = False
                        # print(jr_buyer)
                        jr_buyer_df = pd.merge(jr_buyer_df_heirarhcy, jr_buyer, how='left')
                        jr_buyer_df['selected'] = jr_buyer_df['selected'].fillna(False)
                        jr_buyer_df['highlighted'] = jr_buyer_df['highlighted'].fillna(True)
                        jr_buyer_df = jr_buyer_df[['junior_buyer', 'selected', 'highlighted']]
                        jr_buyer_df = jr_buyer_df.rename(columns={'junior_buyer': 'title'})
                        # print(jr_buyer_df)
                else:
                    jr_buyer['selected'] = False
                    jr_buyer['highlighted'] = False
                    jr_buyer_df = pd.merge(jr_buyer_df_heirarhcy, jr_buyer, how='left')
                    jr_buyer_df['selected'] = jr_buyer_df['selected'].fillna(False)
                    jr_buyer_df['highlighted'] = jr_buyer_df['highlighted'].fillna(True)
                    jr_buyer_df = jr_buyer_df.rename(columns={'junior_buyer': 'title'})

                if brand_indicator_list is not None:
                    brand_indicator['selected'] = True
                    brand_indicator['highlighted'] = False
                    if jr_buyer_list is not None:
                        kwargs_brand_indicator = {
                            'parent_supplier__in': parent_supplier_list,
                            'junior_buyer__in': jr_buyer_list,
                            'buyer__in':buyer_list
                        }
                        kwargs_brand_indicator = dict(filter(lambda item: item[1] is not None, kwargs_brand_indicator.items()))
                        print(kwargs_brand_indicator)
                        heirarchy_check = read_frame(
                            product_hierarchy.objects.filter(**kwargs_brand_indicator))

                    brand_indicator_df_check = pd.merge(brand_indicator_df_heirarchy,
                                                        heirarchy_check[['brand_indicator']].drop_duplicates(),
                                                        on="brand_indicator",
                                                        how='right')

                    # print("after merge_1...")
                    # print(brand_indicator_df_check)
                    brand_indicator_df_selected = pd.merge(brand_indicator_df_check[['brand_indicator']],
                                                           brand_indicator,
                                                           on="brand_indicator", how='left')
                    print("after mergeeee...")
                    brand_indicator_df_selected['selected'] = brand_indicator_df_selected['selected'].fillna(False)
                    brand_indicator_df_selected['highlighted'] = brand_indicator_df_selected['highlighted'].fillna(
                        False)
                    # print(brand_indicator_df_selected)
                    brand_indicator_df = pd.merge(brand_indicator_df_heirarchy, brand_indicator_df_selected, how='left')
                    brand_indicator_df['selected'] = brand_indicator_df['selected'].fillna(False)
                    brand_indicator_df['highlighted'] = brand_indicator_df['highlighted'].fillna(True)
                    # print(brand_indicator_df)
                    brand_indicator_df = brand_indicator_df.rename(columns={'brand_indicator': 'title'})
                    if brand_name_list is not None:
                        print("brand name selected")
                        brand_indicator_df = brand_indicator_df.rename(columns={'title': 'brand_indicator'})
                        brand_indicator_list_df = pd.DataFrame(brand_indicator_list, columns={'brand_indicator'})
                        data = {'brand_indicator': brand_indicator_list_df.brand_indicator.unique()}
                        # print(data)
                        brand_indicator = pd.DataFrame(data)
                        brand_indicator['selected'] = True
                        brand_indicator['highlighted'] = False
                        # print(brand_indicator)
                        brand_indicator_df = pd.merge(brand_indicator_df_heirarchy, brand_indicator, how='left')
                        brand_indicator_df['selected'] = brand_indicator_df['selected'].fillna(False)
                        brand_indicator_df['highlighted'] = brand_indicator_df['highlighted'].fillna(True)
                        brand_indicator_df = brand_indicator_df[['brand_indicator', 'selected', 'highlighted']]
                        brand_indicator_df = brand_indicator_df.rename(columns={'brand_indicator': 'title'})
                        # print(brand_indicator_df)
                else:
                    brand_indicator['selected'] = False
                    brand_indicator['highlighted'] = False
                    brand_indicator_df = pd.merge(brand_indicator_df_heirarchy, brand_indicator, how='left')
                    brand_indicator_df['selected'] = brand_indicator_df['selected'].fillna(False)
                    brand_indicator_df['highlighted'] = brand_indicator_df['highlighted'].fillna(True)
                    brand_indicator_df = brand_indicator_df.rename(columns={'brand_indicator': 'title'})

                if brand_name_list is not None:
                    brand_name['selected'] = True
                    brand_name['highlighted'] = False

                    if brand_indicator_list is not None:
                        print("Brand indicator is present")
                        kwargs_brand_name = {
                                    'parent_supplier__in': parent_supplier_list,
                                    'junior_buyer__in': jr_buyer_list,
                                    'brand_indicator__in': brand_indicator_list,
                                    'buyer__in':buyer_list
                                }
                        kwargs_brand_name = dict(filter(lambda item: item[1] is not None, kwargs_brand_name.items()))
                        print(kwargs_brand_name)
                        heirarchy_check = read_frame(
                            product_hierarchy.objects.filter(**kwargs_brand_name))


                    brand_name_df_check = pd.merge(brand_name_df_heirarchy,
                                                   heirarchy_check[['brand_name']].drop_duplicates(),
                                                   on="brand_name",
                                                   how='right')

                    print("after merge_1...")
                    print(brand_name_df_check)
                    brand_name_df_selected = pd.merge(brand_name_df_check[['brand_name']], brand_name,
                                                      on="brand_name", how='left')
                    print("after mergeeee...")
                    brand_name_df_selected['selected'] = brand_name_df_selected['selected'].fillna(False)
                    brand_name_df_selected['highlighted'] = brand_name_df_selected['highlighted'].fillna(False)
                    print(brand_name_df_selected)
                    brand_name_df = pd.merge(brand_name_df_heirarchy, brand_name_df_selected, how='left')
                    brand_name_df['selected'] = brand_name_df['selected'].fillna(False)
                    brand_name_df['highlighted'] = brand_name_df['highlighted'].fillna(True)
                    print(brand_name_df)
                    brand_name_df = brand_name_df.rename(columns={'brand_name': 'title'})
                    if psg_list is not None:
                        print("psg selected")
                        brand_name_df = brand_name_df.rename(columns={'title': 'brand_name'})
                        brand_name_df = pd.DataFrame(brand_name_list, columns={'brand_name'})
                        data = {'brand_name': brand_name_df.brand_name.unique()}
                        # print(data)
                        brand_name = pd.DataFrame(data)
                        brand_name['selected'] = True
                        brand_name['highlighted'] = False
                        # print(brand_name)
                        brand_name_df = pd.merge(brand_name_df_heirarchy, brand_name, how='left')
                        brand_name_df['selected'] = brand_name_df['selected'].fillna(False)
                        brand_name_df['highlighted'] = brand_name_df['highlighted'].fillna(True)
                        brand_name_df = brand_name_df[['brand_name', 'selected', 'highlighted']]
                        brand_name_df = brand_name_df.rename(columns={'brand_name': 'title'})
                        # print(brand_name_df)

                else:
                    brand_name['selected'] = False
                    brand_name['highlighted'] = False
                    brand_name_df = pd.merge(brand_name_df_heirarchy, brand_name, how='left')
                    brand_name_df['selected'] = brand_name_df['selected'].fillna(False)
                    brand_name_df['highlighted'] = brand_name_df['highlighted'].fillna(True)
                    brand_name_df = brand_name_df.rename(columns={'brand_name': 'title'})

                if psg_list is not None:
                    psg['selected'] = True
                    psg['highlighted'] = False
                    if brand_name_list is not None:
                        print("brand name list is present")
                        kwargs_psg = {
                            'parent_supplier__in': parent_supplier_list,
                            'junior_buyer__in': jr_buyer_list,
                            'brand_indicator__in': brand_indicator_list,
                            'brand_name__in':brand_name_list,
                            'buyer__in': buyer_list
                        }
                        kwargs_psg = dict(filter(lambda item: item[1] is not None, kwargs_psg.items()))
                        print(kwargs_psg)
                        heirarchy_check = read_frame(
                            product_hierarchy.objects.filter(**kwargs_psg))

                    psg_df_check = pd.merge(psg_df_heirarchy,
                                            heirarchy_check[['product_sub_group_description']].drop_duplicates(),
                                            on="product_sub_group_description",
                                            how='right')

                    # print("after merge_1...")
                    # print(psg_df_check)
                    psg_df_selected = pd.merge(psg_df_check[['product_sub_group_description']], psg,
                                               on="product_sub_group_description", how='left')
                    print("after mergeeee...")
                    psg_df_selected['selected'] = psg_df_selected['selected'].fillna(False)
                    psg_df_selected['highlighted'] = psg_df_selected['highlighted'].fillna(False)
                    # print(psg_df_selected)
                    psg_df = pd.merge(psg_df_heirarchy, psg_df_selected, how='left')
                    psg_df['selected'] = psg_df['selected'].fillna(False)
                    psg_df['highlighted'] = psg_df['highlighted'].fillna(True)
                    # print(psg_df)
                    psg_df = psg_df.rename(columns={'product_sub_group_description': 'title'})
                    if long_description_list is not None:
                        print("products selected")
                        psg_df = psg_df.rename(columns={'title': 'product_sub_group_description'})
                        psg_list_df = pd.DataFrame(psg_list, columns={'product_sub_group_description'})
                        data = {'product_sub_group_description': psg_list_df.product_sub_group_description.unique()}
                        # print(data)
                        psg = pd.DataFrame(data)
                        psg['selected'] = True
                        psg['highlighted'] = False
                        # print(psg)
                        psg_df = pd.merge(psg_df_heirarchy, psg, how='left')
                        psg_df['selected'] = psg_df['selected'].fillna(False)
                        psg_df['highlighted'] = psg_df['highlighted'].fillna(True)
                        psg_df = psg_df[['product_sub_group_description', 'selected', 'highlighted']]
                        psg_df = psg_df.rename(columns={'product_sub_group_description': 'title'})
                        # print(psg_df)
                else:
                    psg['selected'] = False
                    psg['highlighted'] = False
                    psg_df = pd.merge(psg_df_heirarchy, psg, how='left')
                    psg_df['selected'] = psg_df['selected'].fillna(False)
                    psg_df['highlighted'] = psg_df['highlighted'].fillna(True)
                    psg_df = psg_df.rename(columns={'product_sub_group_description': 'title'})

                if long_description_list is not None:
                    long_description['selected'] = True
                    long_description['highlighted'] = False
                    print(long_description)
                    #if psg_list is not None:
                    print("psg list is present")
                    kwargs_long = {
                        'parent_supplier__in': parent_supplier_list,
                        'junior_buyer__in': jr_buyer_list,
                        'brand_indicator__in': brand_indicator_list,
                        'brand_name__in': brand_name_list,
                        'buyer__in': buyer_list,
                        'product_sub_group_description__in':psg_list
                    }
                    kwargs_long = dict(filter(lambda item: item[1] is not None, kwargs_long.items()))
                    print(kwargs_long)
                    heirarchy_check = read_frame(
                        product_hierarchy.objects.filter(**kwargs_long))

                    print("not present")
                    print(heirarchy_check.columns)
                    long_description_df_check = pd.merge(long_description_df_heirarchy,
                                                         heirarchy_check[['long_description']].drop_duplicates(),
                                                         on="long_description",
                                                         how='right')

                    print("after merge_1...")
                    print(long_description_df_check)
                    long_description_df_selected = pd.merge(long_description_df_check[['long_description']],
                                                            long_description,
                                                            on="long_description", how='left')
                    print("after mergeeee...")
                    long_description_df_selected['selected'] = long_description_df_selected['selected'].fillna(False)
                    long_description_df_selected['highlighted'] = long_description_df_selected['highlighted'].fillna(
                        False)
                    print(long_description_df_selected)
                    long_description_df = pd.merge(long_description_df_heirarchy, long_description_df_selected,
                                                   how='left')
                    long_description_df['selected'] = long_description_df['selected'].fillna(False)
                    long_description_df['highlighted'] = long_description_df['highlighted'].fillna(True)
                    print(long_description_df)
                    long_description_df = long_description_df.rename(columns={'long_description': 'title'})
                else:
                    long_description['selected'] = False
                    long_description['highlighted'] = False
                    long_description_df = pd.merge(long_description_df_heirarchy, long_description, how='left')
                    long_description_df['selected'] = long_description_df['selected'].fillna(False)
                    long_description_df['highlighted'] = long_description_df['highlighted'].fillna(True)
                    long_description_df = long_description_df.rename(columns={'long_description': 'title'})

                bc_df = bc_df.sort_values(by='selected', ascending=True)
                bc_df['highlighted'] = ~bc_df['highlighted']
                bc_df_sel = bc_df[['selected']]
                bc_df['resource'] = bc_df.to_dict(orient='records')
                bc_final = bc_df.to_json(orient='records')
                bc_final = json.loads(bc_final)

                a = {}
                a['id'] = 'buying_controller'
                a['title'] = 'buying_controller'
                a['required'] = True
                a['items'] = bc_final
                a['category_director'] = "Beers, Wines and Spirits"


                parent_supplier_df['name_supplier'] = parent_supplier_df['title'].str.split('-').str[1]
                parent_supplier_df = parent_supplier_df.sort_values(by=['selected','name_supplier'], ascending=[False,True])
                del parent_supplier_df['name_supplier']
                parent_supplier_df['highlighted'] = ~parent_supplier_df['highlighted']
                parent_supplier_df_sel = parent_supplier_df[['selected']]
                parent_supplier_df['resource'] = parent_supplier_df_sel.to_dict(orient='records')
                del parent_supplier_df_sel['selected']

                parent_supplier_final = parent_supplier_df.to_json(orient='records')
                parent_supplier_final = json.loads(parent_supplier_final)

                b = {}
                b['id'] = 'parent_supplier'
                b['title'] = 'parent_supplier'
                b['items'] = parent_supplier_final
                b['required'] = False
                b['category_director'] = "Beers, Wines and Spirits"

                buyer_df = buyer_df.sort_values(by=['selected','title'], ascending=[False,True])
                buyer_df['highlighted'] = ~buyer_df['highlighted']
                buyer_df_sel = buyer_df[['selected']]
                buyer_df['resource'] = buyer_df_sel.to_dict(orient='records')
                buyer_final = buyer_df.to_json(orient='records')
                buyer_final = json.loads(buyer_final)

                c = {}
                c['id'] = 'buyer'
                c['title'] = 'buyer'
                c['items'] = buyer_final
                c['required'] = False
                c['category_director'] = "Beers, Wines and Spirits"

                jr_buyer_df = jr_buyer_df.sort_values(by=['selected','title'], ascending=[False,True])
                jr_buyer_df['highlighted'] = ~jr_buyer_df['highlighted']
                jr_buyer_df_sel = jr_buyer_df[['selected']]
                jr_buyer_df['resource'] = jr_buyer_df_sel.to_dict(orient='records')
                jr_buyer_final = jr_buyer_df.to_json(orient='records')
                jr_buyer_final = json.loads(jr_buyer_final)

                d = {}
                d['id'] = 'junior_buyer'
                d['title'] = 'junior_buyer'
                d['items'] = jr_buyer_final
                d['required'] = False
                d['category_director'] = "Beers, Wines and Spirits"

                brand_indicator_df = brand_indicator_df.sort_values(by=['selected','title'], ascending=[False,True])
                brand_indicator_df['highlighted'] = ~brand_indicator_df['highlighted']
                brand_indicator_df_sel = brand_indicator_df[['selected']]
                brand_indicator_df['resource'] = brand_indicator_df_sel.to_dict(orient='records')
                brand_indicator_final = brand_indicator_df.to_json(orient='records')
                brand_indicator_final = json.loads(brand_indicator_final)

                e = {}
                e['id'] = 'brand_indicator'
                e['title'] = 'brand_indicator'
                e['required'] = False
                e['items'] = brand_indicator_final
                e['category_director'] = "Beers, Wines and Spirits"

                brand_name_df = brand_name_df.sort_values(by=['selected','title'], ascending=[False,True])
                brand_name_df['highlighted'] = ~brand_name_df['highlighted']
                brand_name_df_sel = brand_name_df[['selected']]
                brand_name_df['resource'] = brand_name_df_sel.to_dict(orient='records')
                brand_name_final = brand_name_df.to_json(orient='records')
                brand_name_final = json.loads(brand_name_final)

                f = {}
                f['id'] = 'brand_name'
                f['title'] = 'brand_name'
                f['required'] = False
                f['items'] = brand_name_final
                f['category_director'] = "Beers, Wines and Spirits"

                psg_df = psg_df.sort_values(by=['selected','title'], ascending=[False,True])
                psg_df['highlighted'] = ~psg_df['highlighted']
                psg_df_sel = psg_df[['selected']]
                psg_df['resource'] = psg_df_sel.to_dict(orient='records')
                psg_final = psg_df.to_json(orient='records')
                psg_final = json.loads(psg_final)

                g = {}
                g['id'] = 'product_sub_group_description'
                g['title'] = 'product_sub_group_description'
                g['required'] = False
                g['items'] = psg_final
                g['category_director'] = "Beers, Wines and Spirits"

                long_description_df = long_description_df.sort_values(by=['selected','title'], ascending=[False,True])
                long_description_df['highlighted'] = ~long_description_df['highlighted']
                long_description_df_sel = long_description_df[['selected']]
                long_description_df['resource'] = long_description_df_sel.to_dict(orient='records')
                long_description_final = long_description_df.to_json(orient='records')
                long_description_final = json.loads(long_description_final)

                h = {}
                h['id'] = 'long_description'
                h['title'] = 'long_description'
                h['required'] = True
                h['items'] = long_description_final
                h['category_director'] = "Beers, Wines and Spirits"

                final = []
                final.append(a)
                final.append(b)
                final.append(c)
                final.append(d)
                final.append(e)
                final.append(f)
                final.append(g)
                final.append(h)

        return JsonResponse({'cols': cols,'checkbox_list': final}, safe=False)
