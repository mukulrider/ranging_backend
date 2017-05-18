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



#models for product impact and negotiation filters
from .models import product_hierarchy,nego_ads_drf


from django.core.paginator import Paginator 
import numpy as np
import gzip
import xgboost as xgb
import pickle
#for cache 
from rest_framework_extensions.cache.decorators import cache_response



#### Negotiation View Filters
def col_distinct(kwargs, col_name,kwargs_header):
    queryset = nego_ads_drf.objects.filter(**kwargs_header).filter(**kwargs).values(col_name).order_by(col_name).distinct()
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
        user_id = sent_req.pop('user_id',None)
        designation = sent_req.pop('designation',None)
        session_id = sent_req.pop('session_id',None)
        user_name = sent_req.pop('user_name', None)
        buying_controller_header = sent_req.pop('buying_controller_header', None)
        buyer_header = sent_req.pop('buyer_header',None)

        if 'admin' in designation:
            kwargs_header = {}
        else:
            if buyer_header is None:
                kwargs_header = {
                    'buying_controller__in' : buying_controller_header
                }
            else:
                kwargs_header = {
                    'buying_controller__in' : buying_controller_header,
                    'buyer__in' : buyer_header
                }


        cols =['buying_controller', 'buyer','junior_buyer','product_sub_group_description','need_state','brand_name']

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


            category_of_sent_obj_list = col_distinct(kwargs2, col_name,kwargs_header)

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
def col_distinct_product(kwargs, col_name,kwargs_header):
    queryset = product_hierarchy.objects.filter(**kwargs_header).filter(**kwargs).values(col_name).order_by(col_name).distinct()
    base_product_number_list = [k.get(col_name) for k in queryset]
    return base_product_number_list


class product_impact_filters(APIView):
    def get(self, request):

        obj = {}
        get_keys = request.GET.keys()
        for i in get_keys:
            obj[i] = request.GET.getlist(i)

        sent_req = obj
        user_id = sent_req.pop('user_id',None)
        designation = sent_req.pop('designation',None)
        session_id = sent_req.pop('session_id',None)
        user_name = sent_req.pop('user_name', None)
        buying_controller_header = sent_req.pop('buying_controller_header',None)
        buyer_header = sent_req.pop('buyer_header',None)


        if 'admin' in designation:
            kwargs_header = {}
        else:
            if buyer_header is None:
                kwargs_header = {
                    'buying_controller__iexact' : buying_controller_header
                }
            else:
                kwargs_header = {
                    'buying_controller__iexact' : buying_controller_header,
                    'buyer__iexact' : buyer_header
                }

        cols =['buying_controller', 'parent_supplier', 'buyer', 'junior_buyer', 'brand_indicator',
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
            col_unique_list = col_distinct_product({}, col_name,kwargs_header)
            col_unique_list_name.append({'name': col_name,
                                         'unique_elements': col_unique_list})
            col_unique_list_name_obj[col_name] = col_unique_list
            # args sent as url params
            kwargs2 = {reqobj + '__in': sent_req.get(reqobj) for reqobj in sent_req.keys()}

            category_of_sent_obj_list = col_distinct_product(kwargs2, col_name,kwargs_header)

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

      






