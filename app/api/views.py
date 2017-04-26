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
#models for npd 1st half and negotiation
from .models import outperformance, pricebucket, unmatchedprod, nego_ads_drf
#models for product impact
from .models import product_hierarchy, pps_ros_quantile, shelf_review_subs, prod_similarity_subs, product_price, cts_data , supplier_share, product_contri, product_desc
#models for npd 2nd half 
from .models import bc_allprod_attributes,attribute_score_allbc,consolidated_calculated_cannibalization,npd_supplier_ads,features_allbc,consolidated_buckets,seasonality_index,uk_holidays,npd_calendar,merch_range,input_npd,brand_grp_mapping,range_space_store_future,store_details
from .serializers import unmatchedprodSerializer, negochartsSerializer, npd_impact_tableSerializer
from django.core.paginator import Paginator 
import numpy as np
import gzip
import xgboost as xgb
import pickle


## for NPD Opportunity View Filters
class npdpage_filterdata_new(APIView):
    def get(self, request, format=None):
        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        default = args.pop('default__iexact',None)
        if default is None:
            if not args:
                print("inside default")

                df = read_frame(pricebucket.objects.filter(**args))
                heirarchy = read_frame(pricebucket.objects.all().values('buying_controller','buyer','junior_buyer','product_sub_group_description'))

                data ={'buying_controller' : df.buying_controller.unique()}
                bc = pd.DataFrame(data)
                bc['selected']=False
                bc['disabled'] =False


                data ={'buyer' : df.buyer.unique()}
                buyer = pd.DataFrame(data)
                buyer['selected']=False
                buyer['disabled'] =False


                data ={'junior_buyer' : df.junior_buyer.unique()}
                jr_buyer = pd.DataFrame(data)
                jr_buyer['selected']=False
                jr_buyer['disabled'] =False


                data ={'product_sub_group_description' : df.product_sub_group_description.unique()}
                psg = pd.DataFrame(data)
                psg['selected']=False
                psg['disabled'] =False

                bc_df = heirarchy[['buying_controller']].drop_duplicates()

                buyer_df = heirarchy[['buyer']].drop_duplicates()

                jr_buyer_df = heirarchy[['junior_buyer']].drop_duplicates()

                psg_df = heirarchy[['product_sub_group_description']].drop_duplicates()


                bc_df = pd.merge(bc_df,bc,how='left')
                bc_df['selected'] =bc_df['selected'].fillna(False)
                bc_df['disabled'] =bc_df['disabled'].fillna(False)

                bc_df = bc_df.rename(columns={'buying_controller': 'name'})


                buyer_df = pd.merge(buyer_df,buyer,how='left')
                buyer_df['selected'] =buyer_df['selected'].fillna(False)
                buyer_df['disabled'] =buyer_df['disabled'].fillna(False)
                buyer_df = buyer_df.rename(columns={'buyer': 'name'})


                jr_buyer_df = pd.merge(jr_buyer_df,jr_buyer,how='left')
                jr_buyer_df['selected'] =jr_buyer_df['selected'].fillna(False)
                jr_buyer_df['disabled'] =jr_buyer_df['disabled'].fillna(False)
                jr_buyer_df = jr_buyer_df.rename(columns={'junior_buyer': 'name'})

                psg_df = pd.merge(psg_df,psg,how='left')
                psg_df['selected'] =psg_df['selected'].fillna(False)
                psg_df['disabled'] =psg_df['disabled'].fillna(False)
                psg_df = psg_df.rename(columns={'product_sub_group_description': 'name'})



                bc_df = bc_df.sort_values(by='name',ascending=True)

                bc_final = bc_df.to_json(orient='records')
                bc_final = json.loads(bc_final)


                a = {}
                a['name']='buying_controller'
                a['items']=bc_final

                buyer_df = buyer_df.sort_values(by='name',ascending=True)

                buyer_final = buyer_df.to_json(orient='records')
                buyer_final = json.loads(buyer_final)

                b = {}
                b['name']='buyer'
                b['items']=buyer_final



                jr_buyer_df = jr_buyer_df.sort_values(by='name',ascending=True)
                jr_buyer_final = jr_buyer_df.to_json(orient='records')
                jr_buyer_final = json.loads(jr_buyer_final)

                c = {}
                c['name']='junior_buyer'
                c['items']=jr_buyer_final


                psg_df = psg_df.sort_values(by='name',ascending=True)
                psg_final = psg_df.to_json(orient='records')
                psg_final = json.loads(psg_final)

                
                d = {}
                d['name']='product_sub_group_description'
                d['items']=psg_final

                final = []
                final.append(a)
                final.append(b)
                final.append(c)
                final.append(d)
            else:
                
                heirarchy = read_frame(pricebucket.objects.all().values('buying_controller','buyer','junior_buyer','product_sub_group_description'))


                bc_df = heirarchy[['buying_controller']].drop_duplicates()
                buyer_df = heirarchy[['buyer']].drop_duplicates()
                jr_buyer_df = heirarchy[['junior_buyer']].drop_duplicates()
                psg_df = heirarchy[['product_sub_group_description']].drop_duplicates()
            
                df = read_frame(pricebucket.objects.filter(**args))

                print("BC ")
                data ={'buying_controller' : df.buying_controller.unique()}
                bc = pd.DataFrame(data)
                print(len(bc))
                
                print("Buyer ")
                data ={'buyer' : df.buyer.unique()}
                buyer = pd.DataFrame(data)
                print(len(buyer))

                print("Jr Buyer ")
                data ={'junior_buyer' : df.junior_buyer.unique()}
                jr_buyer = pd.DataFrame(data)
                print(len(jr_buyer))


                print("PSG ")
                data ={'product_sub_group_description' : df.product_sub_group_description.unique()}
                psg = pd.DataFrame(data)
                print(len(psg))

                bc['selected']=True
                bc['disabled']=False
                bc_df = pd.merge(bc_df,bc,how='left')
                bc_df['selected'] =bc_df['selected'].fillna(False)
                bc_df['disabled'] =bc_df['disabled'].fillna(True)
                print(bc_df)
                bc_df = bc_df.rename(columns={'buying_controller': 'name'})

                if len(buyer)==1:
                    buyer['selected']=True
                    buyer['disabled']=False
                    buyer_df = pd.merge(buyer_df,buyer,how='left')
                    buyer_df['selected'] =buyer_df['selected'].fillna(False)
                    buyer_df['disabled'] =buyer_df['disabled'].fillna(True)
                    buyer_df = buyer_df.rename(columns={'buyer': 'name'})
                else:
                    buyer['selected']=False
                    buyer['disabled']=False
                    buyer_df = pd.merge(buyer_df,buyer,how='left')
                    buyer_df['selected'] =buyer_df['selected'].fillna(False)
                    buyer_df['disabled'] =buyer_df['disabled'].fillna(True)
                    buyer_df = buyer_df.rename(columns={'buyer': 'name'})


                if len(jr_buyer)==1:
                    jr_buyer['selected']=True
                    jr_buyer['disabled']=False
                    jr_buyer_df = pd.merge(jr_buyer_df,jr_buyer,how='left')
                    jr_buyer_df['selected'] =jr_buyer_df['selected'].fillna(False)
                    jr_buyer_df['disabled'] =jr_buyer_df['disabled'].fillna(True)
                    jr_buyer_df = jr_buyer_df.rename(columns={'junior_buyer': 'name'})
                else:
                    jr_buyer['selected']=False
                    jr_buyer['disabled']=False
                    jr_buyer_df = pd.merge(jr_buyer_df,jr_buyer,how='left')
                    jr_buyer_df['selected'] =jr_buyer_df['selected'].fillna(False)
                    jr_buyer_df['disabled'] =jr_buyer_df['disabled'].fillna(True)
                    jr_buyer_df = jr_buyer_df.rename(columns={'junior_buyer': 'name'})

                if len(psg)==1:
                    psg['selected']=True
                    psg['disabled']=False
                    psg_df = pd.merge(psg_df,psg,how='left')
                    psg_df['selected'] =psg_df['selected'].fillna(False)
                    psg_df['disabled'] =psg_df['disabled'].fillna(True)
                    psg_df = psg_df.rename(columns={'product_sub_group_description': 'name'})
                else:
                    psg['selected']=False
                    psg['disabled']=False
                    psg_df = pd.merge(psg_df,psg,how='left')
                    psg_df['selected'] =psg_df['selected'].fillna(False)
                    psg_df['disabled'] =psg_df['disabled'].fillna(True)
                    psg_df = psg_df.rename(columns={'product_sub_group_description': 'name'})



                bc_df = bc_df.sort_values(by='name',ascending=True)
                bc_final = bc_df.to_json(orient='records')
                bc_final = json.loads(bc_final)

                a = {}
                a['name']='buying_controller'
                a['items']=bc_final


                buyer_df = buyer_df.sort_values(by='name',ascending=True)
                buyer_final = buyer_df.to_json(orient='records')
                buyer_final = json.loads(buyer_final)

                b = {}
                b['name']='buyer'
                b['items']=buyer_final

                jr_buyer_df = jr_buyer_df.sort_values(by='name',ascending=True)
                jr_buyer_final = jr_buyer_df.to_json(orient='records')
                jr_buyer_final = json.loads(jr_buyer_final)

                c = {}
                c['name']='junior_buyer'
                c['items']=jr_buyer_final

                psg_df = psg_df.sort_values(by='name',ascending=True)
                psg_final = psg_df.to_json(orient='records')
                psg_final = json.loads(psg_final)

                
                d = {}
                d['name']='product_sub_group_description'
                d['items']=psg_final

                final = []
                final.append(a)
                final.append(b)
                final.append(c)
                final.append(d)

        
        return JsonResponse(final, safe=False)


## for NPD Opportunity View

# for outperformance - chart
class npdpage_outperformance(APIView):
    def get(self, request, *args):

        kwargs = {
            'buying_controller': 'Meat Fish and Veg',
            'junior_buyer': 'Coated Fish'
        }

        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        print(args)
        args.pop('page__iexact', None)
        print("getting session details ")
        x=request.session
        print(x)
        args.pop('product_sub_group_description__iexact',None)
        print(args)

        week={}
        week["week_flag__iexact"]=args.pop('week_flag__iexact',None)
        print("calculating week")
        print(week)
        if week["week_flag__iexact"]==None:
            week = {
                'week_flag__iexact': 'Latest 13 Weeks'
            }
        elif week["week_flag__iexact"]=='Latest 26 Weeks': 
            week = {
                'week_flag__iexact': 'Latest 13 Weeks'
            }
            
        print(week)
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

# for pricebucket - chart
class npdpage_pricebucket(APIView):
    def get(self, request, *args):

        kwargs = {
            'buying_controller': 'Meat Fish and Veg',
            'junior_buyer': 'Coated Fish',
            'product_sub_group_description': 'FRZ SCAMPI'
        }

        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        print(args)
        args.pop('page__iexact', None)

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

        print(df_required)
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

# for psgskudistribution - chart
class npdpage_psgskudistribution(APIView):
    def get(self, request, *args):

        kwargs = {
            'buying_controller': 'Meat Fish and Veg',
            'junior_buyer': 'Coated Fish'
        }

        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        print(args)
        args.pop('page__iexact', None)
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

# for unmatchedprod - table
class npdpage_unmatchedprod(generic.TemplateView):
    def get(self, request, *args):

        kwargs = {
            'buying_controller': 'Meat Fish and Veg',
            'junior_buyer': 'Coated Fish',
            'product_sub_group_description': 'FRZ SCAMPI'
        }
        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        print(args)

        week={}
        week["week_flag__iexact"]=args.pop('week_flag__iexact',None)
        if week["week_flag__iexact"]==None:
                        week = {
                                        'week_flag__iexact': 'Latest 13 Weeks'
                        }

        #### To include pagination feature
        page = 1
        try:
                        page = int(args.get('page__iexact'))
        except:
                        page = 1

        args.pop('page__iexact', None)
        # start_row = int(args.pop('startRow__iexact', 0))
        # end_row = start_row + 8  ### Taking 8 elements per page

        #### To include search feature. Applicable for only retailer
        search = args.pop('search__iexact', '')
        print(args)
        if not args:
                        queryset = unmatchedprod.objects.filter(**kwargs).filter(**week).filter(retailer__icontains=search)#[start_row:end_row]
        else:
                        queryset = unmatchedprod.objects.filter(**args).filter(**week).filter(retailer__icontains=search)#[start_row:end_row]

        p = Paginator(queryset, 5)

        serializer_class = unmatchedprodSerializer(p.page(page), many=True)
        # return JsonResponse(serializer_class.data, safe=False)
        return JsonResponse({'pagination_count': p.num_pages,'page': page,'start_index': p.page(page).start_index(),'count': p.count,'end_index': p.page(page).end_index(),'table': serializer_class.data}, safe=False)


#### Negotiation Filters
def col_distinct(kwargs, col_name):
    queryset = nego_ads_drf.objects.filter(**kwargs).values(col_name).order_by(col_name).distinct()
    base_product_number_list = [k.get(col_name) for k in queryset]
    return base_product_number_list



def make_json(sent_req):
    print('*********************\n       FILTERS2 \n*********************')
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

    # print('lowest_key:', lowest_key, '|', 'lowest', lowest)

    final_list = []  # final list to send

    col_unique_list_name = []  # rename
    col_unique_list_name_obj = {}  # rename
    for col_name in cols:
        print('\n********* \n' + col_name + '\n*********')
        # print('sent_req.get(col_name):', sent_req.get(col_name))
        col_unique_list = col_distinct({}, col_name)
        col_unique_list_name.append({'name': col_name,
                                     'unique_elements': col_unique_list})
        col_unique_list_name_obj[col_name] = col_unique_list
        # args sent as url params
        kwargs2 = {reqobj + '__in': sent_req.get(reqobj) for reqobj in sent_req.keys()}

        category_of_sent_obj_list = col_distinct(kwargs2, col_name)
        print(len(category_of_sent_obj_list))
        sent_obj_category_list = []

        # get unique elements for `col_name`
        for i in category_of_sent_obj_list:
            sent_obj_category_list.append(i)

        def highlight_check(category_unique):
            # print(title)
            if len(sent_req.keys()) > 0:
                highlighted = False
                if col_name in sent_req.keys():
                    if col_name == cols[lowest]:
                        queryset = nego_ads_drf.objects.filter(**{col_name: category_unique})[:1].get()
                        # x = getattr(queryset, cols[lowest])
                        y = getattr(queryset, cols[second_lowest])
                        # print(x, '|', y, '|', cols[lowest], '|',
                        #       'Category_second_last:' + cols[second_lowest],
                        #       '|', col_name,
                        #       '|', category_unique)
                        for i in sent_req.keys():
                            print('keys:', i, sent_req.get(i))
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

class filters_nego(APIView):
    def get(self, request):
        print(request.GET)
        obj = {}
        get_keys = request.GET.keys()
        for i in get_keys:
            # print(request.GET.getlist(i))
            obj[i] = request.GET.getlist(i)
        # print(obj)
        return make_json(obj)

####Negotiation

#for bubble chart
class nego_bubble_chart(APIView):
    def get(self, request, *args):


        kwargs = {
            'store_type': 'Main Estate'
        }

        args = {reqobj + '__in': request.GET.getlist(reqobj) for reqobj in request.GET.keys()}
        print(args)
        args.pop('page__in',None)
        w=args.pop('time_period__in',None)
        week={}
        if not w:
            week = {'time_period__iexact': 'Last 13 Weeks'}
        else:
            week['time_period__iexact'] = w[0]
        print(week)
        print(args)

        if not args:
            df = read_frame(nego_ads_drf.objects.filter(**week).filter(**kwargs).values('base_product_number','long_description', 'rate_of_sale','cps_quartile','pps_quartile','brand_indicator'))

        else:
            df = read_frame(nego_ads_drf.objects.filter(**args).filter(**week).values('base_product_number','long_description', 'rate_of_sale','cps_quartile','pps_quartile','brand_indicator'))


        df["rate_of_sale"] = df["rate_of_sale"].astype('float')
        df["cps_quartile"] = df["cps_quartile"].astype('float')
        df["pps_quartile"] = df["pps_quartile"].astype('float')
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
                "brand_ind" : df["brand_indicator"][i]

            }
            final_bubble.append(bubble_list)
        return JsonResponse(final_bubble, safe=False)

#for table 
class nego_bubble_table(APIView):
    def get(self, request, *args):

        kwargs = {
            'store_type': 'Main Estate'
        }
        args = {reqobj + '__in': request.GET.getlist(reqobj) for reqobj in request.GET.keys()}
        print(args)
        ## for week tab
        week={}
        w=args.pop('time_period__in',None)
        if not w:
            week = {'time_period__iexact': 'Last 13 Weeks'}
        else:
            week['time_period__iexact'] = w[0]
        print(week)

        #### To include pagination feature
        page = 1
        try:
            page = int(args.get('page__in')[0])
            print(page)
                        
        except:
            page = 1
        print(page)
        args.pop('page__in', None)

        #### To include search feature. Applicable for only long desc
        s = args.pop('search__in',[''])
        search = s[0]
        print(search)

        if not args :
            queryset = nego_ads_drf.objects.filter(**week).filter(**kwargs).filter(long_description__icontains=search)

        else:
            queryset = nego_ads_drf.objects.filter(**args).filter(**week).filter(long_description__icontains=search)
        p = Paginator(queryset, 8)
        print(len(queryset))
        serializer_class = negochartsSerializer(p.page(page), many=True)
        return JsonResponse({'pagination_count': p.num_pages,'page': page, 'start_index': p.page(page).start_index(),'count': p.count,'end_index': p.page(page).end_index(),'table': serializer_class.data}, safe=False)


## NPD Second Half 
## NPD IMPACT FILTERS
class npdimpactpage_filterdata(APIView):
    def get(self, request, format=None):


        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}

        bc_name = args.get('buying_controller__iexact')
        buyer_name = args.get('buyer__iexact')
        jr_buyer_name = args.get('junior_buyer__iexact')
        psg_name = args.get('product_sub_group_description__iexact')
        brand_id = args.get('brand_name__iexact')
        # print(brand_id)
        measure_id = args.get('measure_type__iexact')
        # print(measure_id)
        till_roll_id= args.get('till_roll_description__iexact')
        package_id = args.get('package_type__iexact')
        merch_name = args.get('merchandise_group_code_description__iexact')
        range_name = args.get('range_space_break_code__iexact')
        supplier_name = args.get('parent_supplier__iexact')


        kwargs = {
                    'buying_controller__iexact': bc_name,
                    'buyer__iexact' : buyer_name,
                    'junior_buyer__iexact': jr_buyer_name,
                    'product_sub_group_description__iexact': psg_name,
                    }

        kwargs = dict(filter(lambda item: item[1] is not None, kwargs.items()))

        kwargs_brand = {
                                                                        'buying_controller__iexact': bc_name,
                                                                        'brand_name__iexact' : brand_id,

                                                                        }

        kwargs_brand = dict(filter(lambda item: item[1] is not None, kwargs_brand.items()))

        kwargs_measure = {
                                                                        'buying_controller__iexact': bc_name,
                                                                        'measure_type__iexact' : measure_id,

                                                                        }

        kwargs_measure = dict(filter(lambda item: item[1] is not None, kwargs_measure.items()))

        kwargs_till_roll = {
                                                                        'buying_controller__iexact': bc_name,
                                                                        'till_roll_description__iexact' : till_roll_id,

                                                                        }

        kwargs_till_roll = dict(filter(lambda item: item[1] is not None, kwargs_till_roll.items()))
        print(kwargs_till_roll)

        kwargs_package = {
                                                                        'buying_controller__iexact': bc_name,
                                                                        'package_type__iexact' : package_id,
                                                                        }


        kwargs_package = dict(filter(lambda item: item[1] is not None, kwargs_package.items()))

        kwargs_temp = {
                                                        'buying_controller__iexact': bc_name,
                                                        'merchandise_group_code_description__iexact': merch_name,
                                                        'range_space_break_code__iexact': range_name,
                                                        }
        kwargs_temp = dict(filter(lambda item: item[1] is not None, kwargs_temp.items()))


        kwargs_supplier = {
                                                        'buying_controller__iexact': bc_name,
                                                        'parent_supplier__iexact': supplier_name,
                                        
                                                        }
        kwargs_supplier = dict(filter(lambda item: item[1] is not None, kwargs_supplier.items()))                            






        if not args:

                        heirarchy = read_frame(input_npd.objects.filter(**kwargs).values('buying_controller','buyer','junior_buyer','product_sub_group_description','brand_name','package_type',
                                        'till_roll_description','measure_type'))
                        # merch_range_df = read_frame(merch_range.objects.all().filter(**kwargs_temp))

                        # supplier_df = read_frame(npd_supplier_ads.objects.all().filter(**kwargs_supplier))

                        #merch_range = pd.read_csv('merch_range.csv'


                        bc_df = heirarchy[['buying_controller']].drop_duplicates()
                        buyer_df = heirarchy[['buyer']].drop_duplicates()
                        jr_buyer_df = heirarchy[['junior_buyer']].drop_duplicates()
                        psg_df = heirarchy[['product_sub_group_description']].drop_duplicates()
                        # brand_df = heirarchy[['brand_name']].drop_duplicates()
                        # package_df = heirarchy[['package_type']].drop_duplicates()
                        # till_roll_df = heirarchy[['till_roll_description']].drop_duplicates()
                        # measure_df = heirarchy[['measure_type']].drop_duplicates()
                        # merch_grp_df = merch_range_df[['merchandise_group_code_description']].drop_duplicates()
                        # print(merch_grp_df)
                        # range_class_df = merch_range_df[['range_space_break_code']].drop_duplicates()

                        # supplier_df = supplier_df[['parent_supplier']].drop_duplicates()



                        bc_df['selected'] =False
                        bc_df['disabled'] =False
                        print(bc_df)
                        bc_df = bc_df.rename(columns={'buying_controller': 'name'})

                        buyer_df['selected'] =False
                        buyer_df['disabled'] =False
                        buyer_df = buyer_df.rename(columns={'buyer': 'name'})



                        jr_buyer_df['selected'] =False
                        jr_buyer_df['disabled'] =False
                        jr_buyer_df = jr_buyer_df.rename(columns={'junior_buyer': 'name'})


                        psg_df['selected'] =False
                        psg_df['disabled'] =False
                        psg_df = psg_df.rename(columns={'product_sub_group_description': 'name'})



                        # supplier_df['selected'] =False
                        # supplier_df['disabled'] =False
                        # supplier_df = supplier_df.rename(columns={'parent_supplier': 'name'})





                        # brand_df['selected'] =False
                        # brand_df['disabled'] =True
                        # brand_df = brand_df.rename(columns={'brand_name': 'name'})



                        # package_df['selected'] =False
                        # package_df['disabled'] =True
                        # package_df = package_df.rename(columns={'package_type': 'name'})

                        # measure_df['selected'] =False
                        # measure_df['disabled'] =True
                        # measure_df = measure_df.rename(columns={'measure_type': 'name'})


                        # till_roll_df['selected'] = False
                        # till_roll_df['disabled'] = True
                        # till_roll_df = till_roll_df.rename(columns={'till_roll_description': 'name'})



                        # merch_grp_df['selected'] = False
                        # merch_grp_df['disabled'] = True
                        # merch_grp_df = merch_grp_df.rename(columns={'merchandise_group_code_description': 'name'})



                        # range_class_df['selected']  = False
                        # range_class_df['disabled'] = True
                        # range_class_df = range_class_df.rename(columns={'range_space_break_code': 'name'})



                        bc_df = bc_df.sort_values(by='name',ascending=True)
                        bc_final = bc_df.to_json(orient='records')
                        bc_final = json.loads(bc_final)


                        a = {}
                        a['name']='buying_controller'
                        a['items']=bc_final

                        buyer_df = buyer_df.sort_values(by='name',ascending=True)
                        buyer_final = buyer_df.to_json(orient='records')
                        buyer_final = json.loads(buyer_final)

                        b = {}
                        b['name']='buyer'
                        b['items']=buyer_final

                        jr_buyer_df = jr_buyer_df.sort_values(by='name',ascending=True)
                        jr_buyer_final = jr_buyer_df.to_json(orient='records')
                        jr_buyer_final = json.loads(jr_buyer_final)

                        c = {}
                        c['name']='junior_buyer'
                        c['items']=jr_buyer_final

                        psg_df = psg_df.sort_values(by='name',ascending=True)
                        psg_final = psg_df.to_json(orient='records')
                        psg_final = json.loads(psg_final)

                        
                        d = {}
                        d['name']='product_sub_group_description'
                        d['items']=psg_final

                        # supplier_final = supplier_df.to_json(orient='records')
                        # supplier_final = json.loads(supplier_final)

                        
                        # # e = {}
                        # # e['name']='parent_supplier'
                        # # e['items']=supplier_final



                        # # brand_final = brand_df.to_json(orient='records')
                        # # brand_final = json.loads(brand_final)
                        


                        # # f = {}
                        # # f['name']='brand_name'
                        # # f['items']=brand_final



                        # # package_final = package_df.to_json(orient='records')
                        # # package_final = json.loads(package_final)
                        


                        # # g = {}
                        # g['name']='package_type'
                        # g['items']=package_final

                        # measure_final = measure_df.to_json(orient='records')
                        # measure_final = json.loads(measure_final)

                        
                        # h = {}
                        # h['name']='measure_type'
                        # h['items']=measure_final


                        # tillroll_final = till_roll_df.to_json(orient='records')
                        # tillroll_final = json.loads(tillroll_final)

                        
                        # i = {}
                        # i['name']='till_roll_description'
                        # i['items']=tillroll_final



                        # merch_final = merch_grp_df.to_json(orient='records')
                        # merch_final = json.loads(merch_final)

                        
                        # j = {}
                        # j['name']='merchandise_group_code_description'
                        # j['items']=merch_final



                        # range_class_final = range_class_df.to_json(orient='records')
                        # range_class_final = json.loads(range_class_final)

                        
                        # k = {}
                        # k['name']='range_space_break_code'
                        # k['items']=range_class_final


                        final_ph = []
                        final_ph.append(a)
                        final_ph.append(b)
                        final_ph.append(c)
                        final_ph.append(d)
                        # final_pi = []
                        # final_pi.append(e)
                        # final_pi.append(f)
                        # final_pi.append(g)
                        # final_pi.append(h)
                        # final_pi.append(i)
                        # final_pi.append(j)
                        # final_pi.append(k)
                        final = {}
                        final["product_hierarchy"] = final_ph
                        #final["product_information"] = final_pi                               
        else:

                        df = read_frame(input_npd.objects.filter(**kwargs))

                        hh = read_frame(input_npd.objects.filter(buying_controller__in=df.buying_controller.unique()).values('buying_controller','buyer','junior_buyer','product_sub_group_description','brand_name','package_type',
                                        'till_roll_description','measure_type'))

                        # heirarchy = read_frame(input_npd.objects.filter(buying_controller__in=df.buying_controller.unique()).values('buying_controller','buyer','junior_buyer','product_sub_group_description','brand_name','package_type',
                        #             'till_roll_description','measure_type'))
                        merch_range_df = read_frame(merch_range.objects.filter(buying_controller__in=df.buying_controller.unique()))

                        supplier_df = read_frame(npd_supplier_ads.objects.filter(buying_controller__in=df.buying_controller.unique()))
                        #print(df.buying_controller.unique())
                        print(len(supplier_df))
                        #merch_range = pd.read_csv('merch_range.csv'


                        bc_df = hh[['buying_controller']].drop_duplicates()
                        buyer_df = hh[['buyer']].drop_duplicates()
                        jr_buyer_df = hh[['junior_buyer']].drop_duplicates()
                        psg_df = hh[['product_sub_group_description']].drop_duplicates()
                        brand_df = hh[['brand_name']].drop_duplicates()
                        package_df = hh[['package_type']].drop_duplicates()
                        till_roll_df = hh[['till_roll_description']].drop_duplicates()
                        measure_df = hh[['measure_type']].drop_duplicates()
                        merch_grp_df = merch_range_df[['merchandise_group_code_description']].drop_duplicates()
                        range_class_df = merch_range_df[['range_space_break_code']].drop_duplicates()
                        supplier_df = supplier_df[['parent_supplier']].drop_duplicates()
        
                        
                        df_temp = read_frame(input_npd.objects.filter(buying_controller__in=df.buying_controller.unique()))

                        df_brand_id = read_frame(input_npd.objects.filter(**kwargs_brand).values('brand_name'))
        
                        df_measure_id = read_frame(input_npd.objects.filter(**kwargs_measure).values('measure_type'))
                        
                        df_till_roll_id = read_frame(input_npd.objects.filter(**kwargs_till_roll).values('till_roll_description'))
                        # print(len(till_roll_id))


                        df_package_id = read_frame(input_npd.objects.filter(**kwargs_package).values('package_type'))

                        df_merch_range =read_frame(merch_range.objects.filter(**kwargs_temp).values('buying_controller','merchandise_group_code_description','range_space_break_code'))
                        
                        df_supplier_id = read_frame(npd_supplier_ads.objects.filter(**kwargs_supplier).values('parent_supplier'))


                        print("######################")
                        print(len(df_merch_range))

                        print("BC ")
                        data ={'buying_controller' : df.buying_controller.unique()}
                        bc = pd.DataFrame(data)
                        print(len(bc))
                        
                        print("Buyer ")
                        data ={'buyer' : df.buyer.unique()}
                        buyer = pd.DataFrame(data)
                        print(len(buyer))

                        print("Jr Buyer ")
                        data ={'junior_buyer' : df.junior_buyer.unique()}
                        jr_buyer = pd.DataFrame(data)
                        print(len(jr_buyer))


                        print("PSG ")
                        data ={'product_sub_group_description' : df.product_sub_group_description.unique()}
                        psg = pd.DataFrame(data)
                        print(len(psg))

                        print("Supplier ")
                        data ={'parent_supplier' : df_supplier_id.parent_supplier.unique()}
                        supplier = pd.DataFrame(data)
                        print(len(supplier))




                        print("Brand NaME ")
                        data ={'brand_name' : df_brand_id.brand_name.unique()}
                        brand = pd.DataFrame(data)
                        print(len(brand))


                        print("package type ")
                        data ={'package_type' : df_package_id.package_type.unique()}
                        package = pd.DataFrame(data)
                        print(len(package))


                        print("measure type ")
                        data ={'measure_type' : df_measure_id.measure_type.unique()}
                        measure = pd.DataFrame(data)
                        print(len(measure))


                        print("till roll ")
                        data ={'till_roll_description' : df_till_roll_id.till_roll_description.unique()}
                        till_roll = pd.DataFrame(data)
                        print(len(till_roll))



                        print("merch_grp ")
                        data ={'merchandise_group_code_description' : df_merch_range.merchandise_group_code_description.unique()}
                        merch_grp = pd.DataFrame(data)
                        print(len(merch_grp))



                        print("range_class")
                        data ={'range_space_break_code' : df_merch_range.range_space_break_code.unique()}
                        range_class = pd.DataFrame(data)
                        print(len(range_class))








                        bc['selected']=True
                        bc['disabled']=False
                        bc_df = pd.merge(bc_df,bc,how='left')
                        bc_df['selected'] =bc_df['selected'].fillna(False)
                        bc_df['disabled'] =bc_df['disabled'].fillna(True)
                        print(bc_df)
                        bc_df = bc_df.rename(columns={'buying_controller': 'name'})

                        if len(buyer)==1:
                                        buyer['selected']=True
                                        buyer['disabled']=False
                                        buyer_df = pd.merge(buyer_df,buyer,how='left')
                                        buyer_df['selected'] =buyer_df['selected'].fillna(False)
                                        buyer_df['disabled'] =buyer_df['disabled'].fillna(True)
                                        buyer_df = buyer_df.rename(columns={'buyer': 'name'})
                        else:
                                        buyer['selected']=False
                                        buyer['disabled']=False
                                        buyer_df = pd.merge(buyer_df,buyer,how='left')
                                        buyer_df['selected'] =buyer_df['selected'].fillna(False)
                                        buyer_df['disabled'] =buyer_df['disabled'].fillna(True)
                                        buyer_df = buyer_df.rename(columns={'buyer': 'name'})


                        if len(jr_buyer)==1:
                                        jr_buyer['selected']=True
                                        jr_buyer['disabled']=False
                                        jr_buyer_df = pd.merge(jr_buyer_df,jr_buyer,how='left')
                                        jr_buyer_df['selected'] =jr_buyer_df['selected'].fillna(False)
                                        jr_buyer_df['disabled'] =jr_buyer_df['disabled'].fillna(True)
                                        jr_buyer_df = jr_buyer_df.rename(columns={'junior_buyer': 'name'})
                        else:
                                        jr_buyer['selected']=False
                                        jr_buyer['disabled']=False
                                        jr_buyer_df = pd.merge(jr_buyer_df,jr_buyer,how='left')
                                        jr_buyer_df['selected'] =jr_buyer_df['selected'].fillna(False)
                                        jr_buyer_df['disabled'] =jr_buyer_df['disabled'].fillna(True)
                                        jr_buyer_df = jr_buyer_df.rename(columns={'junior_buyer': 'name'})



                        if len(psg)==1:
                                        psg['selected']=True
                                        psg['disabled']=False
                                        psg_df = pd.merge(psg_df,psg,how='left')
                                        psg_df['selected'] =psg_df['selected'].fillna(False)
                                        psg_df['disabled'] =psg_df['disabled'].fillna(True)
                                        psg_df = psg_df.rename(columns={'product_sub_group_description': 'name'})
                        else:
                                        psg['selected']=False
                                        psg['disabled']=False
                                        psg_df = pd.merge(psg_df,psg,how='left')
                                        psg_df['selected'] =psg_df['selected'].fillna(False)
                                        psg_df['disabled'] =psg_df['disabled'].fillna(True)
                                        psg_df = psg_df.rename(columns={'product_sub_group_description': 'name'})



                        if len(supplier)==1:
                                        supplier['selected']=True
                                        supplier['disabled']=False
                                        supplier_df = pd.merge(supplier_df,supplier,how='left')
                                        supplier_df['selected'] =supplier_df['selected'].fillna(False)
                                        supplier_df['disabled'] =supplier_df['disabled'].fillna(True)
                                        supplier_df = supplier_df.rename(columns={'parent_supplier': 'name'})
                        else:
                                        supplier['selected']=False
                                        supplier['disabled']=False
                                        supplier_df = pd.merge(supplier_df,supplier,how='left')
                                        supplier_df['selected'] =supplier_df['selected'].fillna(False)
                                        supplier_df['disabled'] =supplier_df['disabled'].fillna(True)
                                        supplier_df = supplier_df.rename(columns={'parent_supplier': 'name'})








                        if len(brand)==1:
                                        brand['selected']=True
                                        brand['disabled']=False
                                        brand_df = pd.merge(brand_df,brand,how='left')
                                        brand_df['selected'] =brand_df['selected'].fillna(False)
                                        brand_df['disabled'] =brand_df['disabled'].fillna(True)
                                        brand_df = brand_df.rename(columns={'brand_name': 'name'})
                        else:
                                        brand['selected']=False
                                        brand['disabled']=False
                                        brand_df = pd.merge(brand_df,brand,how='left')
                                        brand_df['selected'] =brand_df['selected'].fillna(False)
                                        brand_df['disabled'] =brand_df['disabled'].fillna(True)
                                        brand_df = brand_df.rename(columns={'brand_name': 'name'})

                        if len(package)==1:
                                        package['selected']=True
                                        package['disabled']=False
                                        package_df = pd.merge(package_df,package,how='left')
                                        package_df['selected'] =package_df['selected'].fillna(False)
                                        package_df['disabled'] =package_df['disabled'].fillna(True)
                                        package_df = package_df.rename(columns={'package_type': 'name'})
                        else:
                                        package['selected']=False
                                        package['disabled']=False
                                        package_df = pd.merge(package_df,package,how='left')
                                        package_df['selected'] =package_df['selected'].fillna(False)
                                        package_df['disabled'] =package_df['disabled'].fillna(True)
                                        package_df = package_df.rename(columns={'package_type': 'name'})



                        if len(measure)==1:
                                        print('yes')
                                        measure['selected']=True
                                        measure['disabled']=False
                                        measure_df = pd.merge(measure_df,measure,how='left')
                                        measure_df['selected'] =measure_df['selected'].fillna(False)
                                        measure_df['disabled'] =measure_df['disabled'].fillna(True)
                                        measure_df = measure_df.rename(columns={'measure_type': 'name'})
                        else:
                                        measure['selected']=False
                                        measure['disabled']=False
                                        measure_df = pd.merge(measure_df,measure,how='left')
                                        measure_df['selected'] =measure_df['selected'].fillna(False)
                                        measure_df['disabled'] =measure_df['disabled'].fillna(True)
                                        measure_df = measure_df.rename(columns={'measure_type': 'name'})


                        if len(till_roll)==1:
                                        till_roll['selected']=True
                                        till_roll['disabled']=False
                                        till_roll_df = pd.merge(till_roll_df,till_roll,how='left')
                                        till_roll_df['selected'] =till_roll_df['selected'].fillna(False)
                                        till_roll_df['disabled'] =till_roll_df['disabled'].fillna(True)
                                        till_roll_df = till_roll_df.rename(columns={'till_roll_description': 'name'})
                        else:
                                        till_roll['selected']=False
                                        till_roll['disabled']=False
                                        till_roll_df = pd.merge(till_roll_df,till_roll,how='left')
                                        till_roll_df['selected'] =till_roll_df['selected'].fillna(False)
                                        till_roll_df['disabled'] =till_roll_df['disabled'].fillna(True)
                                        till_roll_df = till_roll_df.rename(columns={'till_roll_description': 'name'})


                        if len(merch_grp)==1:
                                        merch_grp['selected']=True
                                        merch_grp['disabled']=False
                                        merch_grp_df = pd.merge(merch_grp_df,merch_grp,how='left')
                                        merch_grp_df['selected'] =merch_grp_df['selected'].fillna(False)
                                        merch_grp_df['disabled'] =merch_grp_df['disabled'].fillna(True)
                                        merch_grp_df = merch_grp_df.rename(columns={'merchandise_group_code_description': 'name'})
                        else:
                                        merch_grp['selected']=False
                                        merch_grp['disabled']=False
                                        merch_grp_df = pd.merge(merch_grp_df,merch_grp,how='left')
                                        merch_grp_df['selected'] =merch_grp_df['selected'].fillna(False)
                                        merch_grp_df['disabled'] =merch_grp_df['disabled'].fillna(True)
                                        merch_grp_df = merch_grp_df.rename(columns={'merchandise_group_code_description': 'name'})



                        if len(range_class)==1:
                                        range_class['selected']=True
                                        range_class['disabled']=False
                                        range_class_df = pd.merge(range_class_df,range_class,how='left')
                                        range_class_df['selected'] =range_class_df['selected'].fillna(False)
                                        range_class_df['disabled'] =range_class_df['disabled'].fillna(True)
                                        range_class_df = range_class_df.rename(columns={'range_space_break_code': 'name'})
                        else:
                                        range_class['selected']=False
                                        range_class['disabled']=False
                                        range_class_df = pd.merge(range_class_df,range_class,how='left')
                                        range_class_df['selected'] =range_class_df['selected'].fillna(False)
                                        range_class_df['disabled'] =range_class_df['disabled'].fillna(True)
                                        range_class_df = range_class_df.rename(columns={'range_space_break_code': 'name'})




                        bc_df = bc_df.sort_values(by='name',ascending=True)
                        bc_final = bc_df.to_json(orient='records')
                        bc_final = json.loads(bc_final)


                        a = {}
                        a['name']='buying_controller'
                        a['items']=bc_final

                        buyer_df = buyer_df.sort_values(by='name',ascending=True)
                        buyer_final = buyer_df.to_json(orient='records')
                        buyer_final = json.loads(buyer_final)

                        b = {}
                        b['name']='buyer'
                        b['items']=buyer_final


                        jr_buyer_df = jr_buyer_df.sort_values(by='name',ascending=True)
                        jr_buyer_final = jr_buyer_df.to_json(orient='records')
                        jr_buyer_final = json.loads(jr_buyer_final)

                        c = {}
                        c['name']='junior_buyer'
                        c['items']=jr_buyer_final


                        psg_df = psg_df.sort_values(by='name',ascending=True)
                        psg_final = psg_df.to_json(orient='records')
                        psg_final = json.loads(psg_final)

                        
                        d = {}
                        d['name']='product_sub_group_description'
                        d['items']=psg_final



                        supplier_df = supplier_df.sort_values(by='name',ascending=True)
                        supplier_final = supplier_df.to_json(orient='records')
                        supplier_final = json.loads(supplier_final)

                        
                        e = {}
                        e['name']='parent_supplier'
                        e['items']=supplier_final



                        brand_df = brand_df.sort_values(by='name',ascending=True)
                        brand_final = brand_df.to_json(orient='records')
                        brand_final = json.loads(brand_final)
                        


                        f = {}
                        f['name']='brand_name'
                        f['items']=brand_final



                        package_df = package_df.sort_values(by='name',ascending=True)
                        package_final = package_df.to_json(orient='records')
                        package_final = json.loads(package_final)
                        


                        g = {}
                        g['name']='package_type'
                        g['items']=package_final

                        measure_df = measure_df.sort_values(by='name',ascending=True)
                        measure_final = measure_df.to_json(orient='records')
                        measure_final = json.loads(measure_final)

                        
                        h = {}
                        h['name']='measure_type'
                        h['items']=measure_final

                        till_roll_df = till_roll_df.sort_values(by='name',ascending=True)
                        tillroll_final = till_roll_df.to_json(orient='records')
                        tillroll_final = json.loads(tillroll_final)

                        
                        i = {}
                        i['name']='till_roll_description'
                        i['items']=tillroll_final



                        merch_grp_df = merch_grp_df.sort_values(by='name',ascending=True)
                        merch_final = merch_grp_df.to_json(orient='records')
                        merch_final = json.loads(merch_final)

                        
                        j = {}
                        j['name']='merchandise_group_code_description'
                        j['items']=merch_final


                        range_class_df = range_class_df.sort_values(by='name',ascending=True)
                        range_class_final = range_class_df.to_json(orient='records')
                        range_class_final = json.loads(range_class_final)

                        
                        k = {}
                        k['name']='range_space_break_code'
                        k['items']=range_class_final



                        final_ph = []
                        final_ph.append(a)
                        final_ph.append(b)
                        final_ph.append(c)
                        final_ph.append(d)
                        final_pi = []
                        final_pi.append(e)
                        final_pi.append(f)
                        final_pi.append(g)
                        final_pi.append(h)
                        final_pi.append(i)
                        final_pi.append(j)
                        final_pi.append(k)

                        final = {}
                        final["product_hierarchy"] = final_ph
                        final["product_information"] = final_pi
        
        return JsonResponse(final, safe=False)



#NPD IMPACT VIEW
# for bubble chart
class npdpage_impact_bubble_chart(APIView):
    def get(self, request, *args):

        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        # args = {reqobj + '__in': request.GET.getlist(reqobj) for reqobj in request.GET.keys()}
        par_supp = args.get('parent_supplier__iexact')
        bc_name = args.get('buying_controller__iexact')
        args.pop('page__iexact', None)
        week={}
        week["time_period__iexact"]=args.pop('week_flag__iexact',None)
        print("calculating week")
        print(week)
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

        print(args)
        print(week)

        data = read_frame(
            npd_supplier_ads.objects.filter(**kwargs).filter(**week).values('long_description', 'cps_quartile', 'pps_quartile',
                                                           'rate_of_sale','performance_quartile','cps'))
                                                           # ))

        ######Should be taken care in the ads
        data = data[data["rate_of_sale"]>0].reset_index(drop=True)
        # data["rate_of_sale"] = data["rate_of_sale"].astype('float')
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
class npdpage_impact_bubble_table(generic.TemplateView):
    def get(self, request, *args):

        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        print(args)
        par_supp = args.get('parent_supplier__iexact')
        bc_name = args.get('buying_controller__iexact')
        

        ## week tab
        week={}
        week["time_period__iexact"]=args.pop('week_flag__iexact',None)
        print()
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

        #### To include pagination feature
        page = 1
        try:
            page = int(args.get('page1__iexact'))
        except:
            page = 1

        args.pop('page1__iexact', None)
        print(page)

        #### To include search feature. Applicable for only long desc
        search = args.pop('search1__iexact', '')
        print(search)
        print(week)

        kwargs = {
            'buying_controller': bc_name ,
            'parent_supplier': par_supp
                    }
        kwargs = dict(filter(lambda item: item[1] is not None, kwargs.items()))

        queryset = npd_supplier_ads.objects.filter(**kwargs).filter(**week).filter(long_description__icontains=search)

        p = Paginator(queryset, 5)
        serializer_class = npd_impact_tableSerializer(p.page(page), many=True)
        return JsonResponse({'pagination_count': p.num_pages,'page': page,'start_index': p.page(page).start_index(),'count': p.count,'end_index': p.page(page).end_index(),'table': serializer_class.data}, safe=False)

# for forecast and waterfall chart

class npdpage_impact_forecast(APIView):
    def get(self, request, *args):
        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        
        print(args)
        week=args.pop('week_flag__iexact',None)
        week_flag = week
        print("calculating week")
        ## default week tab
        if week==None:
            week = 'Latest 13 Weeks'
    #define variables 
        print("creating varibales for npd impact")
        
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
        Merchandise_Group_Description = args.pop('merchandise_group_code_description__iexact', '')
        Range_class = args.pop('range_class__iexact', '')

        ## for edit forecast
        total_forecasted_volume = float(args.pop('modified_forecast__iexact',0))
        Cannibalization_perc = float(args.pop('Cannibalization_perc__iexact',0)) 
    #variables defined

    #read all files 
        All_attribute = read_frame(bc_allprod_attributes.objects.all())
        #global All_attribute
        attribute_score = read_frame(attribute_score_allbc.objects.all())
        #global attribute_score
        bc_cannibilization = read_frame(consolidated_calculated_cannibalization.objects.all())
        #global bc_cannibilization
        input_dataset = read_frame(input_npd.objects.all())
        #global input_dataset
        dataset = read_frame(features_allbc.objects.all())
        #global dataset
        uk_holidays_df = read_frame(uk_holidays.objects.all())
        #global uk_holidays_df
        consolidated_buckets_df = read_frame(consolidated_buckets.objects.all())
        #global consolidated_buckets_df
        range_space_store = read_frame(range_space_store_future.objects.all())
        #global range_space_store
        store_details_df = read_frame(store_details.objects.all())
        #global store_details_df

        SI = read_frame(seasonality_index.objects.all())
        #global SI
        
        product_contri_df = read_frame(product_contri.objects.all())
        #global product_contri_df
        product_price_df = read_frame(product_price.objects.all())
        #global product_price_df
        week_mapping = read_frame(npd_calendar.objects.all())
        #global week_mapping

        merch_range_df = read_frame(merch_range.objects.all())
        #global merch_range_df

        product_desc_df = read_frame(product_desc.objects.all())
        #global product_desc_df
        brand_grp_mapping_df = read_frame(brand_grp_mapping.objects.all())
        #global brand_grp_mapping_df
    #files read
        search = args.pop('search__iexact', "")
        page = 1
        try:
            page = int(args.get('page__iexact'))
        except:
            page = 1
        args.pop('page__iexact', None)
        start_row = (page-1)*5
        end_row = start_row + 5       

        ### To get the priceband , psg code and merch code

        def get_priceband_psg_merch_code():

            ###Getting a price band based on user selection

            if((Buying_controller == 'Frozen Impulse') | (Buying_controller == 'Meat Fish and Veg') |(Buying_controller == 'Grocery Cereals')):
                
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


            #####Getting a psg code based on psg desc
            psg_code = input_dataset[input_dataset['product_sub_group_description'] == Product_Sub_Group_Description].iloc[0]['product_sub_group_code']

            merch_code = merch_range_df[merch_range_df['merchandise_group_code_description']==Merchandise_Group_Description].iloc[0]['merchandise_group_code']
         
            psg_priceband_merch = {'psg_code' : psg_code, 'price_band' : price_band ,'merch_code' : merch_code}

            return psg_priceband_merch

        #### Making structure with 52 rows 
        def get_ads_structure():

            #Taking Parameters
            #global week_mapping 
            #global current_week 
            #global  dataset1 

            df =dataset1.iloc[0:51,]

            week_mapping_subset = week_mapping.loc[week_mapping.year_week_number>=201631]

            ### To get the list of all 52 weeks
            weeks_list = pd.DataFrame(week_mapping_subset['year_week_number'].unique())
            weeks_list.columns = ['year_week_number']
            weeks_list.sort_values(by= ['year_week_number'],ascending=False)
            weeks_list = weeks_list.iloc[0:52]
            df = pd.concat([df.reset_index(drop=True), weeks_list], axis=1)
            df= df.fillna(0)
            return df
        ###On column added which is the year week number

        #### Have incorporated margin_percent and acp 
        def fill_ads_structure(dataset):
            
            ####Brand to ind and grp mapping
 
            brand_grp_mapping = All_attribute

            ### Adding new feature as abs 
            margin_percent = abs((asp-acp)/acp)

            #### Assigning the value of '1' for the collected inputs (Categorical Variables)
            dataset.loc[:,"buyer_" + Buyer] = 1
            dataset.loc[:,"junior_buyer_" + Junior_Buyer] = 1
            dataset.loc[:,"package_type_" + Package_Type] = 1
            dataset.loc[:,"product_sub_group_description_" + Product_Sub_Group_Description] = 1
            dataset.loc[:,"measure_type_" + measure_type] = 1
            dataset.loc[:,"price_band_" + price_band] = 1

            #### Directly input from the users
            dataset.loc[:,"asp"] = asp
            dataset.loc[:,"acp"] = acp
            dataset.loc[:,"margin_percent"] = margin_percent
            dataset.loc[:,"size"] = Size

            #### Mapping Brand name to brand grp (Getting the rank of the brand selected)

            ##### Reading a mapping file
            brand_group = (brand_grp_mapping_df.loc[brand_grp_mapping_df['brand_name'] == Brand_Name]).iloc[0]['brand_grp20']

            #### Assigning the rank of the group in our ADS
            dataset.loc[:,"brand_grp20"] = brand_group

            ####Getting the brand indicator 
            brand_ind = (brand_grp_mapping_df.loc[brand_grp_mapping_df['brand_name'] == Brand_Name]).iloc[0]['brand_ind']

            # Need to QC this
            if (brand_ind=='T'):
                brand_indicator = 1
            if (brand_ind == 'B'):
                brand_indicator = 0
            dataset.loc[:,"brand_ind"] = brand_indicator
            ### Filling all columns related to week number 
            #### Looping for all the weeks

            dataset.loc[:,"weeks_since_launch"] = 0
            #### Arranging week in ascending order
            week_mapping_subset = week_mapping.sort_values('year_week_number')
            #Converting date to the date format
            week_mapping_subset['calendar_date'] = pd.to_datetime(week_mapping_subset['calendar_date'], format= '%Y-%m-%d')
                    
            date_map = week_mapping_subset[['year_week_number', 'quarter_number', 'period_number', 'week_number',
               'period_week_number']].drop_duplicates()
            date_map.columns = ['year_week_number', 'quarter_number', 'period_number', 'curr_week_number',
               'period_week_number']
            date_sparse = pd.get_dummies(date_map, prefix = ['quarter_number', 'period_number', 'curr_week_number',
               'period_week_number'], columns = ['quarter_number', 'period_number', 'curr_week_number',
               'period_week_number'])
            date_sparse.drop('curr_week_number_53', 1, inplace= True)
            date_sparse.drop('period_number_13', 1, inplace= True)
            print(dataset.columns)
            dataset.drop(date_sparse.columns[1:], 1, inplace =True)
            print(len(dataset.columns))
            dataset = pd.merge(dataset, date_sparse, on = ['year_week_number'], how = 'left')
            
            dataset.drop('weeks_since_launch', 1, inplace =True)
            dataset['weeks_since_launch'] = df.year_week_number.rank(method = 'dense').astype(int)

            #### Weeks since launch will be incremental 
            #Taking weeks from launch as 1 for the first week
                

            #### Reading a mapping file
            SI_psg = SI.loc[(SI['psg'] == psg_code)]
            SI_psg['adjusted_index']=SI_psg['adjusted_index'].astype(float)
            SI_psg = SI_psg[['weeks','adjusted_index']]
            ####Getting a week column to get the seasonality index
            dataset['weeks'] = dataset['year_week_number']%100
            dataset = pd.merge(dataset, SI_psg, left_on=['weeks'], right_on=['weeks'], how='left' )
            del(dataset['si'])

            dataset = dataset.rename(columns={'adjusted_index':'si'})
            del(dataset['weeks'])
            dataset['si'] = dataset['si'].astype(float)

            return dataset

        def similar_products():

            #### Getting number of subs same and different brand
            All_attribute_treated = All_attribute.dropna()
            All_attribute_treated = All_attribute_treated.loc[(All_attribute_treated['product_sub_group_description']== Product_Sub_Group_Description)]

            ###Creating a empty data frame of attributes to compare. Will fill the values based on user selection

            match_df = pd.DataFrame(0, index = [0],  columns = [ "brand_name", "package_type", "till_roll_description", "size", "measure_type", "price_band"] )

            ##### As we need price band we need to convert the asp (user input) into price bands based on BC and asp

            #### Filling the empty data frames with values as now we have the price band

            match_df.loc[:,"brand_name"] = Brand_Name
            match_df.loc[:,"package_type"] = Package_Type
            match_df.loc[:,"till_roll_description"] = Till_Roll_Description
            match_df.loc[:,"size"] = Size
            match_df.loc[:,"measure_type"] = measure_type
            match_df.loc[:,"price_band"] = price_band
            
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
                                                    ((match_all_prod.loc[:,"measure_type_y"] == 'SNGL')  & 
                                                   ((match_all_prod.loc[:,"size_x"] - match_all_prod.loc[:,"size_y"]) <= 2)) , 1, 0)

            match_all_prod['price_flag'] = np.where(match_all_prod.loc[:,"price_band_x"] == match_all_prod.loc[:,"price_band_y"], 1, 0)

            match_all_prod['till_roll_flag'] = np.where(match_all_prod.loc[:,"till_roll_description_x"] == match_all_prod.loc[:,"till_roll_description_y"], 1, 0)

            ### Subsetting score importance based on PSG
            score = attribute_score[attribute_score.loc[:, 'product_sub_group_code'] == psg_code]

            ###Getting the percentage score based on flag*individual score

            match_all_prod['brand_score'] = match_all_prod.loc[:,"brand_flag"]*score['avg_brand'].values
            match_all_prod['package_score'] = match_all_prod.loc[:,"package_flag"]*score['avg_pkg'].values
            match_all_prod['Size_score'] = match_all_prod.loc[:,"Size_flag"]*score['avg_size'].values
            match_all_prod['price_score'] = match_all_prod.loc[:,"price_flag"]*score['avg_price'].values
            match_all_prod['till_roll_score'] = match_all_prod.loc[:,"till_roll_flag"]*score['avg_tillroll'].values

            ####Getting the final score for every row based on summation of individual attribute score
            match_all_prod['final_score'] = match_all_prod['brand_score'] + match_all_prod['package_score'] + match_all_prod['Size_score'] + match_all_prod['price_score'] + match_all_prod['till_roll_score']

            #### Subsetting for score greater than 0.7 (threshold)

            sim_prod = match_all_prod[match_all_prod['final_score'] > 0.7]


            return sim_prod

        def subs_same_different(dataset):
            ###Getting data
            #global df 
            #global sim_prod 
            ####Total products left after putting a threshold of 0.7

            tot_prod = sim_prod.shape[0]

            #### Same brand product count
            same_brand_count = sum(sim_prod['brand_flag'] == 1)

            ### Total - same = different brand
            diff_brand_count = tot_prod - sum(sim_prod['brand_flag'] == 1)

            ##### Assigning values to our columns in ADS (no_of_subs_same_brand , no_of_subs_diff_brand)

            dataset.loc[:, 'no_of_subs_same_brand'] = same_brand_count
            dataset.loc[:, 'no_of_subs_diff_brand'] = diff_brand_count
            return dataset

        def psg_pb_prodcount(dataset):

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
            dataset.loc[:, 'psg_prod_count'] = count_psg_prod
            dataset.loc[:, 'price_band_prod_count'] = count_pb_prod
            return dataset
        ### To incorporate merch grp and range class

        def no_of_stores_holidays(dataset):

            stores = pd.DataFrame(range_space_store[(range_space_store['merchandise_group_code'] == merch_code) & 
                                   (range_space_store['range_space_break_code'] >= Range_class)]['retail_outlet_number'])

            stores_size = pd.merge(stores, store_details_df, on = 'retail_outlet_number', how = 'left')
            APC_stores = stores_size.groupby(['area_price_code'], as_index = False).aggregate({'retail_outlet_number': lambda x: x.nunique(),
                            'pfs_store': lambda x: x.nunique(), 'store_5k': lambda x: x.nunique(),
                            'store_20k': lambda x: x.nunique(), 'store_50k': lambda x: x.nunique(),
                            'store_100k': lambda x: x.nunique(), 'store_100kplus': lambda x: x.nunique()})
            APC_stores = APC_stores.loc[:,['area_price_code', 'retail_outlet_number', 'pfs_store', 'store_5k', 'store_20k', 'store_50k', 'store_100k', 'store_100kplus']]

            APC_stores.columns = ['area_price_code', 'no_stores', 'no_pfs_Stores', 'no_5k_stores',
               'no_20k_stores', 'no_50k_stores', 'no_100k_stores', 'no_100kplus_stores']
            APC_stores.columns = map(str.lower, APC_stores.columns)
            df_final = pd.DataFrame()
            for i in range(0,len(APC_stores)):
                df_temp = dataset.copy()

                ####### To update the no of stores based on store type
                df_temp.loc[:,"area_price_code_" + str(APC_stores['area_price_code'][i])] = 1
                df_temp.loc[:,"no_stores"] = APC_stores['no_stores'][i]
                df_temp.loc[:,"no_pfs_stores"] = APC_stores['no_pfs_stores'][i]
                df_temp.loc[:,"no_5k_stores"] = APC_stores['no_5k_stores'][i]
                df_temp.loc[:,"no_20k_stores"] = APC_stores['no_20k_stores'][i]
                df_temp.loc[:,"no_50k_stores"] = APC_stores['no_50k_stores'][i]
                df_temp.loc[:,"no_100k_stores"] = APC_stores['no_100k_stores'][i]
                df_temp.loc[:,"no_100kplus_stores"] = APC_stores['no_100kplus_stores'][i]


                #### To get the holidays count based on store type
                uk_holidays_df_store = uk_holidays_df[['year_week_number','holiday_flag','area_price_code']]
                uk_holidays_df_store = uk_holidays_df_store.loc[uk_holidays_df['area_price_code']==APC_stores['area_price_code'][i]]
                uk_holidays_df_store = uk_holidays_df_store[['year_week_number','holiday_flag']]

                #### To get the count of holidays in a week. Sme week will repeat for n number of holidays n times
                holiday_count_week = uk_holidays_df_store.groupby(['year_week_number'], as_index=False).agg({'holiday_flag': sum})

                ####Doing a left join on our ADS to get the holiday count. Wherever it is NA it will be given 0 as our holiday table has only ####those weeks which has atleast 1 holiday 
                df_temp = pd.merge(df_temp,holiday_count_week, left_on=['year_week_number'], right_on=['year_week_number'], how='left' )

                df_temp= df_temp.fillna(0)

                ###Removing a holidays_count columns
                del(df_temp['holiday_count'])

                df_temp = df_temp.rename(columns={'holiday_flag':'holiday_count'})
                df_final = df_final.append(df_temp)

            Period_Number = (week_mapping.loc[week_mapping['year_week_number'] == 201631]).iloc[0]['period_number']
            df_final_new = df_final
            df_final_new.loc[:,'launch_month'] = Period_Number
            return df_final_new

        def run_cannibilization_model(input_test_dataset,week_flag,time_frame):
            Cannibalization_perc = 0 

            week_flag =week_flag
            time_frame = time_frame
            ####Xg boost model pickled
            #global xg_model

            input_test_dataset



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
            #### Cannibalization percentage 
            print("llllllllllllllllllllllll")
            print(len(sim_prod))
            if len(sim_prod)==0:
                if Buying_controller=='Frozen Impulse':
                    if week_flag =='Latest 13 Weeks':
                        high_volume_cutoff = (45100/21)*13
                        low_volume_cutoff = (15300/21)*13
                    if week_flag =='Latest 26 Weeks':
                        high_volume_cutoff = (45100/21)*26
                        low_volume_flag_upper_cutoff = (15300/21)*26
                    if week_flag =='Latest 52 Weeks':
                        high_volume_cutoff = (45100/21)*52
                        low_volume_cutoff = (15300/21)*52

                if Buying_controller=='Grocery Cereals':
                    if week_flag =='Latest 13 Weeks':
                        high_volume_cutoff = (44510/21)*13
                        low_volume_cutoff = (37700/21)*13
                    if week_flag =='Latest 26 Weeks':
                        high_volume_cutoff = (44510/21)*26
                        low_volume_cutoff = (37700/21)*26
                    if week_flag =='Latest 52 Weeks':
                        high_volume_cutoff = (44510/21)*52
                        low_volume_cutoff = (37700/21)*52

                if Buying_controller=='Meat Fish and Veg':
                    if week_flag =='Latest 13 Weeks':
                        high_volume_cutoff = (58000/21)*13
                        low_volume_cutoff = (24700/21)*13
                    if week_flag =='Latest 26 Weeks':
                        high_volume_cutoff = (58000/21)*26
                        low_volume_cutoff = (24700/21)*26
                    if week_flag =='Latest 52 Weeks':
                        high_volume_cutoff = (58000/21)*52
                        low_volume_cutoff = (24700/21)*52

                if (total_forecasted_volume>high_volume_cutoff):
                    Volume_flag= 'High'
                elif (total_forecasted_volume<low_volume_cutoff):
                    Volume_flag = 'Low'
                else :
                    Volume_flag = 'Medium'
                ####Need to import psg code to desc mapping

                if (psg_code+Volume_flag+brand_ind  in list(PSGVolBrandBuckets['bucket_value'])):
                    print("puuuuuuuuuuurrrrrrrrrrrrrrrraaaaaaaaaaaaaaaaa")
                    Cannibalization_perc = (PSGVolBrandBuckets.loc[PSGVolBrandBuckets['bucket_value'] == psg_code+Volume_flag+brand_ind]).iloc[0]['cannibalization']
                    #Cannibalization_perc = Cannibalization_perc.round(2)

                elif (psg_code+Volume_flag  in list(PSGVolBuckets['bucket_value'])):
                    print("kkkkkkkkkkkkkkkkkkkaaaaaaaaaaaaaaaaaammmmmmmmmmmmmmmmmmm")
                    Cannibalization_perc = (PSGVolBuckets.loc[PSGVolBuckets['bucket_value'] == psg_code+Volume_flag]).iloc[0]['cannibalization']
                    #Cannibalization_perc = Cannibalization_perc.round(2)

                elif (psg_code  in list(PSGBuckets['bucket_value'])):
                    print("ekdummmmmmmmmmmmmmmmmmm thodaaaaaaaaaaaaaaaaaaaa")
                    Cannibalization_perc = (PSGBuckets.loc[PSGBuckets['bucket_value'] == psg_code]).iloc[0]['cannibalization']
                    #Cannibalization_perc = Cannibalization_perc.round(2)

                else :
                    print("naiiiiiiiiiiiiiiiiiiiiiii")
                    Cannibalization_perc = 0

            sim_prod_subset = sim_prod_new[sim_prod_new['final_score']>0.8]


            #### Applying sim prod with threshold as 0.8

            if len(sim_prod_subset)>0:
                if (brand_ind=='T'):
                    sim_prod_subset= sim_prod_subset.sort_values(['brand_ind','final_score', 'launch_tesco_week'], ascending=[False,False, False])
                elif (brand_ind=='B'):
                    sim_prod_subset= sim_prod_subset.sort_values(['brand_ind','final_score', 'launch_tesco_week'], ascending=[True,False, False])

                Cannibalization_perc = sim_prod_subset.iloc[0,:]['cannibalization']



            #     ##### To get the cannibilized volume 

            total_forecasted_volume = float(total_forecasted_volume)
            # if not Cannibalization_perc:
            #   print("iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
            #   Cannibalization_perc=0
            Cannibalization_perc = float(Cannibalization_perc)
            #global Cannibalization_perc

            forecasted_cannibilization_volume = Cannibalization_perc*total_forecasted_volume

            ##### To get the cannibilized sales
            forecasted_cannibilization_volume = float(forecasted_cannibilization_volume)

            forecasted_cannibilization_sales = forecasted_cannibilization_volume*asp
            #forecasted_cannibilization_sales = forecasted_cannibilization_sales.round(2)

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

            ### Subsetting above table for only the selected psg
            psg_product_contri_df = psg_product_contri_df.loc[psg_product_contri_df['product_sub_group_description']==Product_Sub_Group_Description]




            # #### Taking variable from the function for getting the time frame
            psg_product_contri_df = psg_product_contri_df.loc[psg_product_contri_df['time_period']==time_frame]

            # ###### Getting the total volume for the selected psg and time period
            total_psg_forecasted_volume = sum(psg_product_contri_df['predicted_volume'])





            # ##### Getting the asp for all the bpn in product contri
            product_price_df_new = product_price_df
            product_price_df_new['asp'] = product_price_df_new['asp'].astype('float')
            #print(product_price_df_new.head())
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

            return model_output


        def similar_product_cannabilized(time_frame):
            sim_prod_product = sim_prod[['base_product_number']]

            time_frame = time_frame
            #### GETTING ONLY THOSE bpn WHICH WERE THERE IN SIM PROD (THRESHOLD OF 0.8)

            product_contri_df_new = pd.merge(product_contri_df,sim_prod_product,left_on=['base_product_number'], right_on=['base_product_number'],how='inner')

            All_attribute_subset = All_attribute.dropna()
            All_attribute_subset = All_attribute_subset[['base_product_number','product_sub_group_description']]


            product_psg_mapping =pd.merge(product_contri_df_new, All_attribute_subset, left_on=['base_product_number'], right_on=['base_product_number'], how='left' )

            ### rolling volume at psg bpn level
            psg_product_contri_df = product_psg_mapping.groupby(['time_period','product_sub_group_description','base_product_number'], as_index=False).agg({'predicted_volume': sum})

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
    
            product_desc_branded = pd.merge(psg_product_contri_df, product_desc_df, left_on=['base_product_number'], right_on=['base_product_number'], how='left')
      
            product_desc_branded = product_desc_branded[['long_description','brand_indicator','predicted_volume','predicted_sales']]
            product_desc_branded['brand_indicator'] = product_desc_branded['brand_indicator'].replace('T','Own Label')
            product_desc_branded['brand_indicator'] = product_desc_branded['brand_indicator'].replace('B','Branded')

            return product_desc_branded


        ### for calculating forecast
        if (total_forecasted_volume ==0) & (Cannibalization_perc ==0):  

            print("inside forecast")

            similar_product = pd.DataFrame()
            if week == 'Latest 13 Weeks':
                # call function for priceband and psg code 
                print('function 1')
                psg_priceband_merch = get_priceband_psg_merch_code()
                psg_code = psg_priceband_merch['psg_code']
                price_band = psg_priceband_merch['price_band']
                merch_code = psg_priceband_merch['merch_code']

                week_mapping = week_mapping.sort_values('year_week_number')
                week_mapping['calendar_date'] = pd.to_datetime(week_mapping['calendar_date'], format='%Y-%m-%d')
                today = datetime.datetime.today().strftime('%Y-%m-%d')
                current_week = (week_mapping.loc[week_mapping['calendar_date'] == today]).iloc[0]['year_week_number']
   
                ### Taking Columns to spread
                ### Converting buying controller to lower case and concating the name 
                bc_name = (Buying_controller.replace(" ", "").lower())

                dataset1 = dataset[[bc_name]]
                dataset1.dropna(inplace=True)
                dataset1 =pd.DataFrame(columns=dataset1[bc_name].values)
             
                # call function for creating ads structure
                print('function 2')
                df = get_ads_structure()
             
                # call function for filling ads structure
                print('function 3')
                df1 = fill_ads_structure(df)
              
                # call function for finding similar products
                print('function 4')
                sim_prod = similar_products()
                #call function for substitute calculation 
                if (len(sim_prod)) > 0:
                    df2 = subs_same_different(df1)

                else:
                    df2 = df1
                    df2.loc[:, 'no_of_subs_same_brand'] = 0
                    df2.loc[:, 'no_of_subs_diff_brand'] = 0

                All_attribute_pb = All_attribute.loc[(All_attribute['price_band']== price_band)]

                # call function for product count with same psg and price band
                print('function 5')
                df3 = psg_pb_prodcount(df2)

                print('function 6')
                df_final_new = no_of_stores_holidays(df3)

                week_index = df_final_new[['year_week_number']].drop_duplicates().reset_index()
                week_index = week_index.rename(columns={'index':'rank'})
                # Creating a index for the week number
                df_final_new = pd.merge(df_final_new,week_index,on=['year_week_number'],how='left')
                # global df_final_new

                # Subsetting for 13 weeks 
                df_test_52weeks = df_final_new
                df_test = df_test_52weeks[df_test_52weeks['rank']<=12]

                del(df_test['year_week_number'])
                del(df_test['rank'])

                gzip_pickle = gzip.open("api/pickle/xg_model_MEATFISHANDVEG.pkl", "rb")

                xg_model = pickle.load(gzip_pickle)
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
                # Cannibalization_perc = 0 
                output_cannib = run_cannibilization_model(df_test,"Latest 13 Weeks","3_months")

                output_cannib_volume = pd.DataFrame(output_cannib[0]['data_volume'])  ##Output 1 volume
                output_cannib_sales = pd.DataFrame(output_cannib[0]['data_sales'])  ## Output 1 Sales
                
                data_dict_volume={}
                data = [{
                           "name":"NPD_Volume","value":int(output_cannib_volume['forecast'][0])
                           },
                        {
                            "name":"Cannibilized_volume","value":-int(output_cannib_volume['cannibilization_value'][0])
                            }]

                volume={}
                volume={"Cannibilization_perc":output_cannib_volume['cannibilization_perc'][0].round(decimals=2),
                                                "perc_impact_psg":output_cannib_volume['perc_change_in_psg'][0].round(decimals=4)
                                   }
                data_dict_volume["data"]=data
                data_dict_volume["impact"]=volume

                data_dict_sales={}
                data = [{
                                "name":"NPD_Value","value":int(output_cannib_sales['forecast'][0])
                                },
                                {
                                                "name":"Cannibilized_sales",
                                                "value":-int(output_cannib_sales['cannibilization_value'][0])
                                }]
                sales={}
                sales={
                                                "Cannibilization_perc" : output_cannib_sales['cannibilization_perc'][0].round(decimals=2),
                                                "perc_impact_psg":(output_cannib_sales['perc_change_in_psg'][0]).round(decimals=4)
                                   }
                data_dict_sales["data"]=data
                data_dict_sales["impact"]=sales


                similar_product = similar_product_cannabilized('3_months')
                

            elif week == 'Latest 26 Weeks':
                # call function for priceband and psg code 
                print('function 1')
                psg_priceband_merch = get_priceband_psg_merch_code()
                psg_code = psg_priceband_merch['psg_code']
                price_band = psg_priceband_merch['price_band']
                merch_code = psg_priceband_merch['merch_code']
                week_mapping = week_mapping.sort_values('year_week_number')
                week_mapping['calendar_date'] = pd.to_datetime(week_mapping['calendar_date'], format='%Y-%m-%d')
                today = datetime.datetime.today().strftime('%Y-%m-%d')
                current_week = (week_mapping.loc[week_mapping['calendar_date'] == today]).iloc[0]['year_week_number']
                ### Taking Columns to spread
                ### Converting buying controller to lower case and concating the name 
                bc_name = (Buying_controller.replace(" ", "").lower())

                dataset1 = dataset[[bc_name]]
                dataset1.dropna(inplace=True)
                dataset1 =pd.DataFrame(columns=dataset1[bc_name].values)
                # call function for creating ads structure
                print('function 2')
                df = get_ads_structure()
                 # call function for filling ads structure
                print('function 3')
                df1 = fill_ads_structure(df)
                # call function for finding similar products
                print('function 4')
                sim_prod = similar_products()
                #call function for substitute calculation 
                if (len(sim_prod)) > 0:
                    df2 = subs_same_different(df1)
                elif ():
                    df2.loc[:, 'no_of_subs_same_brand'] = 0
                    df2.loc[:, 'no_of_subs_diff_brand'] = 0

                All_attribute_pb = All_attribute.loc[(All_attribute['price_band']== price_band)]

                # call function for product count with same psg and price band
                print('function 5')
                df3 = psg_pb_prodcount(df2)

                print('function 6')
                df_final_new = no_of_stores_holidays(df3)

                week_index = df_final_new[['year_week_number']].drop_duplicates().reset_index()
                week_index = week_index.rename(columns={'index':'rank'})
                # Creating a index for the week number
                df_final_new = pd.merge(df_final_new,week_index,on=['year_week_number'],how='left')
       
                # Subsetting for 26 weeks 
                df_test_52weeks = df_final_new
                df_test = df_test_52weeks[df_test_52weeks['rank']<=25]

                del(df_test['year_week_number'])
                del(df_test['rank'])

                gzip_pickle = gzip.open("api/pickle/xg_model_MEATFISHANDVEG.pkl", "rb")

                xg_model = pickle.load(gzip_pickle)
                #global xg_model
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
                # Cannibalization_perc = 0 
                output_cannib = run_cannibilization_model(df_test,"Latest 26 Weeks","6_months")

                output_cannib_volume = pd.DataFrame(output_cannib[0]['data_volume'])  ##Output 1 volume
                output_cannib_sales = pd.DataFrame(output_cannib[0]['data_sales'])  ## Output 1 Sales
                
                data_dict_volume={}
                data = [{
                           "name":"NPD_Volume","value":int(output_cannib_volume['forecast'][0])
                           },
                        {
                            "name":"Cannibilized_volume","value":-int(output_cannib_volume['cannibilization_value'][0])
                            }]

                volume={}
                volume={"Cannibilization_perc":output_cannib_volume['cannibilization_perc'][0].round(decimals=2),
                                                "perc_impact_psg":output_cannib_volume['perc_change_in_psg'][0].round(decimals=4)
                                   }
                data_dict_volume["data"]=data
                data_dict_volume["impact"]=volume

                data_dict_sales={}
                data = [{
                        "name":"NPD_Value","value":int(output_cannib_sales['forecast'][0])
                        },
                        {
                                        "name":"Cannibilized_sales",
                                        "value":-int(output_cannib_sales['cannibilization_value'][0])
                        }]
                sales={}
                sales={
                        "Cannibilization_perc" : output_cannib_sales['cannibilization_perc'][0].round(decimals=2),
                        "perc_impact_psg":(output_cannib_sales['perc_change_in_psg'][0]).round(decimals=4)
                       }
                data_dict_sales["data"]=data
                data_dict_sales["impact"]=sales

                similar_product = similar_product_cannabilized('6_months')

            
            elif week == 'Latest 52 Weeks':
                
                            
                print('function 1')
                psg_priceband_merch = get_priceband_psg_merch_code()
                psg_code = psg_priceband_merch['psg_code']
                
                price_band = psg_priceband_merch['price_band']
                
                merch_code = psg_priceband_merch['merch_code']
                
                week_mapping = week_mapping.sort_values('year_week_number')
                week_mapping['calendar_date'] = pd.to_datetime(week_mapping['calendar_date'], format='%Y-%m-%d')
                today = datetime.datetime.today().strftime('%Y-%m-%d')
                current_week = (week_mapping.loc[week_mapping['calendar_date'] == today]).iloc[0]['year_week_number']
                ### Taking Columns to spread
                ### Converting buying controller to lower case and concating the name 
                bc_name = (Buying_controller.replace(" ", "").lower())

                dataset1 = dataset[[bc_name]]
                dataset1.dropna(inplace=True)
                dataset1 =pd.DataFrame(columns=dataset1[bc_name].values)
                # call function for creating ads structure
                print('function 2')
                df = get_ads_structure()
                # call function for filling ads structure
                print('function 3')
                df1 = fill_ads_structure(df)
                # call function for finding similar products
                print('function 4')
                sim_prod = similar_products()
                #global sim_prod
                #call function for substitute calculation 
                if (len(sim_prod)) > 0:
                    df2 = subs_same_different(df1)
                elif ():
                    df2.loc[:, 'no_of_subs_same_brand'] = 0
                    df2.loc[:, 'no_of_subs_diff_brand'] = 0

                All_attribute_pb = All_attribute.loc[(All_attribute['price_band']== price_band)]

                # call function for product count with same psg and price band
                print('function 5')
                df3 = psg_pb_prodcount(df2)

                print('function 6')
                df_final_new = no_of_stores_holidays(df3)

                week_index = df_final_new[['year_week_number']].drop_duplicates().reset_index()
                week_index = week_index.rename(columns={'index':'rank'})
                # Creating a index for the week number
                df_final_new = pd.merge(df_final_new,week_index,on=['year_week_number'],how='left')
              
                # Subsetting for 13 weeks 
                df_test = df_final_new
        
                del(df_test['year_week_number'])
                del(df_test['rank'])

                gzip_pickle = gzip.open("api/pickle/xg_model_MEATFISHANDVEG.pkl", "rb")

                xg_model = pickle.load(gzip_pickle)
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
                Cannibalization_perc = 0 
                output_cannib = run_cannibilization_model(df_test,"Latest 52 Weeks","12_months")

                output_cannib_volume = pd.DataFrame(output_cannib[0]['data_volume'])  ##Output 1 volume
                output_cannib_sales = pd.DataFrame(output_cannib[0]['data_sales'])  ## Output 1 Sales
                
                data_dict_volume={}
                data = [{
                           "name":"NPD_Volume","value":int(output_cannib_volume['forecast'][0])
                           },
                        {
                            "name":"Cannibilized_volume","value":-int(output_cannib_volume['cannibilization_value'][0])
                            }]

                volume={}
                volume={"Cannibilization_perc":output_cannib_volume['cannibilization_perc'][0].round(decimals=2),
                                                "perc_impact_psg":output_cannib_volume['perc_change_in_psg'][0].round(decimals=4)
                                   }
                data_dict_volume["data"]=data
                data_dict_volume["impact"]=volume

                data_dict_sales={}
                data = [{
                        "name":"NPD_Value","value":int(output_cannib_sales['forecast'][0])
                        },
                        {
                        "name":"Cannibilized_sales",
                        "value":-int(output_cannib_sales['cannibilization_value'][0])
                        }]
                sales={}
                sales={
                        "Cannibilization_perc" : output_cannib_sales['cannibilization_perc'][0].round(decimals=2),
                        "perc_impact_psg":(output_cannib_sales['perc_change_in_psg'][0]).round(decimals=4)
                       }
                data_dict_sales["data"]=data
                data_dict_sales["impact"]=sales


                similar_product = similar_product_cannabilized('12_months')

            similar_product_final = similar_product[similar_product['long_description'].str.contains(search,case=False)]

            num_pages = math.ceil((len(similar_product_final)/5))
            start_index = (page-1)*5+1
            count = len(similar_product)
            end_index = page*5
            similar_product_final = similar_product_final.reset_index() 
            df_new=similar_product_final.loc[start_row:end_row,]
            data_table = {
                    'df': df_new.to_dict(orient='records'),
                    'pagination_count': num_pages,
                    'page': page,
                    'start_index': start_index,
                    'count': count,
                    'end_index': end_index,
            }

        # for edit forecast pop-up
        elif (total_forecasted_volume !=0) & (Cannibalization_perc !=0):

            print("inside edit forecast")

            if week_flag=="Latest 13 Weeks":
                time_frame = "3_months"
            elif week_flag=="Latest 26 Weeks":
                time_frame = "6_months"
            elif week_flag=="Latest 52 Weeks":
                time_frame = "12_months"


            #### for calculating data table 
            psg_priceband_merch = get_priceband_psg_merch_code()

            price_band = psg_priceband_merch['price_band']
            psg_code = psg_priceband_merch['psg_code']
            sim_prod = similar_products()
            psg_priceband_merch = get_priceband_psg_merch_code()

            similar_product = similar_product_cannabilized(time_frame)

            similar_product_final = similar_product[similar_product['long_description'].str.contains(search,case=False)]

            num_pages = math.ceil((len(similar_product_final)/5))
            start_index = (page-1)*5+1
            count = len(similar_product)
            end_index = page*5
            similar_product_final = similar_product_final.reset_index() 
            df_new=similar_product_final.loc[start_row:end_row,]
            data_table = {
                    'df': df_new.to_dict(orient='records'),
                    'pagination_count': num_pages,
                    'page': page,
                    'start_index': start_index,
                    'count': count,
                    'end_index': end_index,
            }


            forecasted_cannibilization_volume = float(Cannibalization_perc) * float(total_forecasted_volume)

            ##### To get the cannibilized sales
            forecasted_cannibilization_sales = forecasted_cannibilization_volume*asp
            #forecasted_cannibilization_sales = forecasted_cannibilization_sales.round(2)
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
                       "name":"NPD_Volume","value":output_cannib_volume['forecast'][0]
                       },
                    {
                        "name":"Cannibilized_volume","value":output_cannib_volume['cannibilization_value'][0]
                        }]

            volume={}
            volume={"Cannibilization_perc":output_cannib_volume['cannibilization_perc'][0],
                                            "perc_impact_psg":output_cannib_volume['perc_change_in_psg'][0].round(decimals=4)
                               }
            data_dict_volume["data"]=data
            data_dict_volume["impact"]=volume
            data_dict_sales={}
            data = [{
                    "name":"NPD_Value","value":output_cannib_sales['forecast'][0]
                    },
                    {
                    "name":"Cannibilized_sales",
                    "value":output_cannib_sales['cannibilization_value'][0]
                    }]
            sales={}
            sales={
                    "Cannibilization_perc" : output_cannib_sales['cannibilization_perc'][0],
                    "perc_impact_psg":(output_cannib_sales['perc_change_in_psg'][0]).round(decimals=4)
                   }
            data_dict_sales["data"]=data
            data_dict_sales["impact"]=sales


            model_output = [{'data_volume': data_volume , 'data_sales' : data_sales }]



        return JsonResponse({'sales_chart':data_dict_sales,'volume_chart':data_dict_volume,'similar_product_table':data_table},safe=False)



#### Product Impact Filters

def col_distinct_product(kwargs, col_name):
    queryset = product_hierarchy.objects.filter(**kwargs).values(col_name).order_by(col_name).distinct()
    base_product_number_list = [k.get(col_name) for k in queryset]
    return base_product_number_list

def make_json_product(sent_req):
    print('*********************\n       FILTERS2 \n*********************')
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

    # print('lowest_key:', lowest_key, '|', 'lowest', lowest)

    final_list = []  # final list to send

    col_unique_list_name = []  # rename
    col_unique_list_name_obj = {}  # rename
    for col_name in cols:
        print('\n********* \n' + col_name + '\n*********')
        # print('sent_req.get(col_name):', sent_req.get(col_name))
        col_unique_list = col_distinct_product({}, col_name)
        col_unique_list_name.append({'name': col_name,
                                     'unique_elements': col_unique_list})
        col_unique_list_name_obj[col_name] = col_unique_list
        # args sent as url params
        kwargs2 = {reqobj + '__in': sent_req.get(reqobj) for reqobj in sent_req.keys()}

        category_of_sent_obj_list = col_distinct_product(kwargs2, col_name)
        print(len(category_of_sent_obj_list))
        sent_obj_category_list = []

        # get unique elements for `col_name`
        for i in category_of_sent_obj_list:
            sent_obj_category_list.append(i)

        def highlight_check(category_unique):
            # print(title)
            if len(sent_req.keys()) > 0:
                highlighted = False
                if col_name in sent_req.keys():
                    if col_name == cols[lowest]:
                        queryset = product_hierarchy.objects.filter(**{col_name: category_unique})[:1].get()
                        # x = getattr(queryset, cols[lowest])
                        y = getattr(queryset, cols[second_lowest])
                        # print(x, '|', y, '|', cols[lowest], '|',
                        #       'Category_second_last:' + cols[second_lowest],
                        #       '|', col_name,
                        #       '|', category_unique)
                        for i in sent_req.keys():
                            print('keys:', i, sent_req.get(i))
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

class filters_product_impact(APIView):
    def get(self, request):
        print(request.GET)
        obj = {}
        get_keys = request.GET.keys()
        for i in get_keys:
            # print(request.GET.getlist(i))
            obj[i] = request.GET.getlist(i)
        # print(obj)
        return make_json_product(obj)


####Product Impact
#chart
class product_impact_chart(APIView):

    def get(self, request, *args):
        global supplier_table_popup
        global delist_table_popup
        global sup_sales_table
        global delist_prod_table
        # supplier_table_popup = pd.DataFrame()
        sup_sales_table = pd.DataFrame()
        # delist_table_popup = pd.DataFrame()
        delist_prod_table = pd.DataFrame()

        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        args.pop('format__iexact', None)

        args_list = {reqobj + '__in': request.GET.getlist(reqobj) for reqobj in request.GET.keys()}
        print(args_list)

        if not args:
            bc = ['Meat Fish and Veg']
            store = ['Overview']
            future = ['3_months']
            input_tpns = None
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

            input_tpns = args_list.pop('long_description__in', None)

        def volume_transfer_logic(bc, store, future, input_tpns, delist):

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
            #get DD quantile prods
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
            #make new sub score add upto1
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
            #for A,B,C qauntile prods
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
            # check if substitute product is getting delisted
            check_sub_delisted1 = pd.merge(join_on_parent_prob, input_tpns, left_on=['sub_prod'],
                                           right_on=['base_product_number'], how='left')

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

            join_tot_delisted1 = join_tot_delisted1[
                ['base_product_number_x', 'predicted_volume', 'pps_ros_quantile', 'base_prod', 'sub_prod',
                 'actual_similarity_score',
                 'total_subs_score_alive', 'total_subs_score_delisted', 'subs_score_alive', 'tcs_per',
                 'exclusivity_per', 'new_sub_score']].drop_duplicates().fillna(0).reset_index(drop=True)
            # In[16]:

            dd_quantile_prob_delised = join_tot_delisted1[
                join_tot_delisted1['base_product_number_x'].isin(dd_prods['base_product_number'])]

            dd_quantile_prob_delised = dd_quantile_prob_delised.drop_duplicates().fillna(0).reset_index(drop=True)
            # make new sub score add upto1
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
                1 - dd_quantile_prob_delised['tcs_per'].astype('float') - dd_quantile_prob_delised['exclusivity_per'].astype('float'))

            dd_quantile_prob_delised['vol_transfer_prob'] = 0
            dd_quantile_prob_delised['vol_transfer_prob'] = dd_quantile_prob_delised['predicted_volume'].astype(
                'float') * dd_quantile_prob_delised['final_sub_score'].astype('float')

            # Rest prods (a,b,c quantile)

            rest_prob_delised = join_tot_delisted1[
                ~join_tot_delisted1['base_product_number_x'].isin(dd_prods['base_product_number'])]
            rest_prob_delised['tcs_per'] = rest_prob_delised['tcs_per'].astype('float')
            rest_prob_delised['exclusivity_per'] = rest_prob_delised['exclusivity_per'].astype('float')
            rest_prob_delised['predicted_volume'] = rest_prob_delised['predicted_volume'].astype('float')

            rest_prob_delised['final_sub_score'] = 0
            rest_prob_delised['final_sub_score'] = rest_prob_delised['final_sub_score'].astype('float')
            rest_prob_delised['new_sub_score'] = rest_prob_delised['new_sub_score'].astype('float')
            rest_prob_delised['final_sub_score'] = rest_prob_delised['new_sub_score'] * (1 - rest_prob_delised['tcs_per'] - rest_prob_delised['exclusivity_per'])
            rest_prob_delised['vol_transfer_prob'] = 0
            rest_prob_delised['vol_transfer_prob'] = rest_prob_delised['predicted_volume'] * rest_prob_delised['final_sub_score']
            #append a,b,c dataset
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
            #merge with probable similar products
            cate_fore_similar_prods = pd.merge(cate_fore_SH_missing_delised, prob_sub_score[
                ['base_prod', 'sub_prod', 'actual_similarity_score', 'similarity_score']],
                                               left_on='base_product_number', right_on='base_prod', how='left')
            cate_fore_similar_prods_present = cate_fore_similar_prods[cate_fore_similar_prods.base_prod.notnull()]

            # In[19]:

            cate_fore_similar_prods_present = cate_fore_similar_prods_present[
                [u'base_product_number', u'predicted_volume', u'pps_ros_quantile', u'base_prod', u'sub_prod',
                 u'actual_similarity_score', u'similarity_score', 'ros_quantile']]

            #Qauntile as per (pps, ros) into high, medium,low
            Low_dh_missing_delised = cate_fore_similar_prods_present[
                cate_fore_similar_prods_present.pps_ros_quantile == "Low"]

            # Low_dh_missing_delised = pd.merge(Low_dh_missing_delised[['base_product_number','predicted_volume']],prob_sub[['base_prod','sub_prod','actual_similarity_score']], left_on ="base_product_number", right_on = "base_prod", how = "left")
            #check if sub is getting delisted
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
            #for low bucket: all demand transfer
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
            #get %tcs and %ex from sub prods
            med_dh_missing_delised = pd.merge(med_dh_missing_delised, All_BC_subs, left_on=['sub_prod'],
                                              right_on=['productcode'], how="left")
            med_dh_missing_delised = med_dh_missing_delised[med_dh_missing_delised.productcode.notnull()]
            med_dh_missing_delised = med_dh_missing_delised.drop_duplicates().reset_index(drop=True).fillna(0)

            med_dh_missing_delised['predicted_volume'] = med_dh_missing_delised['predicted_volume'].astype('float')
            med_dh_missing_delised['tcs_per'] = med_dh_missing_delised['tcs_per'].astype('float')
            med_dh_missing_delised['exclusivity_per'] = med_dh_missing_delised['exclusivity_per'].astype('float')


            # In[21]:

            # for condition base prod ros bucket = sub prod ros bucket
            sub_prod = pd.merge(med_dh_missing_delised[['sub_prod', 'ros_quantile']], pps_ros, left_on="sub_prod",
                                right_on="base_product_number", how='left')
            sub_prod = sub_prod.rename(columns={'ros_quantile_y': 'ros_tag_sub'})
            sub_prod = sub_prod[['sub_prod', 'ros_tag_sub']].drop_duplicates().reset_index(drop=True).fillna(0)
            #get similar prods above avg similariy score
            cut_off = med_dh_missing_delised[['base_prod', 'similarity_score']].drop_duplicates()
            cut_off = cut_off[['similarity_score']].mean()
            cut_off = cut_off['similarity_score']

            similar_prods = med_dh_missing_delised[
                [u'base_product_number', u'sub_prod', u'ros_quantile', u'similarity_score']].drop_duplicates().reset_index(drop = True).fillna(0)
            similar_prods = pd.merge(similar_prods, sub_prod[['sub_prod', 'ros_tag_sub']], left_on="sub_prod",
                                     right_on='sub_prod', how="left")
            similar_prods_filter1 = similar_prods[
                (similar_prods.similarity_score > cut_off) & (similar_prods.ros_quantile == similar_prods.ros_tag_sub)]
            similar_prods_filter1['sim_ros'] = "P"
            #if couldnt satisfy above condition, take prods above similar prods only
            similar_prods_filter2 = pd.merge(similar_prods, similar_prods_filter1[['base_product_number', 'sim_ros']],
                                             left_on="base_product_number", right_on="base_product_number", how='left')
            similar_prods_filter2 = similar_prods_filter2[(similar_prods_filter2.sim_ros.isnull())]
            similar_prods_filter2 = similar_prods_filter2[(similar_prods_filter2.similarity_score > cut_off)]
            similar_prods_filter = similar_prods_filter1.append(similar_prods_filter2)
            similar_prods_filter = similar_prods_filter.reset_index(drop=True)
            # In[22]:

            # take only similar prods
            med_dh_missing_delised = med_dh_missing_delised[
                med_dh_missing_delised['sub_prod'].isin(similar_prods_filter['sub_prod'])]

            # In[23]:
            #take mean of %tcs and %ex for base prod
            similar_prods_w_excl_TCS_grp = med_dh_missing_delised.groupby(['base_product_number'], as_index=False).agg(
                {'tcs_per': 'mean', 'exclusivity_per': 'mean'})
            similar_prods_w_excl_TCS_grp = similar_prods_w_excl_TCS_grp.rename(
                columns={'tcs_per': 'tcs_per_avg', 'exclusivity_per': 'exclusivity_per_avg'})
            #dataset ready for med and high bucket
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
            #check if substitute product is getting delisted
            check_sub_delisted1 = pd.merge(cate_fore_similar_prods_present, input_tpns, left_on=['sub_prod'],
                                           right_on=['base_product_number'], how='left')

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
            #susbset for Medium bucket as per ros and pps
            Med_dh_missing_delised = join_tot_delisted1[join_tot_delisted1.pps_ros_quantile == "Med"]
            # adjust new sub score add upto 1
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



            # In[30]:
            #for high bucket as per ros, pps
            High_dh_missing_delised = join_tot_delisted1[join_tot_delisted1.pps_ros_quantile == "High"]

            # In[31]:

            High_dh_missing_delised['final_sub_score'] = 0
            High_dh_missing_delised['final_sub_score'] = High_dh_missing_delised['new_sub_score'] * (
                1 - High_dh_missing_delised['exclusivity_per_avg'])

            High_dh_missing_delised['predicted_volume'] = High_dh_missing_delised['predicted_volume'].astype('float')

            High_dh_missing_delised['vol_transfer_prob'] = 0
            High_dh_missing_delised['vol_transfer_prob'] = High_dh_missing_delised['predicted_volume'] * \
                                                           High_dh_missing_delised['final_sub_score']


            # In[33]:

            Low_dh_missing_delised_sub = Low_dh_missing_delised_sub[
                ['base_product_number_x', 'sub_prod', 'predicted_volume', 'vol_transfer_prob']]
            med_dh_missing_delised_sub = med_dh_missing_delised_sub[
                ['base_product_number_x', 'sub_prod', 'predicted_volume', 'vol_transfer_prob']]
            High_dh_missing_delised = High_dh_missing_delised[
                ['base_product_number_x', 'sub_prod', 'predicted_volume', 'vol_transfer_prob']]

            # In[34]:
            #append high, medium low bucket products
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
            #mask all na
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

            #get brand indicator
            prod_hrchy = read_frame(
                product_desc.objects.all().filter(buying_controller__in=bc).values('base_product_number',
                                                                                   'brand_indicator',
                                                                                   'long_description').distinct())
            product_dataset = pd.merge(product_dataset, prod_hrchy, left_on=['substituteproductcode'],
                                       right_on=['base_product_number'], how='left')
            product_dataset = product_dataset[
                ['productcode', 'substituteproductcode', 'delist_pred_vol', 'tot_vols_transfer', 'brand_indicator']]

            # In[39]:

            # read product price data and assign it to variable 'price' on base prod
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
        #Logic for overview
        if store == ['Overview']:

            if input_tpns is None:
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
                input_tpns_main['base_product_number'] = input_tpns_main['base_product_number'].str[-8:]
                input_tpns_main['base_product_number'] = input_tpns_main['base_product_number'].astype('int')
                delist_main = input_tpns_main['base_product_number'].drop_duplicates().values.tolist()

            # print('below values are passed - main estate')
            # print(args, bc, store, future, input_tpns_main)

            # In[6]:

            product_dataset_main = volume_transfer_logic(bc, ['Main Estate'], future, input_tpns_main, delist_main)
            product_dataset_main.head()

            # In[7]:

            if input_tpns is None:
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
                input_tpns_exp['base_product_number'] = input_tpns_exp['base_product_number'].str[-8:]
                input_tpns_exp['base_product_number'] = input_tpns_exp['base_product_number'].astype('int')
                delist_exp = input_tpns_exp['base_product_number'].drop_duplicates().values.tolist()

            print('below values are passed - express')
            print(args, bc, store, future, input_tpns_exp)

            # In[8]:

            product_dataset_exp = volume_transfer_logic(bc, ['Express'], future, input_tpns_exp, delist_exp)
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

            # In[12]:

            product_dataset.head()

            # In[13]:
            #waterfall charts
            initial_volume = product_dataset[['productcode', 'predicted_volume']].drop_duplicates().reset_index(
                drop=True)
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

            # In[45]:
            #sales chart
            initial_sales = product_dataset[['productcode', 'predicted_value']].drop_duplicates().reset_index(drop=True)
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

            # In[46]:
            #cgm chart
            initial_cgm = product_dataset[['productcode', 'predicted_cgm']].drop_duplicates().reset_index(drop=True)
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

            # In[47]:
            #cts chart
            initial_cts = product_dataset[['productcode', 'predicted_cts']].drop_duplicates().reset_index(drop=True)
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

            # In[21]:

            # delist product table for UI
            delist_prod_table = product_dataset[['productcode', 'predicted_volume', 'predicted_value', 'predicted_cgm']]
            delist_prod_table = delist_prod_table.drop_duplicates().fillna(0).reset_index(drop=True)
            prod_hrchy = read_frame(product_desc.objects.all().values('base_product_number', 'brand_indicator',
                                                                      'long_description').distinct())

            delist_prod_table = pd.merge(delist_prod_table, prod_hrchy, left_on=['productcode'],
                                         right_on=['base_product_number'], how='left')
            del delist_prod_table['base_product_number']
            delist_prod_table = delist_prod_table.drop_duplicates().fillna(0).reset_index(drop=True)

            delist_prod_table = delist_prod_table.groupby(['productcode', 'long_description'], as_index=False).agg(
                {'predicted_volume': sum, 'predicted_value': sum, 'predicted_cgm': sum})

            # In[22]:

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

            # In[24]:

            # list of prods & their substitutes
            delist_prod_subs = read_frame(
                shelf_review_subs.objects.all().filter(buying_controller__in=bc, store_type__in=['Main Estate'],
                                                       productcode__in=delist_main).values('productcode',
                                                                                           'productdescription',
                                                                                           'substituteproductcode',
                                                                                           'substituteproductdescription'))
            prod_sim = read_frame(
                prod_similarity_subs.objects.all().filter(buying_controller__in=bc, store_type__in=['Main Estate'],
                                                          base_prod__in=delist_main).values('base_prod', 'sub_prod',
                                                                                            'actual_similarity_score'))

            delist_prod_sim = prod_sim.sort_values(['base_prod', 'actual_similarity_score'], ascending=False).groupby(
                'base_prod').head(10)
            delist_prod_sim = pd.merge(delist_prod_sim, prod_hrchy, left_on=['base_prod'],
                                       right_on=['base_product_number'], how='left')
            delist_prod_sim = delist_prod_sim.rename(
                columns={'base_prod': 'productcode', 'long_description': 'productdescription'})
            delist_prod_sim = pd.merge(delist_prod_sim, prod_hrchy, left_on=['sub_prod'],
                                       right_on=['base_product_number'], how='left')
            delist_prod_sim = delist_prod_sim.rename(
                columns={'sub_prod': 'substituteproductcode', 'long_description': 'substituteproductdescription'})
            delist_prod_sim = delist_prod_sim[
                ['productcode', 'productdescription', 'substituteproductcode', 'substituteproductdescription']]

            delist_prod_subs_main = pd.DataFrame()
            delist_prod_subs_main = delist_prod_subs.append(delist_prod_sim)
            delist_prod_subs_main = delist_prod_subs_main[
                -delist_prod_subs_main['substituteproductcode'].isin(delist_main)]
            delist_prod_subs_main = delist_prod_subs_main.drop_duplicates().fillna(0).reset_index(drop=True)

            # In[25]:

            # list of prods & their substitutes
            delist_prod_subs = read_frame(
                shelf_review_subs.objects.all().filter(buying_controller__in=bc, store_type__in=['Express'],
                                                       productcode__in=delist_exp).values('productcode',
                                                                                          'productdescription',
                                                                                          'substituteproductcode',
                                                                                          'substituteproductdescription'))
            prod_sim = read_frame(
                prod_similarity_subs.objects.all().filter(buying_controller__in=bc, store_type__in=['Express'],
                                                          base_prod__in=delist_exp).values('base_prod', 'sub_prod',
                                                                                           'actual_similarity_score'))

            delist_prod_sim = prod_sim.sort_values(['base_prod', 'actual_similarity_score'], ascending=False).groupby(
                'base_prod').head(10)
            delist_prod_sim = pd.merge(delist_prod_sim, prod_hrchy, left_on=['base_prod'],
                                       right_on=['base_product_number'], how='left')
            delist_prod_sim = delist_prod_sim.rename(
                columns={'base_prod': 'productcode', 'long_description': 'productdescription'})
            delist_prod_sim = pd.merge(delist_prod_sim, prod_hrchy, left_on=['sub_prod'],
                                       right_on=['base_product_number'], how='left')
            delist_prod_sim = delist_prod_sim.rename(
                columns={'sub_prod': 'substituteproductcode', 'long_description': 'substituteproductdescription'})
            delist_prod_sim = delist_prod_sim[
                ['productcode', 'productdescription', 'substituteproductcode', 'substituteproductdescription']]

            delist_prod_subs_exp = pd.DataFrame()
            delist_prod_subs_exp = delist_prod_subs.append(delist_prod_sim)
            delist_prod_subs_exp = delist_prod_subs_exp[-delist_prod_subs_exp['substituteproductcode'].isin(delist_exp)]
            delist_prod_subs_exp = delist_prod_subs_exp.drop_duplicates().fillna(0).reset_index(drop=True)

            delist_prod_subs = pd.DataFrame()
            delist_prod_subs = delist_prod_subs.append(delist_prod_subs_main)
            delist_prod_subs = delist_prod_subs.append(delist_prod_subs_exp)

            # In[26]:

            # supplier sales impact - table in UI

            sup_share = read_frame(
                supplier_share.objects.all().filter(buying_controller__in=bc, store_type__in=['Main Estate']).values(
                    'parent_supplier', 'base_product_number', 'volume_share'))
            prod_contri = contribution_main[['productcode', 'predicted_volume']].drop_duplicates().fillna(
                0).reset_index(drop=True)
            prod_contri = prod_contri.rename(columns={'productcode': 'base_product_number'})
            sup_table = pd.merge(sup_share, prod_contri, left_on=['base_product_number'],
                                 right_on=['base_product_number'], how='left')
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
            sup_sales_table_main['value_impact_per_abs'] = sup_sales_table_main['value_impact_per'].abs()
            sup_sales_table_main = sup_sales_table_main.sort_values(['value_impact_per_abs'], ascending=[False])
            del sup_sales_table_main['value_impact_per_abs']

            # In[27]:

            # supplier sales impact - table in UI

            sup_share = read_frame(
                supplier_share.objects.all().filter(buying_controller__in=bc, store_type__in=['Express']).values(
                    'parent_supplier', 'base_product_number', 'volume_share'))
            prod_contri = contribution_exp[['productcode', 'predicted_volume']].drop_duplicates().fillna(
                0).reset_index(drop=True)
            prod_contri = prod_contri.rename(columns={'productcode': 'base_product_number'})
            sup_table = pd.merge(sup_share, prod_contri, left_on=['base_product_number'],
                                 right_on=['base_product_number'], how='left')
            sup_table = sup_table.drop_duplicates().fillna(0).reset_index(drop=True)

            impact_product = product_dataset_exp[['productcode', 'predicted_volume']].drop_duplicates().fillna(
                0).reset_index(drop=True)
            impact_product = impact_product.rename(columns={'predicted_volume': 'predicted_volume_x'})
            sup_table = pd.merge(sup_table, impact_product[['productcode', 'predicted_volume_x']],
                                 left_on=['base_product_number'], right_on=['productcode'], how='left')
            del sup_table['productcode']
            sup_table = sup_table.drop_duplicates().fillna(0).reset_index(drop=True)
            sup_table = sup_table.rename(columns={'predicted_volume_x': 'vols_loss'})

            impact_subs = product_dataset_exp[['substituteproductcode', 'volume_transfer']].groupby(
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
                product_price.objects.filter(buying_controller__in=bc, store_type__in=['Express']).values(
                    'base_product_number', 'asp', 'acp'))
            sup_table = pd.merge(sup_table, prod_price_data[['base_product_number', 'asp']],
                                 left_on=['base_product_number'], right_on=['base_product_number'], how='left')

            sup_table['asp'] = sup_table['asp'].astype('float')

            sup_table['value_gain_share'] = sup_table['vols_gain_share'] * sup_table['asp']
            sup_table['value_loss_share'] = sup_table['vols_loss_share'] * sup_table['asp']
            sup_table['predicted_value_share'] = sup_table['predicted_volume_share'] * sup_table['asp']

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

            sup_sales_table_exp = sup_sales_table[
                ['parent_supplier', 'predicted_volume_share', 'vols_gain_share', 'vols_loss_share', 'vol_impact',
                 'vol_impact_per', 'predicted_value_share', 'value_gain_share', 'value_loss_share', 'value_impact',
                 'value_impact_per']]
            sup_sales_table_exp['vol_impact_per'] = sup_sales_table_exp['vol_impact_per'].round(decimals=1)
            sup_sales_table_exp['value_impact_per'] = sup_sales_table_exp['value_impact_per'].round(decimals=1)
            sup_sales_table_exp = sup_sales_table_exp.drop_duplicates().fillna(0).reset_index(drop=True)

            # In[28]:

            sup_sales_table_exp.head(2)

            # In[29]:

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

            sup_sales_table['value_impact_per_abs'] = sup_sales_table['value_impact_per'].abs()
            sup_sales_table = sup_sales_table.sort_values(['value_impact_per_abs'], ascending=[False])
            del sup_sales_table['value_impact_per_abs']

            # In[32]:

            # ##------- supplier product level impact - pop up in UI------##
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

            supplier_table_popup = pd.DataFrame(sup_product_pop)
            delist_table_popup = pd.DataFrame(delist_prod_subs)

        else:

            if input_tpns is None:
                input_tpns = read_frame(nego_ads_drf.objects.all().filter(buying_controller__in=bc, store_type__in=store,
                                                      performance_quartile__in=['Low CPS/Low Profit'],
                                                      time_period__in=['Last 52 Weeks']).values('base_product_number').distinct())

                delist = list(input_tpns['base_product_number'])
            else:
                delist = input_tpns
                input_tpns = pd.DataFrame(input_tpns).reset_index(drop=True)
                input_tpns['base_product_number'] = input_tpns[0].copy()
                input_tpns['base_product_number'] = input_tpns['base_product_number'].str[-8:]
                input_tpns['base_product_number'] = input_tpns['base_product_number'].astype('int')
                delist = input_tpns['base_product_number'].drop_duplicates().values.tolist()

            print('below values are passed')
            print(args, bc, store, future, input_tpns)

            # In[4]:
            product_dataset = volume_transfer_logic(bc, store, future, input_tpns, delist)

            # In[44]:
            prod_hrchy = read_frame(
                product_desc.objects.all().filter(buying_controller__in=bc).values('base_product_number',
                                                                                   'brand_indicator',
                                                                                   'long_description').distinct())

            initial_volume = product_dataset[['productcode', 'predicted_volume']].drop_duplicates().reset_index(
                drop=True)
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

            # In[45]:

            initial_sales = product_dataset[['productcode', 'predicted_value']].drop_duplicates().reset_index(drop=True)
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

            # In[46]:

            initial_cgm = product_dataset[['productcode', 'predicted_cgm']].drop_duplicates().reset_index(drop=True)
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

            # In[47]:

            initial_cts = product_dataset[['productcode', 'predicted_cts']].drop_duplicates().reset_index(drop=True)
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

            # In[50]:

            # supplier share data
            supp_data = read_frame(supplier_share.objects.all().filter(buying_controller__in=bc, store_type__in=store))

            # delist product table for UI
            delist_prod_table = product_dataset[['productcode', 'predicted_volume', 'predicted_value', 'predicted_cgm']]
            delist_prod_table = delist_prod_table.drop_duplicates().fillna(0).reset_index(drop=True)
            delist_prod_table = pd.merge(delist_prod_table, prod_hrchy, left_on=['productcode'],
                                         right_on=['base_product_number'], how='left')
            del delist_prod_table['base_product_number']

            delist_prod_table = delist_prod_table.groupby(['productcode', 'long_description'], as_index=False).agg(
                {'predicted_volume': sum, 'predicted_value': sum, 'predicted_cgm': sum})
            delist_prod_table = delist_prod_table.drop_duplicates().fillna(0)

            delist_prod_table = pd.merge(delist_prod_table, contribution[['productcode', 'no_of_stores']],
                                         left_on=['productcode'], right_on=['productcode'], how='left')
            delist_prod_table = delist_prod_table[delist_prod_table['no_of_stores'] > 0]
            delist_prod_table = delist_prod_table[delist_prod_table['predicted_volume'] > 0]
            delist_prod_table = delist_prod_table.drop_duplicates().fillna(0).reset_index(drop=True)

            # In[51]:

            # list of prods & their substitutes
            delist_prod_subs = read_frame(
                shelf_review_subs.objects.all().filter(buying_controller__in=bc, store_type__in=store,
                                                       productcode__in=delist).values('productcode',
                                                                                      'productdescription',
                                                                                      'substituteproductcode',
                                                                                      'substituteproductdescription'))
            prod_sim = read_frame(
                prod_similarity_subs.objects.all().filter(buying_controller__in=bc, store_type__in=store,
                                                          base_prod__in=delist).values('base_prod', 'sub_prod',
                                                                                       'actual_similarity_score'))

            delist_prod_sim = prod_sim.sort_values(['base_prod', 'actual_similarity_score'], ascending=False).groupby(
                'base_prod').head(10)
            delist_prod_sim = pd.merge(delist_prod_sim, prod_hrchy, left_on=['base_prod'],
                                       right_on=['base_product_number'], how='left')
            delist_prod_sim = delist_prod_sim.rename(
                columns={'base_prod': 'productcode', 'long_description': 'productdescription'})
            delist_prod_sim = pd.merge(delist_prod_sim, prod_hrchy, left_on=['sub_prod'],
                                       right_on=['base_product_number'], how='left')
            delist_prod_sim = delist_prod_sim.rename(
                columns={'sub_prod': 'substituteproductcode', 'long_description': 'substituteproductdescription'})
            delist_prod_sim = delist_prod_sim[
                ['productcode', 'productdescription', 'substituteproductcode', 'substituteproductdescription']]

            delist_prod_subs = delist_prod_subs.append(delist_prod_sim)
            delist_prod_subs = delist_prod_subs[-delist_prod_subs['substituteproductcode'].isin(delist)]
            delist_prod_subs = delist_prod_subs.drop_duplicates().fillna(0).reset_index(drop=True)

            # In[52]:

            # supplier sales impact - table in UI

            sup_share = read_frame(
                supplier_share.objects.all().filter(buying_controller__in=bc, store_type__in=store).values(
                    'parent_supplier', 'base_product_number', 'volume_share'))
            prod_contri = contribution[['productcode', 'predicted_volume']].drop_duplicates().fillna(0).reset_index(
                drop=True)
            prod_contri = prod_contri.rename(columns={'productcode': 'base_product_number'})
            sup_table = pd.merge(sup_share, prod_contri, left_on=['base_product_number'],
                                 right_on=['base_product_number'], how='left')
            sup_table = sup_table.drop_duplicates().fillna(0).reset_index(drop=True)

            impact_product = product_dataset[['productcode', 'predicted_volume']].drop_duplicates().fillna(
                0).reset_index(drop=True)
            impact_product = impact_product.rename(columns={'predicted_volume': 'predicted_volume_x'})
            sup_table = pd.merge(sup_table, impact_product[['productcode', 'predicted_volume_x']],
                                 left_on=['base_product_number'], right_on=['productcode'], how='left')
            del sup_table['productcode']
            sup_table = sup_table.drop_duplicates().fillna(0)
            sup_table = sup_table.rename(columns={'predicted_volume_x': 'vols_loss'})

            impact_subs = product_dataset[['substituteproductcode', 'volume_transfer']].groupby(
                ['substituteproductcode'], as_index=False).agg({'volume_transfer': sum})
            sup_table = pd.merge(sup_table, impact_subs[['substituteproductcode', 'volume_transfer']],
                                 left_on=['base_product_number'], right_on=['substituteproductcode'], how='left')

            sup_table = sup_table.rename(columns={'volume_transfer': 'vols_gain'})

            del sup_table['substituteproductcode']
            sup_table = sup_table.drop_duplicates().fillna(0)

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

            sup_table = pd.merge(sup_table, prod_price_data[['base_product_number', 'asp']],
                                 left_on=['base_product_number'], right_on=['base_product_number'], how='left')

            sup_table['asp'] = sup_table['asp'].astype('float')

            sup_table['value_gain_share'] = sup_table['vols_gain_share'] * sup_table['asp']
            sup_table['value_loss_share'] = sup_table['vols_loss_share'] * sup_table['asp']
            sup_table['predicted_value_share'] = sup_table['predicted_volume_share'] * sup_table['asp']

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

            sup_sales_table = sup_sales_table[
                ['parent_supplier', 'predicted_volume_share', 'vols_gain_share', 'vols_loss_share', 'vol_impact',
                 'vol_impact_per', 'predicted_value_share', 'value_gain_share', 'value_loss_share', 'value_impact',
                 'value_impact_per']]
            sup_sales_table['vol_impact_per'] = sup_sales_table['vol_impact_per'].round(decimals=1)
            sup_sales_table['value_impact_per'] = sup_sales_table['value_impact_per'].round(decimals=1)
            sup_sales_table = sup_sales_table.drop_duplicates().fillna(0).reset_index(drop=True)

            # In[53]:

            # ##------- supplier product level impact - pop up in UI------##
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

            supplier_table_popup = pd.DataFrame(sup_product_pop)
            delist_table_popup = pd.DataFrame(delist_prod_subs)

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
            'psg_cts_contri': psg_cts_contri
        }
        return JsonResponse(data, safe=False)


#supplier table
class product_impact_supplier_table(APIView):
    def get(self, request, *args):
        global sup_sales_table
        
        sup_sales_table['vol_impact_per'] =  sup_sales_table['vol_impact_per'].round(decimals=1)
        sup_sales_table['value_impact_per'] =  sup_sales_table['value_impact_per'].round(decimals=1)
        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        args.pop('format__iexact', None)

        supplier_search = args.pop('supplier_search__iexact', '')

        if supplier_search is not None:
            supplier_search_table = sup_sales_table[sup_sales_table['parent_supplier'].str.contains(supplier_search, case=False)]

        ## set default delist page as 1
        supplier_page = 1

        ## take page from args if entered
        try:
            supplier_page = int(args.get('supplier_page__iexact'))
        ## else 1
        except:
            supplier_page = 1

        ## remove page number from args
        args.pop('supplier_page__iexact', None)

        ## assign start and end points for subsetting data frame
        start_row = (supplier_page - 1) * 8  ## example: for page 2 => 9
        end_row = start_row + 8

        ## calculate total number of pages
        num_pages = math.ceil((len(supplier_search_table) / 8))
        ## calculate start index for data frame
        start_index = (supplier_page - 1) * 8 + 1
        print(start_index)
        # calculate total number of rows
        count = len(supplier_search_table)
        # calculate end index
        end_index = supplier_page * 8
        ## subset the queryset to display required data
        supplier_search_table = supplier_search_table.loc[start_row:end_row, ]

        data = {
            'sup_sales_table': supplier_search_table.to_dict(orient='records')
        }
        ## passing data in required format
        return JsonResponse({'pagination_count': num_pages,
                             'supplier_page': supplier_page,
                             'start_index': start_index,
                             'count': count,
                             'end_index': end_index,
                             'sup_sales_table': data['sup_sales_table']}, safe=False)


#supplier popup
class supplier_popup(APIView):

    def get(self, request, *args):
        global supplier_table_popup
        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        args.pop('format__iexact', None)
        supplier = args.get('supplier__iexact')
        supplier = [supplier]
        sup_pop = pd.DataFrame(supplier)
        sup_pop['supplier'] = sup_pop[0]
        popup_data1 = pd.merge(supplier_table_popup, sup_pop, left_on=['delist_supplier'], right_on=['supplier'], how='inner')
        popup_data1 = popup_data1.drop_duplicates().fillna(0).reset_index(drop=True)

        ## set default page as 1
        supplier_popup_page = 1

        ## take page from args if entered
        try:
            supplier_popup_page = int(args.get('supplier_popup_page__iexact'))
        ## else 1
        except:
            supplier_popup_page = 1
        ## remove page number from args
        args.pop('supplier_popup_page__iexact', None)

        ## assign start and end points for subsetting data frame
        start_row = (supplier_popup_page - 1) * 8  ## example: for page 2 => 9
        end_row = start_row + 8

        ## calculate total number of pages
        num_pages = math.ceil((len(popup_data1) / 8))
        ## calculate start index for data frame
        start_index = (supplier_popup_page - 1) * 8 + 1
        print(start_index)
        # calculate total number of rows
        count = len(popup_data1)
        # calculate end index
        end_index = supplier_popup_page * 8
        ## subset the queryset to display required data
        popup_data1 = popup_data1.loc[start_row:end_row, ]

        data = {
            'supplier_table_popup': popup_data1.to_dict(orient='records')
        }
        ## passing data in required format
        return JsonResponse({'pagination_count': num_pages,
                             'supplier_popup_page': supplier_popup_page,
                             'start_index': start_index,
                             'count': count,
                             'end_index': end_index,
                             'table': data['supplier_table_popup']}, safe=False)


#delist table
class product_impact_delist_table(APIView):
    def get(self, request, *args):
        global delist_prod_table
        print(delist_prod_table)
        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        args.pop('format__iexact', None)
        args.pop('time_period__iexact',None)

        delist_search = args.pop('delist_search__iexact', '')

        if delist_search is not None:
            delist_search_table = delist_prod_table[delist_prod_table['long_description'].str.contains(delist_search, case=False)]

        ## set default delist page as 1
        delist_page = 1

        ## take page from args if entered
        try:
            delist_page = int(args.get('delist_page__iexact'))
        ## else 1
        except:
            delist_page = 1

        ## remove page number from args
        args.pop('delist_page__iexact', None)

        ## assign start and end points for subsetting data frame
        start_row = (delist_page - 1) * 8  ## example: for page 2 => 9
        end_row = start_row + 8

        ## calculate total number of pages
        num_pages = math.ceil((len(delist_search_table) / 8))
        ## calculate start index for data frame
        start_index = (delist_page - 1) * 8 + 1
        print(start_index)
        # calculate total number of rows
        count = len(delist_search_table)
        # calculate end index
        end_index = delist_page * 8
        ## subset the queryset to display required data
        delist_search_table = delist_search_table.loc[start_row:end_row, ]

        data = {
            'delist_prod_table': delist_search_table.to_dict(orient='records')
        }
        ## passing data in required format
        
        return JsonResponse({'pagination_count': num_pages,
                             'delist_page': delist_page,
                             'start_index': start_index,
                             'count': count,
                             'end_index': end_index,
                             'delist_prod_table': data['delist_prod_table']}, safe=False)


#delist popup
class delist_popup(APIView):

    def get(self, request, *args):
        global delist_table_popup
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


        ## set default page as 1
        delist_popup_page = 1

        ## take page from args if entered
        try:
            delist_popup_page = int(args.get('delist_popup_page__iexact'))
        ## else 1
        except:
            delist_popup_page = 1

        ## remove page number from args
        args.pop('delist_popup_page__iexact', None)

        ## assign start and end points for subsetting data frame
        start_row = (delist_popup_page - 1) * 8  ## example: for page 2 => 9
        end_row = start_row + 8

        ## calculate total number of pages
        num_pages = math.ceil((len(popup_data2) / 8))
        ## calculate start index for data frame
        start_index = (delist_popup_page - 1) * 8 + 1
        print(start_index)
        # calculate total number of rows
        count = len(popup_data2)
        # calculate end index
        end_index = delist_popup_page * 8
        ## subset the queryset to display required data
        popup_data2 = popup_data2.loc[start_row:end_row, ]

        data = {
            'delist_table_popup': popup_data2.to_dict(orient='records')
        }
        ## passing data in required format
        return JsonResponse({'pagination_count': num_pages,
                             'delist_popup_page': delist_popup_page,
                             'start_index': start_index,
                             'count': count,
                             'end_index': end_index,
                             'table': data['delist_table_popup']}, safe=False)


# class classA(APIView):
#     var3 = 4
#     # var4 = 5
#     # def __init__(self):
#     #     classA.var1 = 1
#     #     classA.var2 = 2

#     def methodA(var3):
#         var5 = var3 * 2 
#         # return var5
#     def get(self, request, *args):
#         var1 = 1
#         var2 = 2
#         var4 = var1 + var2 + self.var5
#         return JsonResponse(var4,safe=False)

#     # copy_var2 = var2
#     copy_var4 = methodA()

# object1 = classA()
# # print(classA.var3)
# print(object1.var3)
# print(object1.copy_var4)
# # print(object1.copy_var2)

# class classB(classA):
#     # def __init__(self):
#     #     print(classA.var1)
#     #     print(classA.var3)

#     def get(self, request, *args):
#         calculate = classA.copy_var4 + classA.var3
#         return JsonResponse(calculate,safe=False) 


# object1 = classA()
# sum_total = object1.methodA()
# object2 = classB()
# sum_total1 = object2.methodB()

