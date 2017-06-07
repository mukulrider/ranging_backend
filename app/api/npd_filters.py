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
import environ
import logging


# Models for NPD Opportunity View filters
from .models import pricebucket

# Models for NPD Impact View filters
from .models import npd_supplier_ads,merch_range,input_npd,buyertillroll_input_npd

#for cache
from rest_framework_extensions.cache.decorators import cache_response

# logs
import os
import datetime

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




## NPD Opportunity View Filters(single select)
class opportunity_filters(APIView):
    def get(self, request, format=None):
        #input from header
        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}

        designation = args.pop('designation__iexact',None)
        user_id = args.pop('user_id__iexact',None)
        session_id = args.pop('session_id__iexact',None)
        user_name = args.pop('user_name__iexact', None)
        buying_controller_header = args.pop('buying_controller_header__iexact',None)
        buyer_header = args.pop('buyer_header__iexact',None)
        #header over

        # for admin user (access to 7 buying controllers)
        if  designation=='admin':
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
        print("kwargs_header")
        print(kwargs_header)
        #input from args
        default = args.pop('default__iexact',None)
        if default is None:
            if not args:

                df = read_frame(pricebucket.objects.filter(**kwargs_header).filter(**args))
                heirarchy = read_frame(pricebucket.objects.filter(**kwargs_header).values('buying_controller','buyer','junior_buyer','product_sub_group_description'))

                data ={'buying_controller' : df.buying_controller.unique()}
                bc = pd.DataFrame(data)
                if len(bc)==1:

                    bc['selected']=True
                    bc['disabled'] =False
                else:
                    bc['selected']=False
                    bc['disabled'] =False



                data ={'buyer' : df.buyer.unique()}
                buyer = pd.DataFrame(data)

                if len(buyer)==1:
                    buyer['selected']=True
                    buyer['disabled'] =False
                else:
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

                heirarchy = read_frame(pricebucket.objects.values('buying_controller','buyer','junior_buyer','product_sub_group_description'))

                bc_df = heirarchy[['buying_controller']].drop_duplicates()
                buyer_df = heirarchy[['buyer']].drop_duplicates()
                jr_buyer_df = heirarchy[['junior_buyer']].drop_duplicates()
                psg_df = heirarchy[['product_sub_group_description']].drop_duplicates()

                df = read_frame(pricebucket.objects.filter(**args))


                data ={'buying_controller' : df.buying_controller.unique()}
                bc = pd.DataFrame(data)


                data ={'buyer' : df.buyer.unique()}
                buyer = pd.DataFrame(data)



                data ={'junior_buyer' : df.junior_buyer.unique()}
                jr_buyer = pd.DataFrame(data)

                data ={'product_sub_group_description' : df.product_sub_group_description.unique()}
                psg = pd.DataFrame(data)

                bc['selected']=True
                bc['disabled']=False
                bc_df = pd.merge(bc_df,bc,how='left')
                bc_df['selected'] =bc_df['selected'].fillna(False)
                bc_df['disabled'] =bc_df['disabled'].fillna(True)

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


## NPD Impact View Filters(single select)
class impact_filters(APIView):
    def get(self, request, format=None):

        #input from header
        args = {reqobj + '__iexact': request.GET.get(reqobj) for reqobj in request.GET.keys()}
        designation = args.pop('designation__iexact',None)
        user_id = args.pop('user_id__iexact',None)
        session_id = args.pop('session_id__iexact',None)
        user_name = args.pop('user_name__iexact', None)
        buying_controller_header = args.pop('buying_controller_header__iexact',None)
        buyer_header = args.pop('buyer_header__iexact',None)
        #header over

        # for admin user (access to 7 buying controllers)
        if  designation=='admin':
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

        #input from args
        bc_name = args.get('buying_controller__iexact')
        buyer_name = args.get('buyer__iexact')
        jr_buyer_name = args.get('junior_buyer__iexact')
        psg_name = args.get('product_sub_group_description__iexact')
        brand_id = args.get('brand_name__iexact')

        measure_id = args.get('measure_type__iexact')

        till_roll_id= args.get('till_roll_description__iexact')
        package_id = args.get('package_type__iexact')
        merch_name = args.get('merchandise_group_description__iexact')
        range_name = args.get('range_class__iexact')
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
                            'buyer__iexact': buyer_name,
                            'till_roll_description__iexact' : till_roll_id,

                            }

        kwargs_till_roll = dict(filter(lambda item: item[1] is not None, kwargs_till_roll.items()))

        kwargs_package = {
                        'buying_controller__iexact': bc_name,
                        'package_type__iexact' : package_id,
                        }


        kwargs_package = dict(filter(lambda item: item[1] is not None, kwargs_package.items()))

        kwargs_temp = {
                    'buying_controller__iexact': bc_name,
                    'merchandise_group_description__iexact': merch_name,
                    'range_class__iexact': range_name,
                    }
        kwargs_temp = dict(filter(lambda item: item[1] is not None, kwargs_temp.items()))


        kwargs_supplier = {
                            'buying_controller__iexact': bc_name,
                            'parent_supplier__iexact': supplier_name,

                            }
        kwargs_supplier = dict(filter(lambda item: item[1] is not None, kwargs_supplier.items()))






        if not args:

            heirarchy = read_frame(input_npd.objects.filter(**kwargs_header).filter(**kwargs).values('buying_controller','buyer','junior_buyer','product_sub_group_description','brand_name','package_type',
                            'measure_type'))

            bc_df = heirarchy[['buying_controller']].drop_duplicates()
            buyer_df = heirarchy[['buyer']].drop_duplicates()
            jr_buyer_df = heirarchy[['junior_buyer']].drop_duplicates()
            psg_df = heirarchy[['product_sub_group_description']].drop_duplicates()

            if len(bc_df)==1:
                bc_df['selected'] =True
                bc_df['disabled'] =False
            else:
                bc_df['selected'] =False
                bc_df['disabled'] =False

            bc_df = bc_df.rename(columns={'buying_controller': 'name'})
            if len(buyer_df)==1:
                buyer_df['selected'] =True
                buyer_df['disabled'] =False
            else:
                buyer_df['selected'] =False
                buyer_df['disabled'] =False
            buyer_df = buyer_df.rename(columns={'buyer': 'name'})



            jr_buyer_df['selected'] =False
            jr_buyer_df['disabled'] =False
            jr_buyer_df = jr_buyer_df.rename(columns={'junior_buyer': 'name'})


            psg_df['selected'] =False
            psg_df['disabled'] =False
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


            final_ph = []
            final_ph.append(a)
            final_ph.append(b)
            final_ph.append(c)
            final_ph.append(d)
            final = {}
            final["product_hierarchy"] = final_ph
        else:

            df = read_frame(input_npd.objects.filter(**kwargs))


            hh = read_frame(input_npd.objects.filter(buying_controller__in=df.buying_controller.unique()).values('buying_controller','buyer','junior_buyer','product_sub_group_description','brand_name','package_type',
                            'measure_type'))

            merch_range_df = read_frame(merch_range.objects.filter(buying_controller__in=df.buying_controller.unique()))

            supplier_df = read_frame(npd_supplier_ads.objects.filter(buying_controller__in=df.buying_controller.unique()))




            till_roll_buyer_df = read_frame(buyertillroll_input_npd.objects.filter(buyer__in = df.buyer.unique()))


            # till_roll_buyer_df = pd.read_csv('BUYERTILLROLL_INPUT_NPD.csv')
            # till_roll_buyer_df = till_roll_buyer_df[till_roll_buyer_df['buyer']==buyer_name]


            #merch_range = pd.read_csv('merch_range.csv'


            bc_df = hh[['buying_controller']].drop_duplicates()
            buyer_df = hh[['buyer']].drop_duplicates()
            jr_buyer_df = hh[['junior_buyer']].drop_duplicates()
            psg_df = hh[['product_sub_group_description']].drop_duplicates()
            brand_df = hh[['brand_name']].drop_duplicates()
            package_df = hh[['package_type']].drop_duplicates()
            #till_roll_df = hh[['till_roll_description']].drop_duplicates()
            till_roll_df = till_roll_buyer_df[['till_roll_description']].drop_duplicates()
            measure_df = hh[['measure_type']].drop_duplicates()
            merch_grp_df = merch_range_df[['merchandise_group_description']].drop_duplicates()
            range_class_df = merch_range_df[['range_class']].drop_duplicates()
            supplier_df = supplier_df[['parent_supplier']].drop_duplicates()


            df_temp = read_frame(input_npd.objects.filter(buying_controller__in=df.buying_controller.unique()))

            df_brand_id = read_frame(input_npd.objects.filter(**kwargs_brand).values('brand_name'))

            df_measure_id = read_frame(input_npd.objects.filter(**kwargs_measure).values('measure_type'))

            df_till_roll_id = read_frame(buyertillroll_input_npd.objects.filter(**kwargs_till_roll).values('till_roll_description'))



            df_package_id = read_frame(input_npd.objects.filter(**kwargs_package).values('package_type'))

            df_merch_range =read_frame(merch_range.objects.filter(**kwargs_temp).values('buying_controller','merchandise_group_description','range_class'))

            df_supplier_id = read_frame(npd_supplier_ads.objects.filter(**kwargs_supplier).values('parent_supplier'))




            data ={'buying_controller' : df.buying_controller.unique()}
            bc = pd.DataFrame(data)



            data ={'buyer' : df.buyer.unique()}
            buyer = pd.DataFrame(data)



            data ={'junior_buyer' : df.junior_buyer.unique()}
            jr_buyer = pd.DataFrame(data)




            data ={'product_sub_group_description' : df.product_sub_group_description.unique()}
            psg = pd.DataFrame(data)



            data ={'parent_supplier' : df_supplier_id.parent_supplier.unique()}
            supplier = pd.DataFrame(data)






            data ={'brand_name' : df_brand_id.brand_name.unique()}
            brand = pd.DataFrame(data)




            data ={'package_type' : df_package_id.package_type.unique()}
            package = pd.DataFrame(data)




            data ={'measure_type' : df_measure_id.measure_type.unique()}
            measure = pd.DataFrame(data)




            data ={'till_roll_description' : df_till_roll_id.till_roll_description.unique()}
            till_roll = pd.DataFrame(data)





            data ={'merchandise_group_description' : df_merch_range.merchandise_group_description.unique()}
            merch_grp = pd.DataFrame(data)





            data ={'range_class' : df_merch_range.range_class.unique()}
            range_class = pd.DataFrame(data)




            bc['selected']=True
            bc['disabled']=False
            bc_df = pd.merge(bc_df,bc,how='left')
            bc_df['selected'] =bc_df['selected'].fillna(False)
            bc_df['disabled'] =bc_df['disabled'].fillna(True)

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
                merch_grp_df = merch_grp_df.rename(columns={'merchandise_group_description': 'name'})
            else:
                merch_grp['selected']=False
                merch_grp['disabled']=False
                merch_grp_df = pd.merge(merch_grp_df,merch_grp,how='left')
                merch_grp_df['selected'] =merch_grp_df['selected'].fillna(False)
                merch_grp_df['disabled'] =merch_grp_df['disabled'].fillna(True)
                merch_grp_df = merch_grp_df.rename(columns={'merchandise_group_description': 'name'})



            if len(range_class)==1:
                range_class['selected']=True
                range_class['disabled']=False
                range_class_df = pd.merge(range_class_df,range_class,how='left')
                range_class_df['selected'] =range_class_df['selected'].fillna(False)
                range_class_df['disabled'] =range_class_df['disabled'].fillna(True)
                range_class_df = range_class_df.rename(columns={'range_class': 'name'})
            else:
                range_class['selected']=False
                range_class['disabled']=False
                range_class_df = pd.merge(range_class_df,range_class,how='left')
                range_class_df['selected'] =range_class_df['selected'].fillna(False)
                range_class_df['disabled'] =range_class_df['disabled'].fillna(True)
                range_class_df = range_class_df.rename(columns={'range_class': 'name'})




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

            supplier_df['name_supplier'] = supplier_df['name'].str.split('-').str[1]
            supplier_df = supplier_df.sort_values(by='name_supplier', ascending=True)
            del supplier_df['name_supplier']
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


            k = {}
            k['name']='measure_type'
            k['items']=measure_final

            till_roll_df = till_roll_df.sort_values(by='name',ascending=True)
            tillroll_final = till_roll_df.to_json(orient='records')
            tillroll_final = json.loads(tillroll_final)


            h = {}
            h['name']='till_roll_description'
            h['items']=tillroll_final



            merch_grp_df = merch_grp_df.sort_values(by='name',ascending=True)
            merch_final = merch_grp_df.to_json(orient='records')
            merch_final = json.loads(merch_final)


            i = {}
            i['name']='merchandise_group_description'
            i['items']=merch_final


            range_class_df = range_class_df.sort_values(by='name',ascending=True)
            range_class_final = range_class_df.to_json(orient='records')
            range_class_final = json.loads(range_class_final)


            j = {}
            j['name']='range_class'
            j['items']=range_class_final



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



