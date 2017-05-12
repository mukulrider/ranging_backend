from rest_framework import serializers
from .models import unmatchedprod,nego_ads_drf,npd_supplier_ads,SaveScenario
#models for product impact
from .models import delist_scenario

class unmatchedprodSerializer(serializers.ModelSerializer):
	class Meta:
		model = unmatchedprod
		fields = ('competitor_product_desc','retailer','asp')

class negochartsSerializer(serializers.ModelSerializer):

	class Meta:
		model = nego_ads_drf
		fields = ('store_type','base_product_number', 'long_description', 'cps','pps', 'subs_count', 'sales_value', 'sales_volume', 'cogs_value', 'rate_of_sale','store_count','rsp')

class npd_impact_tableSerializer(serializers.ModelSerializer):
	class Meta:
		model = npd_supplier_ads
		fields = ('base_product_number','long_description','pps','cps','store_count','rate_of_sale')



class npd_SaveScenarioSerializer(serializers.ModelSerializer):
	class Meta:
		model = SaveScenario
		fields = ('system_time','scenario_name','scenario_tag')


class npd_ViewScenarioSerializer(serializers.ModelSerializer):
	class Meta:
		model = SaveScenario
		fields = ('forecast_data','similar_products','page')


#product impact
class delist_savescenarioserializer(serializers.ModelSerializer):
	class Meta:
		model = delist_scenario
		fields = ('system_time', 'scenario_name', 'user_name')
