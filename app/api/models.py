from __future__ import unicode_literals
from django.utils.encoding import python_2_unicode_compatible
import uuid
from django.db import models
from django.conf import settings
from datetime import date


class outperformance(models.Model):
    category_director = models.TextField('category_director',max_length=100, blank=True, null=True)
    buying_controller = models.TextField('buying_controller',max_length=100, blank=True, null=True)
    buyer = models.TextField('buyer',max_length=100, blank=True, null=True)
    junior_buyer = models.TextField('junior_buyer',max_length=100, blank=True, null=True)
    product_sub_group_code = models.TextField('product_sub_group_code',max_length=100, blank=True, null=True)
    week_flag = models.TextField('output_date',max_length=100, blank=True, null=True)
    update_date = models.DateField('update_date')
    week_number = models.IntegerField('week_number', blank=True, null=True)
    tesco_outperformanc_percentage = models.DecimalField('tesco_outperformanc_percentage', max_digits=20, decimal_places=10, default=0.0)
    tesco_outperformanc_unit_prcnt = models.DecimalField('tesco_outperformanc_unit_prcnt', max_digits=20, decimal_places=10, default=0.0)
    product_sub_group_description = models.TextField('product_sub_group_description',max_length=100, blank=True, null=True)

    def __str__(self):
        return '%s' % (self.buying_controller)

class pricebucket(models.Model):
    product_sub_group_code = models.TextField('product_sub_group_code',max_length=100, blank=True, null=True)
    category_director = models.TextField('category_director',max_length=100, blank=True, null=True)
    buying_controller = models.TextField('buying_controller',max_length=100, blank=True, null=True)
    buyer = models.TextField('buyer',max_length=100, blank=True, null=True)
    junior_buyer = models.TextField('junior_buyer',max_length=100, blank=True, null=True)
    retailer = models.TextField('retailer',max_length=100, blank=True, null=True)
    week_flag = models.TextField('week_flag',max_length=100, blank=True, null=True)
    sku = models.IntegerField('sku', blank=True, null=True)
    sku_gravity = models.IntegerField('sku_gravity', blank=True, null=True)
    product_sub_group_description = models.TextField('product_sub_group_description',max_length=100, blank=True, null=True)
    price_gravity = models.TextField('price_gravity',max_length=100, blank=True, null=True)
    
    def __str__(self):
      return '%s' % (self.buying_controller)


class unmatchedprod(models.Model):
    competitor_id = models.IntegerField('competitor_id', blank=True, null=True)
    competitor_product_sid = models.IntegerField('competitor_product_sid', blank=True, null=True)
    week_flag = models.TextField('week_flag',max_length=100, blank=True, null=True)
    product_sub_group_code = models.TextField('product_sub_group_code',max_length=100, blank=True, null=True)
    category_director = models.TextField('category_director',max_length=100, blank=True, null=True)
    buying_controller = models.TextField('buying_controller',max_length=100, blank=True, null=True)
    buyer = models.TextField('buyer',max_length=100, blank=True, null=True)
    junior_buyer = models.TextField('junior_buyer',max_length=100, blank=True, null=True)
    product_sub_group_description = models.TextField('product_sub_group_description',max_length=100, blank=True, null=True)
    retailer = models.TextField('retailer',max_length=100, blank=True, null=True)
    competitor_product_desc = models.TextField('competitor_product_desc',max_length=100, blank=True, null=True)
    asp = models.DecimalField('asp', max_digits=20, decimal_places=2, default=0.0)
    
    def __str__(self):
      return '%s' % (self.buying_controller)

#### delist product impact & negotiation page 
class pps_ros_quantile(models.Model):
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    base_product_number = models.IntegerField('base_product_number', blank=True, null=True)
    store_type = models.TextField('store_type', max_length=100, blank=True, null=True)
    pps_ros_quantile = models.TextField('pps_ros_quantile', max_length=100, blank=True, null=True)
    pps_quantile = models.TextField('pps_quantile', max_length=100, blank=True, null=True)
    ros_quantile = models.TextField('ros_quantile', max_length=100,blank=True,null=True)

    def __str__(self):
        return '%s' % (self.base_product_number)

class cts_data(models.Model):
    base_product_number = models.IntegerField('base_product_number', blank=True, null=True)
    long_description = models.TextField('long_description', max_length=100, blank=True, null=True)
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    store_type = models.TextField('store_type',max_length=100, blank=True, null=True)
    cts_per_unit = models.DecimalField('cts_per_unit', max_digits=15, decimal_places=2, default=0.0)
    def __str__(self):
        return '%s' % (self.base_product_number)


class product_hierarchy(models.Model):
    commercial_director = models.TextField('commercial_director',max_length=100, blank=True, null=True)
    category_director = models.TextField('category_director',max_length=100, blank=True, null=True)
    buying_controller = models.TextField('buying_controller',max_length=100, blank=True, null=True)
    buyer = models.TextField('buyer',max_length=100, blank=True, null=True)
    junior_buyer = models.TextField('junior_buyer', max_length=200,blank=True, null=True)
    product_sub_group_code = models.TextField('product_sub_group_code',max_length=200,blank=True, null=True)
    product_sub_group_description = models.TextField('product_sub_group_description',max_length=200,blank=True, null=True)
    base_product_number = models.IntegerField('base_product_number', blank=True, null=True)
    long_description = models.TextField('long_description',max_length=200, blank=True)
    brand_indicator = models.TextField('brand_indicator',max_length=100, blank=True, null=True)
    brand_name = models.TextField('brand_name',max_length=200, blank=True, null=True)
    parent_supplier = models.TextField('parent_supplier', max_length=100, blank=True, null=True)
    def __str__(self):
        return '%s' % (self.base_product_number)

class product_price(models.Model):
    buying_controller = models.TextField('buying_controller',max_length=100, blank=True, null=True)
    store_type = models.TextField('store_type',max_length=100, blank=True, null=True)
    base_product_number = models.IntegerField('base_product_number', blank=True, null=True)
    brand_indicator = models.TextField('brand_indicator',max_length=100, blank=True, null=True)
    asp = models.DecimalField('asp', blank=True, max_digits=20, decimal_places=3, default=0.0)
    acp = models.DecimalField('acp', blank=True, max_digits=20, decimal_places=3, default=0.0)
    
    def __str__(self):
        return '%s' % (self.base_product_number)


class shelf_review_subs(models.Model):
    buying_controller = models.TextField('buying_controller',max_length=100, blank=True, null=True)
    store_type = models.TextField('store_type',max_length=100, blank=True, null=True)
    productcode = models.IntegerField('productcode', blank=True, null=True)
    productdescription = models.TextField('productdescription',max_length=100, blank=True, null=True)
    substituteproductcode = models.IntegerField('substituteproductcode', blank=True, null=True)
    substituteproductdescription = models.TextField('substituteproductdescription',max_length=100, blank=True, null=True)
    substitutescore = models.DecimalField('substitutescore', blank=True, max_digits=20, decimal_places=4, default=0.0)
    tcs_per = models.DecimalField('tcs_per', blank=True, max_digits=20, decimal_places=4, default=0.0)
    exclusivity_per = models.DecimalField('exclusivity_per', blank=True, max_digits=20, decimal_places=4, default=0.0)
    def __str__(self):
        return '%s' % (self.productcode)


class prod_similarity_subs(models.Model):
    buying_controller = models.TextField('buying_controller',max_length=100, blank=True, null=True)
    store_type = models.TextField('store_type',max_length=100, blank=True, null=True)
    base_prod = models.IntegerField('base_prod', blank=True, null=True)
    sub_prod = models.IntegerField('sub_prod', blank=True, null=True)
    actual_similarity_score = models.DecimalField('actual_similarity_score', blank=True, max_digits=20, decimal_places=4, default=0.0)
    similarity_score = models.DecimalField('similarity_score', blank=True, max_digits=20, decimal_places=4, default=0.0)
    def __str__(self):
        return '%s' % (self.base_prod)



class supplier_share(models.Model):
    buying_controller = models.TextField('buying_controller',max_length=100, blank=True, null=True)
    store_type = models.TextField('store_type',max_length=100, blank=True, null=True)
    parent_supplier = models.TextField('parent_supplier',max_length=100, blank=True, null=True)
    base_product_number = models.IntegerField('base_product_number', blank=True, null=True)
    brand_indicator = models.TextField('brand_indicator',max_length=100, blank=True, null=True)
    volume_share = models.DecimalField('volume_share', blank=True, max_digits=20, decimal_places=4, default=0.0)
    def __str__(self):
        return '%s' % (self.base_product_number)

class product_impact_filter(models.Model):
    bc = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    store = models.TextField('store_type', max_length=100, blank=True, null=True)
    future = models.TextField('time_period', max_length=100, blank=True, null=True)
    input_tpns = models.IntegerField('base_product_number', blank=True, null=True)
    def __str__(self):
        return '%s' % (self.input_tpns)


class nego_ads_drf(models.Model):
    buying_controller = models.TextField('buying_controller',max_length=100, blank=True, null=True)
    buyer = models.TextField('buyer', max_length=100, blank=True, null=True)
    junior_buyer = models.TextField('junior_buyer', max_length=100, blank=True, null=True)
    brand_indicator = models.TextField('brand_indicator', max_length=100, blank=True, null=True)
    brand_name = models.TextField('brand_name', max_length=100, blank=True, null=True)
    product_sub_group_code = models.TextField('product_sub_group_code',max_length=100, blank=True, null=True)
    product_sub_group_description = models.TextField('product_sub_group_description',max_length=100, blank=True, null=True)
    base_product_number = models.IntegerField('base_product_number', blank=True, null=True)
    long_description = models.TextField('long_description',max_length=100, blank=True, null=True)
    store_type = models.TextField('store_type',max_length=100, blank=True, null=True)
    store_count = models.IntegerField('store_count', blank=True, null=True)
    subs_count = models.IntegerField('subs_count', blank=True, null=True)
    sales_value = models.DecimalField('sales_value', blank=True, max_digits=20, decimal_places=2, default=0.0)
    sales_volume = models.DecimalField('sales_volume', blank=True, max_digits=20, decimal_places=2, default=0.0)
    cogs_value = models.DecimalField('cogs_value', blank=True, max_digits=20, decimal_places=2, default=0.0)
    rsp = models.DecimalField('rsp', blank=True, max_digits=20, decimal_places=2, default=0.0)
    cgm_value = models.DecimalField('cgm_value', blank=True, max_digits=20, decimal_places=2, default=0.0)
    need_state = models.TextField('need_state', max_length=100, blank=True, null=True)
    time_period = models.TextField('time_period', max_length=100, blank=True, null=True)
    cps = models.DecimalField('cps', blank=True, max_digits=20, decimal_places=2, default=0.0)
    pps = models.DecimalField('pps', blank=True, max_digits=20, decimal_places=2, default=0.0)
    rate_of_sale = models.DecimalField('rate_of_sale', blank=True, max_digits=20, decimal_places=2, default=0.0)
    cps_quartile = models.DecimalField('cps_quartile', blank=True, max_digits=5, decimal_places=2, default=0.0)
    pps_quartile = models.DecimalField('pps_quartile', blank=True, max_digits=5, decimal_places=2, default=0.0)
    performance_quartile = models.TextField('performance_quartile', max_length=100, blank=True, null=True)
    def __str__(self):
        return '%s' % (self.base_product_number)


class product_desc(models.Model):
    buying_controller = models.TextField('buying_controller',max_length=100, blank=True, null=True)
    base_product_number = models.IntegerField('base_product_number', blank=True, null=True)
    long_description = models.TextField('long_description',max_length=100, blank=True, null=True)
    brand_indicator = models.TextField('brand_indicator',max_length=100, blank=True, null=True)
    def __str__(self):
        return '%s' % (self.base_product_number)


class product_contri(models.Model):
    buying_controller = models.TextField('buying_controller',max_length=100, blank=True, null=True)
    store_type = models.TextField('store_type',max_length=100, blank=True, null=True)
    base_product_number = models.IntegerField('base_product_number', blank=True, null=True)
    predicted_volume = models.DecimalField('predicted_volume', blank=True, max_digits=20, decimal_places=2, default=0.0)
    time_period = models.TextField('time_period',max_length=200, blank=True)
    no_of_stores = models.IntegerField('no_of_stores', blank=True, null=True)

    def __str__(self):
        return '%s' % (self.base_product_number)


#### Holiday flag as 1 for weeks and store type
class uk_holidays (models.Model):
    area_price_code = models.IntegerField('area_price_code', blank=True, null=True)
    year_week_number = models.IntegerField('year_week_number', blank=True, null=True)
    holiday_flag = models.IntegerField('holiday_flag', blank=True, null=True)
    store_type  = models.TextField('store_type',max_length=100, blank=True, null=True)

    def __str__(self):
        return '%s' % (self.area_price_code)

#### Have the si for week and psg
class seasonality_index (models.Model):
    weeks   = models.IntegerField('year_week_number', blank=True, null=True)
    psg  = models.TextField('psg',max_length=100, blank=True, null=True)
    wkly_avg_ind = models.DecimalField('wkly_avg_ind', max_digits=20, decimal_places=10, default=0.0)
    adjusted_index = models.DecimalField('adjusted_index', max_digits=20, decimal_places=10, default=0.0)

    def __str__(self):
        return '%s' % (self.psg)

class npd_calendar(models.Model):
    calendar_date = models.DateField(blank=True,null=True)
    year_week_number = models.IntegerField('year_week_number')
    year_number = models.IntegerField('year_number')
    quarter_number = models.IntegerField('quarter_number')
    period_number = models.IntegerField('period_number')
    week_number = models.IntegerField('week_number')
    day_number = models.IntegerField('day_number')
    day_text = models.TextField('day_text')
    period_week_number = models.IntegerField('period_week_number')
    tesco_wf = models.IntegerField('tesco_wf')
    tesco_pf = models.IntegerField('tesco_pf')
    commercial_tesco_wf = models.IntegerField('commercial_tesco_wf')
    commercial_tesco_pf = models.IntegerField('commercial_tesco_pf')
    commercial_day_number = models.IntegerField('commercial_day_number')
    
    def __str__(self):  
        return '%s' % (self.year_week_number)

class input_npd (models.Model):
    buying_controller = models.TextField('buying_controller',max_length=100, blank=True, null=True)
    buyer   = models.TextField('buyer',max_length=100, blank=True, null=True)
    junior_buyer    = models.TextField('junior_buyer',max_length=100, blank=True, null=True)
    product_sub_group_description = models.TextField('product_sub_group_description',max_length=200, blank=True, null=True)
    product_sub_group_code = models.TextField('product_sub_group_code',max_length=100, blank=True, null=True)
    brand_name  =  models.TextField('brand_name',max_length=100, blank=True, null=True)
    package_type    = models.TextField('package_type',max_length=100, blank=True, null=True)
    till_roll_description = models.TextField('till_roll_description',max_length=200, blank=True, null=True)
    area_price_code = models.IntegerField('area_price_code', blank=True, null=True)
    measure_type    = models.TextField('measure_type',max_length=100, blank=True, null=True)
    store_type  = models.TextField('store_type',max_length=100, blank=True, null=True)
    price_band  = models.TextField('price_band',max_length=100, blank=True, null=True)
    curr_week_number = models.IntegerField('curr_week_number', blank=True, null=True)
    period_number = models.IntegerField('period_number', blank=True, null=True)
    period_week_number = models.IntegerField('period_week_number', blank=True, null=True)
    quarter_number = models.IntegerField('quarter_number', blank=True, null=True)

    def __str__(self):
        return '%s' % (self.product_sub_group_description)

class attribute_score_allbc (models.Model):
    bc   = models.TextField('bc',max_length=100, blank=True, null=True)
    product_sub_group_code = models.TextField('product_sub_group_code',max_length=100, blank=True, null=True)
    avg_brand   = models.DecimalField('avg_brand', max_digits=20, decimal_places=10, default=0.0)
    avg_pkg = models.DecimalField('avg_pkg', max_digits=20, decimal_places=10, default=0.0)
    avg_size    = models.DecimalField('avg_size', max_digits=20, decimal_places=10, default=0.0)
    avg_price   = models.DecimalField('avg_price', max_digits=20, decimal_places=10, default=0.0)
    avg_tillroll = models.DecimalField('avg_tillroll', max_digits=20, decimal_places=10, default=0.0)
    

    def __str__(self):
        return '%s' % (self.bc)

class bc_allprod_attributes (models.Model):
    bc = models.TextField('bc',max_length=100, blank=True, null=True)
    base_product_number = models.IntegerField('base_product_number', blank=True, null=True)
    product_sub_group_description = models.TextField('product_sub_group_description',max_length=200, blank=True, null=True)
    brand_name  =  models.TextField('brand_name',max_length=100, blank=True, null=True)
    package_type    = models.TextField('package_type',max_length=100, blank=True, null=True)
    till_roll_description = models.TextField('till_roll_description',max_length=200, blank=True, null=True)
    size    = models.DecimalField('size', max_digits=20, decimal_places=10, default=0.0, null=True)
    measure_type    = models.TextField('measure_type',max_length=100, blank=True, null=True)
    price_band  = models.TextField('price_band',max_length=100, blank=True, null=True)
    long_description = models.TextField('long_description',max_length=200, blank=True, null=True)
    brand_grp20 = models.IntegerField('brand_grp20', blank=True, null=True)
    brand_ind   = models.TextField('brand_ind',max_length=100, blank=True, null=True)
    launch_tesco_week   = models.IntegerField('launch_tesco_week', blank=True, null=True)
    
    def __str__(self):
        return '%s' % (self.base_product_number)

class consolidated_buckets(models.Model):
    buying_controller = models.TextField('buying_controller',max_length=100, blank=True, null=True)
    bucket_value = models.TextField('bucket_value',max_length=100, blank=True, null=True)
    cannibalization = models.DecimalField('cannibalization', blank=True, max_digits=20, decimal_places=2, default=0.0)
    bucket_flag = models.TextField('bucket_flag',max_length=100, blank=True, null=True)
    
    def __str__(self):
        return '%s' % (self.buying_controller)

class consolidated_calculated_cannibalization(models.Model):
    buying_controller = models.TextField('buying_controller',max_length=100, blank=True, null=True)
    base_product_number = models.IntegerField('base_product_number', blank=True, null=True)
    cannibalization = models.DecimalField('cannibalization', blank=True, max_digits=20, decimal_places=2, default=0.0)
    brand_ind = models.TextField('brand_ind',max_length=100, blank=True, null=True)
    def __str__(self):
        return '%s' % (self.buying_controller)
        
class range_space_store_future(models.Model):
    retail_outlet_number = models.IntegerField('retail_outlet_number', blank=True, null=True)
    record_type = models.TextField('record_type',max_length=100, blank=True, null=True)
    merchandise_group_code = models.TextField('merchandise_group_code',max_length=100, blank=True, null=True)
    start_date = models.DateField(blank=True,null=True) 
    store_cluster_id =  models.DecimalField('store_cluster_id', blank=True, max_digits=20, decimal_places=2, default=0.0)
    equipment_type = models.TextField('equipment_type',max_length=100, blank=True, null=True)
    equipment_type_single_code = models.TextField('equipment_type_single_code',max_length=100, blank=True, null=True)   
    range_space_break_code = models.TextField('range_space_break_code',max_length=100, blank=True, null=True)   
    def __str__(self):
        return '%s' % (self.range_space_break_code) 
    
class store_details(models.Model):
    retail_outlet_number = models.IntegerField('retail_outlet_number', blank=True, null=True)
    area_price_code = models.IntegerField('area_price_code', blank=True, null=True)
    pfs_store = models.DecimalField('pfs_store', blank=True, max_digits=20, decimal_places=2, default=0.0)
    store_5k = models.DecimalField('store_5k', blank=True, max_digits=20, decimal_places=2, default=0.0)
    store_20k = models.DecimalField('store_20k', blank=True, max_digits=20, decimal_places=2, default=0.0)
    store_50k = models.DecimalField('store_50k', blank=True, max_digits=20, decimal_places=2, default=0.0)
    store_100k = models.DecimalField('store_100k', blank=True, max_digits=20, decimal_places=2, default=0.0)
    store_100kplus = models.DecimalField('store_100kplus', blank=True, max_digits=20, decimal_places=2, default=0.0)
    def __str__(self):
        return '%s' % (self.retail_outlet_number)
    
class features_allbc(models.Model):
    beers = models.TextField('beers',max_length=100, blank=True, null=True)
    spirits = models.TextField('spirits',max_length=100, blank=True, null=True) 
    wines = models.TextField('wines',max_length=100, blank=True, null=True) 
    frozenimpulse = models.TextField('frozenimpulse',max_length=100, blank=True, null=True) 
    meatfishandveg = models.TextField('meatfishandveg',max_length=100, blank=True, null=True)   
    freshbakery = models.TextField('freshbakery',max_length=100, blank=True, null=True) 
    plantbakery = models.TextField('plantbakery',max_length=100, blank=True, null=True) 
    baby = models.TextField('baby',max_length=100, blank=True, null=True)   
    bbtmisc = models.TextField('bbtmisc',max_length=100, blank=True, null=True) 
    beauty = models.TextField('beauty',max_length=100, blank=True, null=True)   
    healthcare = models.TextField('healthcare',max_length=100, blank=True, null=True)   
    opticians = models.TextField('opticians',max_length=100, blank=True, null=True) 
    toiletries = models.TextField('toiletries',max_length=100, blank=True, null=True)   
    cheeseyoghdesrt = models.TextField('cheeseyoghdesrt',max_length=100, blank=True, null=True) 
    coldeat = models.TextField('coldeat',max_length=100, blank=True, null=True) 
    milk = models.TextField('milk',max_length=100, blank=True, null=True)   
    readymeals = models.TextField('readymeals',max_length=100, blank=True, null=True)   
    cleaninglaundry = models.TextField('cleaninglaundry',max_length=100, blank=True, null=True) 
    paperandhomeessen = models.TextField('paperandhomeessen',max_length=100, blank=True, null=True) 
    petcare = models.TextField('petcare',max_length=100, blank=True, null=True) 
    biscuits = models.TextField('biscuits',max_length=100, blank=True, null=True)   
    chilledjuice = models.TextField('chilledjuice',max_length=100, blank=True, null=True)   
    csn = models.TextField('csn',max_length=100, blank=True, null=True) 
    softdrinks = models.TextField('softdrinks',max_length=100, blank=True, null=True)   
    pigmeat = models.TextField('pigmeat',max_length=100, blank=True, null=True) 
    poultryandeggs = models.TextField('poultryandeggs',max_length=100, blank=True, null=True)   
    foodtorder = models.TextField('foodtorder',max_length=100, blank=True, null=True)   
    cereals = models.TextField('cereals',max_length=100, blank=True, null=True) 
    canned = models.TextField('canned',max_length=100, blank=True, null=True)   
    hotandsweet = models.TextField('hotandsweet',max_length=100, blank=True, null=True) 
    foodoftheworld = models.TextField('foodoftheworld',max_length=100, blank=True, null=True)   
    horticulture = models.TextField('horticulture',max_length=100, blank=True, null=True)   
    confectionery = models.TextField('confectionery',max_length=100, blank=True, null=True) 
    fish = models.TextField('fish',max_length=100, blank=True, null=True)   
    redmeat = models.TextField('redmeat',max_length=100, blank=True, null=True) 
    fruit = models.TextField('fruit',max_length=100, blank=True, null=True) 
    prepared = models.TextField('prepared',max_length=100, blank=True, null=True)   
    producenons = models.TextField('producenons',max_length=100, blank=True, null=True) 
    salads = models.TextField('salads',max_length=100, blank=True, null=True)   
    vegetables = models.TextField('vegetables',max_length=100, blank=True, null=True)
    def __str__(self):
        return '%s' % (self.beers)
        
class merch_range(models.Model):
    buying_controller = models.TextField('buying_controller',max_length=100, blank=True, null=True)
    merchandise_group_code = models.TextField('merchandise_group_code',max_length=100, blank=True, null=True)
    range_space_break_code = models.TextField('range_space_break_code',max_length=100, blank=True, null=True) 
    merchandise_group_code_description = models.TextField('merchandise_group_code_description',max_length=100, blank=True, null=True)   
    def __str__(self):
        return '%s' % (self.merchandise_group_code_description)
        

        
#################################To get new heirarchy details for npd###########################################################################

class npd_product_hierarchy(models.Model):
    buying_controller = models.TextField('buying_controller',max_length=100, blank=True, null=True)
    buyer = models.TextField('buyer',max_length=100, blank=True, null=True)
    junior_buyer = models.TextField('junior_buyer', max_length=200,blank=True, null=True)
    product_sub_group_code = models.TextField('product_sub_group_code',max_length=200,blank=True, null=True)
    product_sub_group_description = models.TextField('product_sub_group_description',max_length=200,blank=True, null=True)

    def __str__(self):
        return '%s' % (self.product_sub_group_code)

class npd_supplier_ads(models.Model):
    buying_controller = models.TextField('buying_controller',max_length=100, blank=True, null=True)
    parent_supplier = models.TextField('parent_supplier',max_length=100, blank=True, null=True)
    product_sub_group_code = models.TextField('product_sub_group_code',max_length=100, blank=True, null=True)
    product_subgroup = models.TextField('product_subgroup',max_length=100, blank=True, null=True)
    base_product_number = models.IntegerField('base_product_number', blank=True, null=True)
    long_description = models.TextField('long_description',max_length=100, blank=True, null=True)
    brand_indicator = models.TextField('brand_indicator',max_length=100, blank=True, null=True)
    store_type = models.TextField('store_type',max_length=100, blank=True, null=True)
    store_count = models.IntegerField('store_count', blank=True, null=True)
    sales_value = models.DecimalField('sales_value', blank=True, max_digits=20, decimal_places=2, default=0.0)
    sales_volume = models.DecimalField('sales_volume', blank=True, max_digits=20, decimal_places=2, default=0.0)
    cogs_value = models.DecimalField('cogs_value', blank=True, max_digits=20, decimal_places=2, default=0.0)
    rsp = models.DecimalField('rsp', blank=True, max_digits=20, decimal_places=4, default=0.0)
    cgm_value = models.DecimalField('cgm_value', blank=True, max_digits=20, decimal_places=2, default=0.0)
    cps = models.DecimalField('cps', blank=True, max_digits=20, decimal_places=2, default=0.0)
    pps = models.DecimalField('pps', blank=True, max_digits=20, decimal_places=2, default=0.0)
    cps_quartile = models.DecimalField('cps_quartile', blank=True, max_digits=5, decimal_places=2, default=0.0)
    pps_quartile = models.DecimalField('pps_quartile', blank=True, max_digits=5, decimal_places=2, default=0.0)
    performance_quartile = models.TextField('performance_quartile', max_length=100, blank=True, null=True)
    rate_of_sale = models.DecimalField('rate_of_sale', blank=True, max_digits=20, decimal_places=2, default=0.0)
    time_period = models.TextField('time_period', max_length=100, blank=True, null=True)
    buyer = models.TextField('buyer', max_length=100, blank=True, null=True)
    junior_buyer = models.TextField('junior_buyer', max_length=100, blank=True, null=True)
    

    def __str__(self):
                    return '%s' % (self.base_product_number)

class brand_grp_mapping(models.Model):
    buying_controller = models.TextField('buying_controller',max_length=100, blank=True, null=True)
    brand_name  = models.TextField('brand_name',max_length=100, blank=True, null=True)
    brand_grp20 = models.IntegerField('brand_grp20', blank=True, null=True)
    brand_ind = models.TextField('brand_ind',max_length=100, blank=True, null=True)

    def __str__(self):
                    return '%s' % (self.brand_name)

class SaveScenario(models.Model):
    
    #user id 
    user_id = models.CharField('user_id',max_length=100, default="none")# event = models.ForeignKey(Event, related_name='event', on_delete=models.CASCADE)
    #user_name
    user_name = models.CharField('user_name',max_length=100, default="none")
    #designation
    designation = models.TextField('designation', max_length=100, blank=True, null=True)
    #session id from front end 
    session_id = models.CharField('session_id',max_length=100, default="none")    
    #scenario name from front end
    scenario_name = models.TextField('scenario_name', max_length=100, blank=True, null=True)
    #product name 
    scenario_tag = models.TextField('sceanrio_tag',max_length=100, blank=True, null=True)
    #week flag
    week_tab = models.IntegerField('week_tab', blank=True, null=True)
    #Buying_controller from frontend
    buying_controller = models.TextField('buying_controller',max_length=100, blank=True, null=True)
    #Supplier from frontend
    parent_supplier = models.TextField('parent_supplier',max_length=100, blank=True, null=True)
    #Buyer from frontend
    buyer = models.TextField('buyer',max_length=100, blank=True, null=True)
    #user input from front end 
    user_attributes = models.TextField('user_attributes', default='')
    #asp 
    asp = models.DecimalField('asp', blank=True, max_digits=20, decimal_places=4, default=0.0)
    #forecast generated 
    forecast_data = models.TextField('forecast_data', default='') 
    #value_forecast
    value_forecast = models.DecimalField('value_forecast', blank=True, max_digits=20, decimal_places=4, default=0.0)
    #value_impact
    value_impact = models.DecimalField('value_impact', blank=True, max_digits=20, decimal_places=4, default=0.0)
    # value_cannibalized
    value_cannibalized = models.DecimalField('value_cannibalized', blank=True, max_digits=20, decimal_places=4, default=0.0)
    #volume_forecast
    volume_forecast = models.DecimalField('volume_forecast', blank=True, max_digits=20, decimal_places=4, default=0.0)
    #volume_impact
    volume_impact = models.DecimalField('volume_impact', blank=True, max_digits=20, decimal_places=4, default=0.0)
    # volume_cannibalized
    volume_cannibalized = models.DecimalField('volume_cannibalized', blank=True, max_digits=20, decimal_places=4, default=0.0)
    #similar products table 
    similar_products = models.TextField('similar_products',default = '')
    #modified forecast from front end 
    modified_flag = models.IntegerField('modified_flag', blank=True, null=True)
    #time of creation
    system_time = models.DateField('system_time',auto_now=False, auto_now_add=False,default='')
    page = models.TextField('page',default = '')
    

class ScenarioTracker(models.Model):
    
    user_id = models.CharField('user_id',max_length=100, default="none")
    session_id = models.CharField('user_id',max_length=100, default="none")
    scenario_name = models.TextField('scenario_name', max_length=100, blank=True, null=True)
    modified_scenario = models.IntegerField('modified_flag', blank=True, null=True)
    week_tab = models.IntegerField('week_tab', blank=True, null=True)
    buying_controller = models.TextField('buying_controller',max_length=100, blank=True, null=True)
    parent_supplier = models.TextField('parent_supplier',max_length=100, blank=True, null=True)    
    buyer = models.TextField('buyer',max_length=100, blank=True, null=True)
    user_attributes = models.TextField('user_attributes', default='')    
    forecast_data = models.TextField('forecast_data', default='') 
    similar_products = models.TextField('similar_products',default = '')
    modified_flag = models.IntegerField('modified_flag', blank=True, null=True)
    system_time = models.DateField('system_time',auto_now=False, auto_now_add=False,default='')
    page = models.TextField('page',default = '')

#product impact save scenario

class delist_scenario(models.Model):
    scenario_name = models.TextField('scenario_name', max_length=100, blank=True, null=True)
    token  = models.CharField(max_length=100, default="none")
    user_id = models.CharField(max_length=100, default="none")
    user_name = models.TextField('user_name', max_length=100, blank=True, null=True)
    time_period = models.TextField('time_period', blank=True, null=True)
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    designation = models.TextField('designation', max_length=100, blank=True, null=True)
    buyer = models.TextField('buyer', max_length=100, blank=True, null=True)
    user_attributes = models.TextField('user_attributes', default='')
    chart_attr = models.TextField('chart_attr', default='')
    supp_attr = models.TextField('supp_attr', default='')
    delist_attr = models.TextField('delist_attr', default='')
    system_time = models.DateTimeField('system_time', auto_now=False, auto_now_add=False, default='')
    page = models.TextField('page', default='')
    session_id=models.CharField(max_length=100, default="none",null=True)
    view_mine= models.TextField('view_mine', max_length=100, blank=True, null=True)
    input_tpns = models.TextField('chart_attr', default='')
    
