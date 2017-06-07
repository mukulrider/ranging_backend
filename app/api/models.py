from __future__ import unicode_literals
from django.utils.encoding import python_2_unicode_compatible
import uuid
from django.db import models
from django.conf import settings
from datetime import date


class outperformance(models.Model):
    category_director = models.TextField('category_director', max_length=100, blank=True, null=True)
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    buyer = models.TextField('buyer', max_length=100, blank=True, null=True)
    junior_buyer = models.TextField('junior_buyer', max_length=100, blank=True, null=True)
    product_sub_group_code = models.TextField('product_sub_group_code', max_length=100, blank=True, null=True)
    week_flag = models.TextField('output_date', max_length=100, blank=True, null=True)
    update_date = models.DateField('update_date')
    week_number = models.IntegerField('week_number', blank=True, null=True)
    tesco_outperformanc_percentage = models.DecimalField('tesco_outperformanc_percentage', max_digits=20,
                                                         decimal_places=10, default=0.0)
    tesco_outperformanc_unit_prcnt = models.DecimalField('tesco_outperformanc_unit_prcnt', max_digits=20,
                                                         decimal_places=10, default=0.0)
    product_sub_group_description = models.TextField('product_sub_group_description', max_length=100, blank=True,
                                                     null=True)

    def __str__(self):
        return '%s' % (self.buying_controller)


class pricebucket(models.Model):
    product_sub_group_code = models.TextField('product_sub_group_code', max_length=100, blank=True, null=True)
    category_director = models.TextField('category_director', max_length=100, blank=True, null=True)
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    buyer = models.TextField('buyer', max_length=100, blank=True, null=True)
    junior_buyer = models.TextField('junior_buyer', max_length=100, blank=True, null=True)
    retailer = models.TextField('retailer', max_length=100, blank=True, null=True)
    week_flag = models.TextField('week_flag', max_length=100, blank=True, null=True)
    sku = models.IntegerField('sku', blank=True, null=True)
    sku_gravity = models.IntegerField('sku_gravity', blank=True, null=True)
    product_sub_group_description = models.TextField('product_sub_group_description', max_length=100, blank=True,
                                                     null=True)
    price_gravity = models.TextField('price_gravity', max_length=100, blank=True, null=True)

    def __str__(self):
        return '%s' % (self.buying_controller)


class unmatchedprod(models.Model):
    competitor_id = models.IntegerField('competitor_id', blank=True, null=True)
    competitor_product_sid = models.IntegerField('competitor_product_sid', blank=True, null=True)
    week_flag = models.TextField('week_flag', max_length=100, blank=True, null=True)
    product_sub_group_code = models.TextField('product_sub_group_code', max_length=100, blank=True, null=True)
    category_director = models.TextField('category_director', max_length=100, blank=True, null=True)
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    buyer = models.TextField('buyer', max_length=100, blank=True, null=True)
    junior_buyer = models.TextField('junior_buyer', max_length=100, blank=True, null=True)
    product_sub_group_description = models.TextField('product_sub_group_description', max_length=100, blank=True,
                                                     null=True)
    retailer = models.TextField('retailer', max_length=100, blank=True, null=True)
    competitor_product_desc = models.TextField('competitor_product_desc', max_length=100, blank=True, null=True)
    asp = models.DecimalField('asp', max_digits=20, decimal_places=2, default=0.0)

    def __str__(self):
        return '%s' % (self.buying_controller)


class pps_ros_quantile(models.Model):
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    base_product_number = models.IntegerField('base_product_number', blank=True, null=True)
    store_type = models.TextField('store_type', max_length=100, blank=True, null=True)
    pps_ros_quantile = models.TextField('pps_ros_quantile', max_length=100, blank=True, null=True)
    pps_quantile = models.TextField('pps_quantile', max_length=100, blank=True, null=True)
    ros_quantile = models.TextField('ros_quantile', max_length=100, blank=True, null=True)

    def __str__(self):
        return '%s' % (self.base_product_number)


class cts_data(models.Model):
    base_product_number = models.IntegerField('base_product_number', blank=True, null=True)
    long_description = models.TextField('long_description', max_length=100, blank=True, null=True)
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    store_type = models.TextField('store_type', max_length=100, blank=True, null=True)
    cts_per_unit = models.DecimalField('cts_per_unit', max_digits=15, decimal_places=2, default=0.0)

    def __str__(self):
        return '%s' % (self.base_product_number)


class product_hierarchy(models.Model):
    commercial_director = models.TextField('commercial_director', max_length=100, blank=True, null=True)
    category_director = models.TextField('category_director', max_length=100, blank=True, null=True)
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    buyer = models.TextField('buyer', max_length=100, blank=True, null=True)
    junior_buyer = models.TextField('junior_buyer', max_length=200, blank=True, null=True)
    product_sub_group_code = models.TextField('product_sub_group_code', max_length=200, blank=True, null=True)
    product_sub_group_description = models.TextField('product_sub_group_description', max_length=200, blank=True,
                                                     null=True)
    base_product_number = models.IntegerField('base_product_number', blank=True, null=True)
    long_description = models.TextField('long_description', max_length=200, blank=True)
    brand_indicator = models.TextField('brand_indicator', max_length=100, blank=True, null=True)
    brand_name = models.TextField('brand_name', max_length=200, blank=True, null=True)
    parent_supplier = models.TextField('parent_supplier', max_length=100, blank=True, null=True)

    def __str__(self):
        return '%s' % (self.base_product_number)


class product_price(models.Model):
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    store_type = models.TextField('store_type', max_length=100, blank=True, null=True)
    base_product_number = models.IntegerField('base_product_number', blank=True, null=True)
    brand_indicator = models.TextField('brand_indicator', max_length=100, blank=True, null=True)
    asp = models.DecimalField('asp', blank=True, max_digits=20, decimal_places=3, default=0.0)
    acp = models.DecimalField('acp', blank=True, max_digits=20, decimal_places=3, default=0.0)

    def __str__(self):
        return '%s' % (self.base_product_number)


class shelf_review_subs(models.Model):
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    store_type = models.TextField('store_type', max_length=100, blank=True, null=True)
    productcode = models.IntegerField('productcode', blank=True, null=True)
    productdescription = models.TextField('productdescription', max_length=100, blank=True, null=True)
    substituteproductcode = models.IntegerField('substituteproductcode', blank=True, null=True)
    substituteproductdescription = models.TextField('substituteproductdescription', max_length=100, blank=True,
                                                    null=True)
    substitutescore = models.DecimalField('substitutescore', blank=True, max_digits=20, decimal_places=4, default=0.0)
    tcs_per = models.DecimalField('tcs_per', blank=True, max_digits=20, decimal_places=4, default=0.0)
    exclusivity_per = models.DecimalField('exclusivity_per', blank=True, max_digits=20, decimal_places=4, default=0.0)

    def __str__(self):
        return '%s' % (self.productcode)


class prod_similarity_subs(models.Model):
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    store_type = models.TextField('store_type', max_length=100, blank=True, null=True)
    base_prod = models.IntegerField('base_prod', blank=True, null=True)
    sub_prod = models.IntegerField('sub_prod', blank=True, null=True)
    actual_similarity_score = models.DecimalField('actual_similarity_score', blank=True, max_digits=20,
                                                  decimal_places=4, default=0.0)
    similarity_score = models.DecimalField('similarity_score', blank=True, max_digits=20, decimal_places=4, default=0.0)

    def __str__(self):
        return '%s' % (self.base_prod)


class supplier_share(models.Model):
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    store_type = models.TextField('store_type', max_length=100, blank=True, null=True)
    parent_supplier = models.TextField('parent_supplier', max_length=100, blank=True, null=True)
    base_product_number = models.IntegerField('base_product_number', blank=True, null=True)
    brand_indicator = models.TextField('brand_indicator', max_length=100, blank=True, null=True)
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
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    buyer = models.TextField('buyer', max_length=100, blank=True, null=True)
    junior_buyer = models.TextField('junior_buyer', max_length=100, blank=True, null=True)
    brand_indicator = models.TextField('brand_indicator', max_length=100, blank=True, null=True)
    brand_name = models.TextField('brand_name', max_length=100, blank=True, null=True)
    product_sub_group_code = models.TextField('product_sub_group_code', max_length=100, blank=True, null=True)
    product_sub_group_description = models.TextField('product_sub_group_description', max_length=100, blank=True,
                                                     null=True)
    base_product_number = models.IntegerField('base_product_number', blank=True, null=True)
    long_description = models.TextField('long_description', max_length=100, blank=True, null=True)
    store_type = models.TextField('store_type', max_length=100, blank=True, null=True)
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
    psg_value_impact = models.DecimalField('psg_value_impact', blank=True, max_digits=20, decimal_places=2, default=0.0)

    def __str__(self):
        return '%s' % (self.base_product_number)


class product_desc(models.Model):
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    base_product_number = models.IntegerField('base_product_number', blank=True, null=True)
    long_description = models.TextField('long_description', max_length=100, blank=True, null=True)
    brand_indicator = models.TextField('brand_indicator', max_length=100, blank=True, null=True)

    def __str__(self):
        return '%s' % (self.base_product_number)


class product_contri(models.Model):
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    store_type = models.TextField('store_type', max_length=100, blank=True, null=True)
    base_product_number = models.IntegerField('base_product_number', blank=True, null=True)
    predicted_volume = models.DecimalField('predicted_volume', blank=True, max_digits=20, decimal_places=2, default=0.0)
    time_period = models.TextField('time_period', max_length=200, blank=True)
    no_of_stores = models.IntegerField('no_of_stores', blank=True, null=True)

    def __str__(self):
        return '%s' % (self.base_product_number)


# changed to monthly_holiday
class uk_holidays(models.Model):
    period_number = models.IntegerField('period_number', blank=True, null=True)
    year_number = models.IntegerField('year_number', blank=True, null=True)
    holiday_count = models.IntegerField('holiday_count', blank=True, null=True)
    store_type = models.TextField('store_type', max_length=100, blank=True, null=True)

    def __str__(self):
        return '%s' % (self.period_number)


# dummy model
class uk_holidays_dummy(models.Model):
    period_number = models.IntegerField('period_number', blank=True, null=True)
    year_number = models.IntegerField('year_number', blank=True, null=True)
    holiday_count = models.IntegerField('holiday_count', blank=True, null=True)
    area_price_code = models.TextField('area_price_code', max_length=100, blank=True, null=True)

    def __str__(self):
        return '%s' % (self.period_number)


# changed
class seasonality_index(models.Model):
    months = models.IntegerField('months', blank=True, null=True)
    psg = models.TextField('psg', max_length=100, blank=True, null=True)
    psg_names = models.TextField('psg_names', max_length=100, blank=True, null=True)
    monthly_avg_ind = models.DecimalField('monthly_avg_ind', max_digits=20, decimal_places=10, default=0.0)
    adjusted_index = models.DecimalField('adjusted_index', max_digits=20, decimal_places=10, default=0.0)

    def __str__(self):
        return '%s' % (self.psg)


class npd_calendar(models.Model):
    calendar_date = models.DateField(blank=True, null=True)
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


# in process
class input_npd(models.Model):
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    buyer = models.TextField('buyer', max_length=100, blank=True, null=True)
    junior_buyer = models.TextField('junior_buyer', max_length=100, blank=True, null=True)
    product_sub_group_code = models.TextField('product_sub_group_code', max_length=100, blank=True, null=True)
    product_sub_group_description = models.TextField('product_sub_group_description', max_length=200, blank=True,
                                                     null=True)
    brand_name = models.TextField('brand_name', max_length=100, blank=True, null=True)
    package_type = models.TextField('package_type', max_length=100, blank=True, null=True)
    area_price_code = models.IntegerField('area_price_code', blank=True, null=True)
    measure_type = models.TextField('measure_type', max_length=100, blank=True, null=True)
    area_price_code_description = models.TextField('area_price_code_description', max_length=100, blank=True, null=True)
    price_band = models.TextField('price_band', max_length=100, blank=True, null=True)
    curr_week_number = models.IntegerField('curr_week_number', blank=True, null=True)
    period_number = models.IntegerField('period_number', blank=True, null=True)
    period_week_number = models.IntegerField('period_week_number', blank=True, null=True)
    quarter_number = models.IntegerField('quarter_number', blank=True, null=True)

    def __str__(self):
        return '%s' % (self.product_sub_group_description)


# changed
class attribute_score_allbc(models.Model):
    bc = models.TextField('bc', max_length=100, blank=True, null=True)
    avg_psg = models.DecimalField('avg_psg', max_digits=20, decimal_places=10, default=0.0)
    avg_brand = models.DecimalField('avg_brand', max_digits=20, decimal_places=10, default=0.0)
    avg_pkg = models.DecimalField('avg_pkg', max_digits=20, decimal_places=10, default=0.0)
    avg_price = models.DecimalField('avg_price', max_digits=20, decimal_places=10, default=0.0)
    avg_tillroll = models.DecimalField('avg_tillroll', max_digits=20, decimal_places=10, default=0.0)
    avg_size = models.DecimalField('avg_size', max_digits=20, decimal_places=10, default=0.0)

    def __str__(self):
        return '%s' % (self.bc)


class bc_allprod_attributes(models.Model):
    bc = models.TextField('bc', max_length=100, blank=True, null=True)
    base_product_number = models.IntegerField('base_product_number', blank=True, null=True)
    product_sub_group_description = models.TextField('product_sub_group_description', max_length=200, blank=True,
                                                     null=True)
    brand_name = models.TextField('brand_name', max_length=100, blank=True, null=True)
    package_type = models.TextField('package_type', max_length=100, blank=True, null=True)
    till_roll_description = models.TextField('till_roll_description', max_length=200, blank=True, null=True)
    size = models.DecimalField('size', max_digits=20, decimal_places=10, default=0.0, null=True)
    measure_type = models.TextField('measure_type', max_length=100, blank=True, null=True)
    price_band = models.TextField('price_band', max_length=100, blank=True, null=True)
    long_description = models.TextField('long_description', max_length=200, blank=True, null=True)
    brand_grp20 = models.IntegerField('brand_grp20', blank=True, null=True)
    brand_ind = models.TextField('brand_ind', max_length=100, blank=True, null=True)
    launch_tesco_week = models.IntegerField('launch_tesco_week', blank=True, null=True)

    def __str__(self):
        return '%s' % (self.base_product_number)


class consolidated_buckets(models.Model):
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    bucket_value = models.TextField('bucket_value', max_length=100, blank=True, null=True)
    cannibalization = models.DecimalField('cannibalization', blank=True, max_digits=20, decimal_places=2, default=0.0)
    bucket_flag = models.TextField('bucket_flag', max_length=100, blank=True, null=True)

    def __str__(self):
        return '%s' % (self.buying_controller)


class consolidated_calculated_cannibalization(models.Model):
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    base_product_number = models.IntegerField('base_product_number', blank=True, null=True)
    cannibalization = models.DecimalField('cannibalization', blank=True, max_digits=20, decimal_places=2, default=0.0)
    brand_ind = models.TextField('brand_ind', max_length=100, blank=True, null=True)

    def __str__(self):
        return '%s' % (self.buying_controller)


class range_space_store_future(models.Model):
    retail_outlet_number = models.IntegerField('retail_outlet_number', blank=True, null=True)
    record_type = models.TextField('record_type', max_length=100, blank=True, null=True)
    merchandise_group_code = models.TextField('merchandise_group_code', max_length=100, blank=True, null=True)
    start_date = models.DateField(blank=True, null=True)
    store_cluster_id = models.DecimalField('store_cluster_id', blank=True, max_digits=20, decimal_places=2, default=0.0)
    equipment_type = models.TextField('equipment_type', max_length=100, blank=True, null=True)
    equipment_type_single_code = models.TextField('equipment_type_single_code', max_length=100, blank=True, null=True)
    range_class = models.TextField('range_class', max_length=100, blank=True, null=True)

    def __str__(self):
        return '%s' % (self.range_class)



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

'''


class store_details(models.Model):
    retail_outlet_number = models.IntegerField('retail_outlet_number', blank=True, null=True)
    area_price_code = models.IntegerField('area_price_code', blank=True, null=True)

    pfs_store = models.DecimalField('pfs_store', blank=True, max_digits=20, decimal_places=2, default=0.0)
    store_5k = models.DecimalField('store_5k', blank=True, max_digits=20, decimal_places=2, default=0.0)
    store_20k = models.DecimalField('store_20k', blank=True, max_digits=20, decimal_places=2, default=0.0)
    store_50k = models.DecimalField('store_50k', blank=True, max_digits=20, decimal_places=2, default=0.0)
    store_100k = models.DecimalField('store_100k', blank=True, max_digits=20, decimal_places=2, default=0.0)
    store_100kplus = models.DecimalField('store_100kplus', blank=True, max_digits=20, decimal_places=2, default=0.0)

    express_standalone_store = models.DecimalField('express_standalone_store', blank=True, max_digits=20,
                                                   decimal_places=2, default=0.0)
    homeplus_store = models.DecimalField('homeplus_store', blank=True, max_digits=20, decimal_places=2, default=0.0)
    superstore_store = models.DecimalField('superstore_store', blank=True, max_digits=20, decimal_places=2, default=0.0)
    express_esso_store = models.DecimalField('express_esso_store', blank=True, max_digits=20, decimal_places=2,
                                             default=0.0)
    metro_high_street_store = models.DecimalField('metro_high_street_store', blank=True, max_digits=20,
                                                  decimal_places=2, default=0.0)
    metro_badged_store = models.DecimalField('metro_badged_store', blank=True, max_digits=20, decimal_places=2,
                                             default=0.0)
    wine_shop_store = models.DecimalField('wine_shop_store', blank=True, max_digits=20, decimal_places=2, default=0.0)
    express_tesco_store = models.DecimalField('express_tesco_store', blank=True, max_digits=20, decimal_places=2,
                                              default=0.0)
    metro_classic_store = models.DecimalField('metro_classic_store', blank=True, max_digits=20, decimal_places=2,
                                              default=0.0)
    dotcom_store = models.DecimalField('dotcom_store', blank=True, max_digits=20, decimal_places=2, default=0.0)
    metro_ranged_store = models.DecimalField('metro_ranged_store', blank=True, max_digits=20, decimal_places=2,
                                             default=0.0)

    region2_south_store = models.DecimalField('region2_south_store', blank=True, max_digits=20, decimal_places=2,
                                              default=0.0)
    region2_midlands_store = models.DecimalField('region2_midlands_store', blank=True, max_digits=20, decimal_places=2,
                                                 default=0.0)
    region2_other_store = models.DecimalField('region2_other_store', blank=True, max_digits=20, decimal_places=2,
                                              default=0.0)
    region2_scoland_store = models.DecimalField('region2_scoland_store', blank=True, max_digits=20, decimal_places=2,
                                                default=0.0)
    region2_north_store = models.DecimalField('region2_north_store', blank=True, max_digits=20, decimal_places=2,
                                              default=0.0)

    countrycode_ni_store = models.DecimalField('countrycode_ni_store', blank=True, max_digits=20, decimal_places=2,
                                               default=0.0)
    countrycode_gb_store = models.DecimalField('countrycode_gb_store', blank=True, max_digits=20, decimal_places=2,
                                               default=0.0)
    countrycode_i_store = models.DecimalField('countrycode_i_store', blank=True, max_digits=20, decimal_places=2,
                                              default=0.0)

    def __str__(self):
        return '%s' % (self.retail_outlet_number)


class npd_vol_sum_avg(models.Model):
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    index_columns = models.TextField('index_columns', max_length=100, blank=True, null=True)
    entries = models.TextField('value', max_length=200, blank=True, null=True)
    store_type = models.TextField('store_type', max_length=200, blank=True, null=True)
    year_period = models.IntegerField('year_period', max_length=20, blank=True, null=True)
    year_quarter = models.IntegerField('year_quarter', max_length=20, blank=True, null=True)
    year_number = models.IntegerField('year_number', max_length=20, blank=True, null=True)
    vol_sum_month = models.IntegerField('vol_sum_month', max_length=30, blank=True, null=True)
    vol_avg_month = models.DecimalField('vol_avg_month', max_digits=30,decimal_places=10, blank=True, null=True)
    vol_sum_quarter = models.IntegerField('vol_sum_quarter', max_length=30, blank=True, null=True)
    vol_avg_quarter = models.DecimalField('vol_avg_quarter', max_digits=30,decimal_places=10, blank=True, null=True)
    vol_sum_year = models.IntegerField('vol_sum_year', max_length=30, blank=True, null=True)
    vol_avg_year = models.DecimalField('vol_avg_year', max_digits=30,decimal_places=10, blank=True, null=True)

    def __str__(self):
        return '%s' % (self.index_columns)


class npd_vol_sku_period(models.Model):
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    index_columns = models.TextField('index_columns', max_length=100, blank=True, null=True)
    entries = models.TextField('value', max_length=200, blank=True, null=True)
    vol_sku_period = models.DecimalField('vol_sku_period', max_digits=30,decimal_places=10, blank=True, null=True)

    def __str__(self):
        return '%s' % (self.index_columns)
'''

# changed
class merch_range(models.Model):
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    merchandise_group_code = models.TextField('merchandise_group_code', max_length=100, blank=True, null=True)
    range_class = models.TextField('range_class', max_length=100, blank=True, null=True)
    merchandise_group_description = models.TextField('merchandise_group_description', max_length=100, blank=True,
                                                     null=True)

    def __str__(self):
        return '%s' % (self.merchandise_group_description)


class npd_product_hierarchy(models.Model):
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    buyer = models.TextField('buyer', max_length=100, blank=True, null=True)
    junior_buyer = models.TextField('junior_buyer', max_length=200, blank=True, null=True)
    product_sub_group_code = models.TextField('product_sub_group_code', max_length=200, blank=True, null=True)
    product_sub_group_description = models.TextField('product_sub_group_description', max_length=200, blank=True,
                                                     null=True)

    def __str__(self):
        return '%s' % (self.product_sub_group_code)


class npd_supplier_ads(models.Model):
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    parent_supplier = models.TextField('parent_supplier', max_length=100, blank=True, null=True)
    product_sub_group_code = models.TextField('product_sub_group_code', max_length=100, blank=True, null=True)
    product_subgroup = models.TextField('product_subgroup', max_length=100, blank=True, null=True)
    base_product_number = models.IntegerField('base_product_number', blank=True, null=True)
    long_description = models.TextField('long_description', max_length=100, blank=True, null=True)
    brand_indicator = models.TextField('brand_indicator', max_length=100, blank=True, null=True)
    store_type = models.TextField('store_type', max_length=100, blank=True, null=True)
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
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    brand_name = models.TextField('brand_name', max_length=100, blank=True, null=True)
    brand_grp20 = models.IntegerField('brand_grp20', blank=True, null=True)
    brand_ind = models.TextField('brand_ind', max_length=100, blank=True, null=True)

    def __str__(self):
        return '%s' % (self.brand_name)


# new models
class buyertillroll_input_npd(models.Model):
    till_roll_description = models.TextField('till_roll_description', max_length=200, blank=True, null=True)
    buyer = models.TextField('buyer', max_length=100, blank=True, null=True)

    def __str__(self):
        return '%s' % (self.till_roll_description)


class cannibalization_vol_buckets(models.Model):
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    high_cutoff = models.IntegerField('high_cutoff', blank=True, null=True)
    low_cutoff = models.IntegerField('low_cutoff', blank=True, null=True)


## SAVE SCENARIO FOR NPD IMPACT

class SaveScenario(models.Model):
    user_id = models.CharField('user_id', max_length=100, default="none")
    user_name = models.CharField('user_name', max_length=100, default="none")
    designation = models.TextField('designation', max_length=100, blank=True, null=True)
    session_id = models.CharField('session_id', max_length=100, default="none")
    scenario_name = models.TextField('scenario_name', max_length=100, blank=True, null=True)
    scenario_tag = models.TextField('scenario_tag', max_length=100, blank=True, null=True)
    week_tab = models.IntegerField('week_tab', blank=True, null=True)
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    parent_supplier = models.TextField('parent_supplier', max_length=100, blank=True, null=True)
    buyer = models.TextField('buyer', max_length=100, blank=True, null=True)
    user_attributes = models.TextField('user_attributes', default='')
    asp = models.DecimalField('asp', blank=True, max_digits=20, decimal_places=4, default=0.0)
    forecast_data = models.TextField('forecast_data', default='')
    value_forecast = models.DecimalField('value_forecast', blank=True, max_digits=20, decimal_places=4, default=0.0)
    value_impact = models.DecimalField('value_impact', blank=True, max_digits=20, decimal_places=4, default=0.0)
    value_cannibalized = models.DecimalField('value_cannibalized', blank=True, max_digits=20, decimal_places=4,
                                             default=0.0)
    volume_forecast = models.DecimalField('volume_forecast', blank=True, max_digits=20, decimal_places=4, default=0.0)
    volume_impact = models.DecimalField('volume_impact', blank=True, max_digits=20, decimal_places=4, default=0.0)
    volume_cannibalized = models.DecimalField('volume_cannibalized', blank=True, max_digits=20, decimal_places=4,
                                              default=0.0)
    similar_products = models.TextField('similar_products', default='')
    modified_flag = models.IntegerField('modified_flag', blank=True, null=True)
    system_time = models.DateField('system_time', auto_now=False, auto_now_add=False, default='')
    page = models.TextField('page', default='')


# scenario tracker for npd (stores all entries)
class ScenarioTracker(models.Model):
    user_id = models.CharField('user_id', max_length=100, default="none")
    user_name = models.CharField('user_name', max_length=100, default="none")
    designation = models.TextField('designation', max_length=100, blank=True, null=True)
    session_id = models.CharField('session_id', max_length=100, default="none")
    scenario_name = models.TextField('scenario_name', max_length=100, blank=True, null=True)
    scenario_tag = models.TextField('scenario_tag', max_length=100, blank=True, null=True)
    week_tab = models.IntegerField('week_tab', blank=True, null=True)
    buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
    parent_supplier = models.TextField('parent_supplier', max_length=100, blank=True, null=True)
    buyer = models.TextField('buyer', max_length=100, blank=True, null=True)
    user_attributes = models.TextField('user_attributes', default='')
    asp = models.DecimalField('asp', blank=True, max_digits=20, decimal_places=4, default=0.0)
    forecast_data = models.TextField('forecast_data', default='')
    value_forecast = models.DecimalField('value_forecast', blank=True, max_digits=20, decimal_places=4, default=0.0)
    value_impact = models.DecimalField('value_impact', blank=True, max_digits=20, decimal_places=4, default=0.0)
    value_cannibalized = models.DecimalField('value_cannibalized', blank=True, max_digits=20, decimal_places=4,
                                             default=0.0)
    volume_forecast = models.DecimalField('volume_forecast', blank=True, max_digits=20, decimal_places=4, default=0.0)
    volume_impact = models.DecimalField('volume_impact', blank=True, max_digits=20, decimal_places=4, default=0.0)
    volume_cannibalized = models.DecimalField('volume_cannibalized', blank=True, max_digits=20, decimal_places=4,
                                              default=0.0)
    similar_products = models.TextField('similar_products', default='')
    modified_flag = models.IntegerField('modified_flag', blank=True, null=True)
    system_time = models.DateField('system_time', auto_now=False, auto_now_add=False, default='')
    page = models.TextField('page', default='')


# SAVE SCENARIO FOR PRODUCT IMPACT
class delist_scenario(models.Model):
    scenario_name = models.TextField('scenario_name', max_length=100, blank=True, null=True)
    token = models.CharField(max_length=100, default="none")
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
    session_id = models.CharField(max_length=100, default="none", null=True)
    view_mine = models.TextField('view_mine', max_length=100, blank=True, null=True)
    input_tpns = models.TextField('chart_attr', default='')


class generate_add_order(models.Model):
 npd_add_order = models.TextField('npd_scenario_name',blank=True, null=True)
 delist_add_order = models.TextField('delist_scenario_name',blank=True, null=True)
 system_time = models.DateField('system_time',auto_now=False, auto_now_add=False,default='')
 user_id = models.CharField('user_id',max_length=100, default="none")
 user_name = models.CharField('user_name',max_length=100, default="none")
 designation = models.TextField('designation', max_length=100, blank=True, null=True)
 buying_controller = models.TextField('buying_controller', max_length=100, blank=True, null=True)
 buyer = models.TextField('buyer', max_length=100, blank=True, null=True)

 def __unicode__(self):
    return '%d: %s' % (self.event_name)

