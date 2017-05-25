from django.conf.urls import url, include
from . import npd_filters,npd_view,delist_filters,delist_view

urlpatterns = [
    
    ###Url for NPD Opportunity view filters data 
    #### Single Select Cascading 
    url(r'^npd_view1/filter_data', npd_filters.opportunity_filters.as_view(), name='opportunity_filters'),    

    ####NPD Opportunity View Urls
    url(r'^npd/outperformance$', npd_view.market_outperformance.as_view(), name='outperformance'),
    url(r'^npd/pricebucket', npd_view.pricebucket_skudistribution.as_view(), name='pricebucket'),
    url(r'^npd/psgskudistribution', npd_view.psgskudistribution.as_view(), name='psgskudistribution'),
    url(r'^npd/unmatchedprod', npd_view.unmatched_products.as_view(), name='unmatchedprod'),

    ####NPD Impact View

    ####Url filters data for NPD Impact View
    #### Single Select Cascading 
    url(r'^npd_impact_view/filter_data', npd_filters.impact_filters.as_view(), name='impact_filters'),


    ####NPD Impact View
    url(r'^npd_impact_view_forecast', npd_view.forecast_impact.as_view(), name='forecast_impact'),
    url(r'^npd_impact_view_bubble_chart', npd_view.supplier_performance_chart.as_view(), name='npd_impact_view_bubble_chart'),
    url(r'^npd_impact_view_bubble_table', npd_view.supplier_performance_table.as_view(), name='npd_impact_view_bubble_table'),

    ###NPD impact save scenario
    url(r'^npd_impact_save_scenario', npd_view.npd_save_scenario.as_view(), name='npd_impact_save_scenario'),
    url(r'^npd_impact_list_scenario', npd_view.npd_list_scenario.as_view(), name='npd_impact_list_scenario'),
    url(r'^npd_impact_view_scenario', npd_view.npd_view_scenario.as_view(), name='npd_impact_view_scenario'),


    ####NEGOTIATION

    ####Url for negotiation filter data 
    ### Multselect filters
    url(r'^nego/filter_data', delist_filters.negotiation_filters.as_view(), name='negotiation_filters'),
    url(r'^nego/filter_new', delist_filters.negotiation_filters_new.as_view(), name='negotiation_filters_new'),

    ####Negotiation URL 
    url(r'^nego_chart$', delist_view.supplier_importance_chart.as_view(), name='supplier_importance_chart'),
    url(r'^nego_table$', delist_view.supplier_importance_table.as_view(), name='supplier_importance_table'),


    ####PRODUCT IMPACT

    ####Url for product impact filter data
    ### Multiselect Filters
    url(r'^product_impact/filter_data', delist_filters.product_impact_filters.as_view(), name='product_impact_filters'),
    url(r'^product_impact/filter_new', delist_filters.product_impact_filters_new.as_view(), name='product_impact_filters_new'),

    url(r'^product_impact_chart', delist_view.product_impact_chart.as_view(), name='product_impact_chart'),
    
    url(r'^product_impact_delist_table', delist_view.product_impact_delist_table.as_view(), name='product_impact_delist_table'),
    url(r'^product_impact_supplier_table', delist_view.product_impact_supplier_table.as_view(), name='product_impact_supplier_table'),

    url(r'^supplier_table_popup', delist_view.supplier_popup.as_view(), name='supplier_popup'),
    url(r'^delist_table_popup', delist_view.delist_popup.as_view(), name='delist_popup'),

    ##Delist save scenario
    url(r'^delist_scenario', delist_view.delist_scenario_final.as_view(), name='delist_scenario_final'),
    url(r'^delist_list_scenario', delist_view.delist_scenario_list.as_view(), name='delist_scenario_list'),
    url(r'^display_delist_scenario', delist_view.display_delist_scenario.as_view(), name='display_delist_scenario'),

    ]
