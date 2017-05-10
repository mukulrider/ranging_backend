from django.conf.urls import url, include
from . import views

urlpatterns = [
    
    ###Url for NPD Opportunity view filters data 
    #### Single Select Cascading 
    url(r'^npd_view1/filter_data', views.npdpage_filterdata_new.as_view(), name='npdpage_filterdata_new'),    

    ####NPD Opportunity View Urls
    url(r'^npd/outperformance$', views.npdpage_outperformance.as_view(), name='npd_outperformance'),
    url(r'^npd/pricebucket', views.npdpage_pricebucket.as_view(), name='npd_pricebucket'),
    url(r'^npd/psgskudistribution', views.npdpage_psgskudistribution.as_view(), name='npd_psgskudistribution'),
    url(r'^npd/unmatchedprod', views.npdpage_unmatchedprod.as_view(), name='npd_unmatchedprod'),

    ####NPD 2nd HALF

    ####Url filters data for NPD Impact View
    url(r'^npd_impact_view/filter_data', views.npdimpactpage_filterdata.as_view(), name='npdpage_filterdata'),


    ####NPD Impact page
    url(r'^npd_impact_view_bubble_chart', views.npdpage_impact_bubble_chart.as_view(), name='npd_impact_view_bubble_chart'),
    url(r'^npd_impact_view_bubble_table', views.npdpage_impact_bubble_table.as_view(), name='npd_impact_view_bubble_table'),
    url(r'^npd_impact_view_forecast', views.npdpage_impact_forecast.as_view(), name='npd_impact_view_forecast'),

    ###NPD save scenario
    url(r'^npd_impact_save_scenario', views.npdpage_impact_save_scenario.as_view(), name='npd_impact_save_scenario'),
    url(r'^npd_impact_list_scenario', views.npd_scenario_list.as_view(), name='npd_impact_list_scenario'),
    url(r'^npd_impact_view_scenario', views.npd_view_scenario.as_view(), name='npd_impact_list_scenario'),
    

    


    ####Url for negotiation filter data  
    url(r'^nego/filter_data', views.filters_nego.as_view(), name='nego_filterdata'),

    ####Negotiation URL 
    url(r'^nego_chart$', views.nego_bubble_chart.as_view(), name='nego_chart'),
    url(r'^nego_table$', views.nego_bubble_table.as_view(), name='nego_table'),

    ####Url for negotiation filter data
    url(r'^product_impact/filter_data', views.filters_product_impact.as_view()), 



    ####Product Impact URL
    url(r'^product_impact_chart', views.product_impact_chart.as_view(), name='product_impact_chart'),
    
    url(r'^product_impact_delist_table', views.product_impact_delist_table.as_view(), name='product_impact_delist_table'),
    url(r'^product_impact_supplier_table', views.product_impact_supplier_table.as_view(), name='product_impact_supplier_table'),

    url(r'^supplier_table_popup', views.supplier_popup.as_view(), name='supplier_popup'),
    url(r'^delist_table_popup', views.delist_popup.as_view(), name='delist_popup'),


    # ##Delist save scenario
    url(r'^delist_scenario', views.delist_scenario_final.as_view(), name='delist_scenario_final'),
    url(r'^delist_list_scenario', views.delist_scenario_list.as_view(), name='delist_scenario_list'),
    # # url(r'^display_delist_scenario', views.display_delist_scenario.as_view(), name='display_delist_scenario'),


    # url(r'^classa', views.classA.as_view(), name='classA'),
    # url(r'^classb', views.classB.as_view(), name='classB'),
        



    # url(r'^npd_impact_view_scenario', views.displaynpd_scenario.as_view(), name='displaynpd_scenario'),
    # url(r'^npd_edit_forecast', views.npdpage_impact_edit_forecast.as_view(), name='npd_impact_view_edit_forecast'),


    ]
