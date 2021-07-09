from os import read
import pandas as pd
import logging as log

log.basicConfig(filename='app.log', format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=log.INFO)

#Path Of PartnerCenter File
workfile = "PartnerCenter.xlsx"
try:    
    df_1 = pd.read_excel(workfile)
    log.info('Reading PartnerCenter File')

    frames = [df_1]

    all_data_df = pd.concat(frames, axis=0)
    data_group = all_data_df.groupby(['CustomerCompanyName','SubscriptionId','ServiceType','ResourceName','Region']).sum()
    final_data = data_group['ConsumedQuantity'].round(0).sort_values(ascending=False)

    #File after Pivoting table
    final_data.to_excel('abc.xlsx')

    #Repeat all Item Label 
    data_frame = pd.read_excel('abc.xlsx')
    data_frame.CustomerCompanyName.ffill(inplace = True)
    data_frame.SubscriptionId.ffill(inplace = True)
    data_frame.ServiceType.ffill(inplace = True)

    #Instance Count calculation
    data_frame['InstanceCount'] = (data_frame['ConsumedQuantity']/585).round(0)

    #Final File
    data_frame.to_excel('finalFile.xlsx',index=False)
    log.info('Pivot Excel created with name finalFile.xlsx')
except IOError as e:
    log.error('Error occurred ' + str(e))

