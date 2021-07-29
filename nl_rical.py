from os import read
import pandas as pd
import logging as log
from azure.storage.blob import BlobServiceClient
import json,os
from zipfile import ZipFile

#logging into app.log file
log.basicConfig(filename='app.log', format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=log.INFO)

try:
    #getting storageAccount Details from Config File
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    config_path = '/'.join([ROOT_DIR, 'config.json'])
    log.info('config.json file fetched.')

    log.info('Reading config.json file')
    # read config file
    with open(config_path) as config_file:
        config = json.load(config_file)
        config = config['nl_ri_calculation']

    STORAGEACCOUNTURL= config['url']
    STORAGEACCOUNTKEY= config['key']
    LOCALPCFILE= config['pcfile']
    LOCALCMPFILE= config['cmpfile']
    CONTAINERNAME1= config['container1']
    CONTAINERNAME2= config['container2']
    BLOBPCFILE= config['blobpc']
    BLOBCMPFILE= config['blobcmp']

    log.info('Storage Account details fetched.')

    #download PC from blob
    blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
    blob_client_instance = blob_service_client_instance.get_blob_client(CONTAINERNAME1+'/NL_RAW_FILE', BLOBPCFILE, snapshot=None)
    with open(LOCALPCFILE, "wb") as my_blob:
        blob_data = blob_client_instance.download_blob()
        blob_data.readinto(my_blob)

    log.info('PC file downloaded from Storage Account.')

    #download CMP from blob
    blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
    blob_client_instance = blob_service_client_instance.get_blob_client(CONTAINERNAME1+'/NL_RAW_FILE', BLOBCMPFILE, snapshot=None)
    with open(LOCALCMPFILE, "wb") as my_blob:
        blob_data = blob_client_instance.download_blob()
        blob_data.readinto(my_blob)

    log.info('CMP file downloaded from Storage Account.')

    file_name = "NL_CMPFile.zip"
  
    # opening the zip file in READ mode
    with ZipFile(file_name, 'r') as zip:
        # extracting all the files
        print('Extracting all the files now...')
        ab = zip.namelist()[0]
        zip.extractall()
    
    os.rename(ab,"NL_CMPFile.csv")

    os.remove('NL_CMPFile.zip')

    #Path Of PartnerCenter File
    PCworkfile = "NL_PartnerCenter.xlsx"
    CMPworkfile = "NL_CMPFile.csv"

    df = pd.read_excel(PCworkfile)
    filterData = df[df["ServiceName"]=="Virtual Machines"]
    filterData.to_excel("NL_filterFile.xlsx",index=False)

    df_1 = pd.read_excel("NL_filterFile.xlsx")
    log.info('Reading PartnerCenter File')

    frames = [df_1]

    all_data_df = pd.concat(frames, axis=0)
    data_group = all_data_df.groupby(['CustomerCompanyName','SubscriptionId','ServiceType','ResourceName','Region']).sum()
    final_data = data_group['ConsumedQuantity'].round(0).sort_values(ascending=False)

    #File after Pivoting table
    final_data.to_excel('nl_abc.xlsx')

    #Repeat all Item Label 
    data_frame = pd.read_excel('nl_abc.xlsx')
    data_frame.CustomerCompanyName.ffill(inplace = True)
    data_frame.SubscriptionId.ffill(inplace = True)
    data_frame.ServiceType.ffill(inplace = True)

    #Instance Count calculation
    data_frame['InstanceCount'] = (data_frame['ConsumedQuantity']/585).round(0)

    #PivotFile
    data_frame.to_excel('NL_PCPivot.xlsx',index=False)
    os.remove('NL_filterFile.xlsx')
    os.remove('nl_abc.xlsx')
    os.remove('NL_PartnerCenter.xlsx')
    log.info('CMP File Pivot Excel created with name NL_PCPivot.xlsx')

    #Reading CMP csv file
    data = pd.read_csv(CMPworkfile, low_memory=False)
    log.info('Reading CMPFile')

    #Changing SubscriptionId into lower case
    data['SubscriptionId'] = data['SubscriptionId'].str.lower()
    pt = data.pivot_table(data, index=['SubscriptionId','ResellerCompanyName'])
    writer = pd.ExcelWriter('NL_CMPPivot.xlsx')
    pt.to_excel(writer, sheet_name='Sheet1')
    writer.save()

    workfile1 = "NL_PCPivot.xlsx"
    workfile2 = "NL_CMPPivot.xlsx"

    df_1 = pd.read_excel(workfile1)
    df_2 = pd.read_excel(workfile2)

    df_1.reset_index(drop=True)
    df_2.reset_index(drop=True)

    Left_join = pd.merge(df_1,df_2[['SubscriptionId','ResellerCompanyName']], on = "SubscriptionId", how= 'left')

    Left_join.to_excel("NL_CompletedFile.xlsx",index=False)
    log.info('Final Calculated File created with name NL_CompletedFile.xlsx..')
    os.remove('NL_PCPivot.xlsx')
    os.remove('NL_CMPPivot.xlsx')
    os.remove('NL_CMPFile.csv')

    blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
    blob_client_instance = blob_service_client_instance.get_blob_client(CONTAINERNAME2+'/NL_COMP_FILE', "NL_CompletedFile.xlsx", snapshot=None)

    print("\nUploading to Azure Storage as blob:\n\t" + "NL_CompletedFile.xlsx")

    # Upload the created file
    with open("NL_CompletedFile.xlsx", "rb") as data:
        blob_client_instance.upload_blob(data)
     
except IOError as e:
        log.error('Error occurred ' + str(e))

