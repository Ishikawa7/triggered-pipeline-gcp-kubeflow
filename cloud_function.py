import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
import google.cloud.aiplatform as aip

def ingest(event, context):
  client = storage.Client.from_service_account_json("chiave_storage.json")
  bucket = client.get_bucket('report_aggiornato_csv')
  blob = bucket.get_blob('TransactionsListReport.csv')
  blob.download_to_filename('/tmp/scaricato.csv')
  client.close()

  df = pd.read_csv("/tmp/scaricato.csv",sep=';',encoding='cp1250')
  df = df.iloc[5:-9]
  df=df.dropna(axis=0,how='all')
  df=df.dropna(axis=1,how='all')
  df.columns = [str(i) for i in range(0,df.shape[1])]

  dict_transazioni = {
    "Data":[],"Data contabile":[],"Num. trans.":[],"Carta":[],"Negozio":[],
    "Tavolo":[],"Cassa":[],"Ospiti":[],"Operatore":[],"Listino":[]
    }

  dict_articoli = {"Data":[],"Num. trans.":[],"Categoria articolo":[],'Nome articolo':[],
    "Quantità":[],'Importo':[],"IVA":[]}

  for row in df.iloc[:].values:

    if row[0] == "Dati" and not pd.isna(row[0]):
      dict_transazioni["Data"].append(row[2])
      dict_transazioni["Num. trans."].append(row[9])

    if row[11] not in ["Articolo","Totale"] and not pd.isna(row[11]):
      dict_articoli["Data"].append(dict_transazioni['Data'][-1])
      dict_articoli["Num. trans."].append(dict_transazioni['Num. trans.'][-1])
      dict_articoli["Categoria articolo"].append(row[11])
      dict_articoli["Nome articolo"].append(row[12])
      dict_articoli["Quantità"].append(row[13])
      dict_articoli["Importo"].append(row[14])
      dict_articoli["IVA"].append(row[15])

    if row[0] == "Data contabile" and not pd.isna(row[0]):
      dict_transazioni["Data contabile"].append(row[2])
      dict_transazioni["Carta"].append(row[7])
      continue

    if row[0] == "Negozio" and not pd.isna(row[0]):
      dict_transazioni["Negozio"].append(row[2])
      dict_transazioni["Tavolo"].append(row[10])
      continue
    if row[0] == "Cassa" and not pd.isna(row[0]):
      dict_transazioni["Cassa"].append(row[2])
      dict_transazioni["Ospiti"].append(row[10])
      continue
    if row[0] == "Operatore" and not pd.isna(row[0]):
      dict_transazioni["Operatore"].append(row[2])
      continue
    if row[0] == "Listino" and not pd.isna(row[0]):
      dict_transazioni["Listino"].append(row[2])
      continue
    
  df_articoli = pd.DataFrame(dict_articoli)
  df_transazioni = pd.DataFrame(dict_transazioni)
  df_result = df_transazioni.merge(df_articoli, on="Data")

  df_result.drop(columns=["Num. trans._y"],inplace=True)
  df_result["Carta"].fillna(value="NO CARTA",inplace=True)
  df_result["Ospiti"].fillna(value=0,inplace=True)
  df_result["IVA"].fillna(value="ASSENTE",inplace=True)
  df_result["Tavolo"].fillna("NO TAVOLO",inplace=True)
  df_result["Importo"].fillna(0,inplace=True)

  dict_iva = {
    "ASSENTE":0,
    '1':4,
    '2':10,
    '3':22,
    '4':0,
    '99':0
  }

  def stringa_to_num(stringa):
    if len(stringa)>6:
        contenuto_stringa = stringa.split(",")
        risultato = float(contenuto_stringa[0])*1000 + float(contenuto_stringa[1])
    else:
        risultato = float(stringa)
    return risultato

  df_result["Importo"] = df_result["Importo"].apply(stringa_to_num)
  df_result["IVA %"] = df_result["IVA"].apply(lambda x: dict_iva[x])
  df_result["IVA pagata"] = df_result["Importo"]*df_result["IVA %"]*0.01
  df_result["Importo netto"] = df_result['Importo'] - df_result["IVA pagata"]
  df_result["Data contabile"] = pd.to_datetime(df_result['Data contabile'],format="%d/%m/%Y")
  df_result["Data"] = pd.to_datetime(df_result['Data'],format="%d/%m/%Y %H:%M")
  df_result["Giorno settimana"] = df_result["Data"].apply(lambda x : x.day_name())
  df_result["Mese"] = df_result["Data"].apply(lambda x : x.month_name())
  df_result["Fascia oraria"] = df_result["Data"].apply(lambda x : x.hour)
  df_result["Categoria articolo"] = df_result["Categoria articolo"].apply(str)

  def estraiCategoria (stringa):
    lista = stringa.replace(" ","").split("-")
    return lista[0]
  
  def estraiCodice (stringa):
    lista = stringa.replace(" ","").split("-")
    if len(lista) == 1:
        return lista[0]
    else:
        return lista[1]
  
  df_result["Solo categoria articolo"] = df_result["Categoria articolo"].apply(estraiCategoria)
  df_result["Codice articolo"] = df_result["Categoria articolo"].apply(estraiCodice)
  df_result.rename(columns={"Num. trans._x":"Numero transazione","Categoria articolo":"Categoria e codice prodotto","Solo categoria articolo":"Categoria articolo"},inplace=True)

  df_result = df_result[['Data', 'Data contabile','Mese',
       'Giorno settimana','Fascia oraria','Numero transazione', 'Carta', 'Negozio',
       'Tavolo', 'Cassa', 'Ospiti', 'Operatore', 'Listino','Categoria e codice prodotto','Categoria articolo', 'Codice articolo','Nome articolo', 'Quantità', 'Importo',
       'IVA', 'IVA %', 'IVA pagata', 'Importo netto']]
  
  df_result.rename(columns={"IVA":"Tipo IVA","IVA %":"Percentuale tipo IVA","IVA pagata":"IVA","Importo":"Importo lordo"},inplace=True)
  df_result["Coperto Asporto/Delivery"] = df_result["Ospiti"].apply(lambda x: "Asporto/Delivery" if x == 0 else "Coperto")
  df_result["Pranzo/Cena"] = df_result["Data"].apply(lambda x : "Pranzo" if x.hour > 6 and x.hour < 18 else "Cena")
  df_result = df_result[df_result["Importo lordo"]!=0]

  df_result.rename(columns={
    "Data":"Data_orario",
    "Data contabile":"Data",
    "Giorno settimana":"Giorno_settimana",
    "Fascia oraria":"Fascia_oraria",
    "Numero transazione":"Numero_transazione",
    "Categoria e codice prodotto":"Categoria_e_codice_articolo",
    "Categoria articolo":"Categoria_articolo",
    "Codice articolo":"Codice_articolo",
    "Nome articolo":"Nome_articolo",
    "Quantità":"Quantity",
    "Importo lordo":"Importo_lordo",
    "Tipo IVA":"Tipo_IVA",
    "Percentuale tipo IVA":"Percentuale_tipo_IVA",
    "Importo netto":"Importo_netto",
    "Coperto Asporto/Delivery":"Coperto_AsportoDelivery",
    "Pranzo/Cena":"Pranzo_Cena"
  },inplace=True)

  df_result["Quantity"] = df_result["Quantity"].apply(lambda x: float(x))
  df_result["Ospiti"] = df_result["Ospiti"].apply(lambda x: int(x))

  df_result.drop_duplicates(["Data_orario","Numero_transazione"],inplace=True)

  df_result.sort_values(by="Data_orario",inplace=True)

  df_result["Categoria_articolo"].unique()

  articoli_accorpare = ['EA5562','EA78918', 'ET00219', 'ET00701','ET04209','ET00345','EA1597','EA14943','ET03721','AF32311','ET03720','ET03722','ET03444', 'ET01054','EA71763', 'ET02546','ET02946','ET01033','EA2099','ET03724','ET03725', 'ET04178', 'AF32315','ET03837','ET03723','AF32318','AF32316','EA114790','AF24173','AF3473','AF3475','EA2091','EA43973','EA8628','9901','ET02545','ET04232','AF25595','ET02944','AF00150','ET29293','ET02948','ET00304','ET00301','ET38615','AF00107','AF29097','AF00110','ET02194','EA1596','EA31712','ET02943','ET02000','ET01031','EA19541','ET04218','EA8623','ET02952','EA143135','ET00302','EA1585','EA39630','EA20749','ET01490','ET02987','AF25597','ET04237','EA1593','AF33065','ET03840','ET02587','ET04228','EA14991','EA82774','EA14990','AF00203','ET99261','ET01001','ET02999','EA2077','ET00217','ET27057','AF32319','ET02576','EA87453', 'ET04249', 'AF15450', 'EA14930', 'ET04217', 'ET04216', 'EA2073', 'AF14858', 'ET01028', 'EA151136', 'AF25613', 'ET01025', 'ET02562', 'ET01045', 'ET00001', 'ET03972', 'AF86285', 'ET00303', 'ET04096', 'AF25541', 'ET02965', 'EA2083', 'ET03841', 'AF25562', 'ET01546', 'ET01547', 'ET01545', 'AF86286', 'AF86284', 'ET02988', 'ET04206', 'ET0128', 'ET01729', 'ET04238', 'ET02956', 'ET01223', 'ET03930', 'ET02029', 'ET02001', 'ET01525', 'ET03173', 'ET01427', 'ET01024', 'EA62861', 'ET03168', 'ET02951', 'AF17839', 'EA19540', 'EA2185', 'ET02566', 'ET02563','ET04227','ET01052','ET29262','ET29259','EA14927','EA31389','ET01544','ET02607','ET03717','ET04057','EA14918','ET01044','ET4201','EA8618','ET02950','EA131079','ET04207','ET02953','EAUR98401','ET00305','AF32323','ET00308','ET02020','ET01053','ET02862','EA80543','EA8616','AF32059','ET01491','EA19539','ET51461','ET01737','ET04247','ET04107','ET04155','EA14994','ET04144','ET221','9903','ET03353','ET03966','ET03969','EA147775','ET01742','ET01499','ET482','EA85561','ET02905','ET03967','AF25578','ET03839','ET04213','AFTN108','ET02168','AFLI020','ET03838','ET01224','ET02964','ET02267','AF25559','AF25577','ET03354','44270','ET02836','EA1582','EA160942','ET02945','ET36192','ET04223','EA72193','ETBLT55','EA99968','EA75862','AF25610','ET04177','ET01013','ET29263','ET01738','AF32312','AF32299','ET37272','ET02949','ET02954','ET04176','ET02134','EA42222','ET01017','ET02958','ET02206','ET03921','ET02065','ET02564','ET04181','AF32304','ET3154','ET29264','ET02589','ET04239','ET04243','ET01019','ET01496','ET04231','ET01450','AF32313','ET03477','ET03842','ET02599','ET04179','AF00160','EA136222','ET01495','EA127946','ET02028','ET02280','ET01026','ET04210','ET01283','ET01527','ET142451','ET02017','EA8615','EA8620','EA8626','EA8622','EA8605','ET04053','EA8625','ET01736','AF8749','EA8621','EA8617','ET29300','EA8627','ET01964','ET99263','ET01433','ET01020','EA8613','ET02196','EA8624','ET01743','ET105325','ET01004','EA94696','ET04235','ET03718','EA34179','AF3474','ET04170','ET02963','ET01021','ET01440','ET00223','ET02305','ET00229','ET04199','ET02251','EA106370','ET01978','EA75869','AF00159','EA118432','ET04234','AF18483','ET132297','ET03568','AF0120','ET96044','AF26320','AF0122','AF0121','AF3249','AF25551','ET04233','AF25553','ET03932','AF18495','EA5069','EA189162','ET04089','AF22698','Shoppers','ET02005','ET02175','ET04215','ET04212','ET03706','ET04095','AF22699','AF25563','AF32322','ET01018','AF00127','ET03802','EA170230','ET04154','ET01436','ET03570','AF22995','EA128406','ET02061','ET04230','ET03073','AF8723','ET00310','ET00317','ET00318','ET00319','EA23556','EA23557','ET03100','ET99270','AF0138','ET27074','AF23842','ET36362','ET03880','AF25560','AF0520','ET701156','ET101592','AF32306','ET04254','AF32309','ET120202','EA74970','AF32314','ET99265','ET00314','ET00316','ET04248','ET02030','ET04242','AGRICOLO','ET00006','AF8705','ET04220','AF23843','ET04226','ET99266','AF25537','ET00324','ET04256','ET00325','ET02021','ET04135','ET00220','ET00221','AF86656','ET29188','ET00315','ET29189','EA129106','28408','ET04255','EA102248','AF8716','ET100305','ET04229','EA100816','ET03981','EA51341','ET04250','ET04251','AF25238','AF00089','ET04245','EA114207','ET00320','ET01999','ET04182','AF3479','AF3476','AF3478','ET03624','ET04241','ET04240','EA23855','ET04211','EA102249','ET00102','ET04253','ET13122','ET04214','AF16179','ET02077','ET04222','AF00034','ET04225','ET919191919191','AFbilancia','ET99269','AF27001','ET02268','AFLI024','ET02161','EA106245','ET03125','AF33064','AF8740','ET01428','ET04219','ET01050','EA120012','ET04183','ET00322','ET02316','EA198830','ET51342','ET03726','ET02601','ET01512','ET106660','EA87539','ET02084','ET02315','ET04221','AF25067','AFBAR2D','AF8741','ET86672'
  ]
  df_result["Categoria_articolo"] = df_result["Categoria_articolo"].apply(lambda x : "Articolo da vendita" if x in articoli_accorpare else x)

  df_result.drop(columns=['Carta', 'Negozio', 'Tavolo', 'Cassa','Operatore', 'Listino',
       'Categoria_e_codice_articolo','Codice_articolo','Tipo_IVA', 'Percentuale_tipo_IVA'],inplace=True)
  
  transazioni_mezza_giornata = pd.concat([df_result,pd.get_dummies(df_result["Categoria_articolo"])],axis=1)

  transazioni_mezza_giornata = pd.concat([transazioni_mezza_giornata,pd.get_dummies(transazioni_mezza_giornata["Coperto_AsportoDelivery"])],axis=1)

  transazioni_mezza_giornata = transazioni_mezza_giornata.groupby(["Data","Pranzo_Cena"]).sum()

  transazioni_mezza_giornata["Quantity"] = transazioni_mezza_giornata["Quantity"] -transazioni_mezza_giornata["COPERTO"] -transazioni_mezza_giornata["SISTEMA"] - transazioni_mezza_giornata["STORNI"]

  colonne_da_eliminare = ["COPERTO",'SISTEMA',"VINI.EA","VINI.OI","EAPRODGENERICO","CAFFETTERIA.OI","ATTESA","STORNI",'VARIANTI', 'VARIANTIBIRRA',
       'VARIANTICAFFE', 'VARIANTICONT.EA', 'VARIANTIGEL', 'VARIANTITAG']
  colonne_attualmente_da_eliminare = []
  for col in colonne_da_eliminare:
    if col in transazioni_mezza_giornata.columns:
      colonne_attualmente_da_eliminare.append(col)

  transazioni_mezza_giornata.drop(columns=colonne_attualmente_da_eliminare,inplace=True)

  transazioni_mezza_giornata.rename(columns={
    'Articolo da vendita':"ARTICOLO_DA_VENDITA",
    "Asporto/Delivery":"Asporto_Delivery",
    "MESDI'":"MESDI_",
    "MESDI'KARNE'":"MESDI_KARNE"
  },inplace=True)

  transazioni_mezza_giornata.reset_index(inplace=True)

  for col in transazioni_mezza_giornata.columns[8:]:
    transazioni_mezza_giornata[col] = transazioni_mezza_giornata[col].apply(int)

  transazioni_mezza_giornata["Data"] = pd.to_datetime(transazioni_mezza_giornata["Data"])

  client2 = bigquery.Client.from_service_account_json("chiave_bq.json")
  job = client2.load_table_from_dataframe(
      transazioni_mezza_giornata, "Acqua_Farina.mezze_giornate_base", job_config=None
  )
  job.result()
  client2.close()

  client3 = bigquery.Client.from_service_account_json("chiave_bq.json")
  job = client3.load_table_from_dataframe(
      df_result, "Acqua_Farina.transazioni_base", job_config=None
  )
  job.result()
  client3.close()
  ######################################################
  from datetime import datetime

  TIMESTAMP = datetime.now().strftime("%Y%m%d%H%M%S")
  PROJECT_ID = "gruppo-ethos-355909"  # @param {type:"string"}
  REGION = "europe-west6-a"  # @param {type: "string"}
  BUCKET_NAME = "bucket_pipeline_previsionale"  # @param {type:"string"}
  BUCKET_URI = "gs://" + BUCKET_NAME
  PIPELINE_ROOT = "{}/pipeline_root/bucket_pipeline_previsionale".format(BUCKET_URI)
  aip.init(project=PROJECT_ID, staging_bucket=BUCKET_URI)
  DISPLAY_NAME = "test_" + TIMESTAMP

  job = aip.PipelineJob(
      display_name=DISPLAY_NAME,
      template_path="pipeline_previsionale.json",
      pipeline_root=PIPELINE_ROOT,
      parameter_values={"message": "INIZIO"},
  )
  
  job.run()

