import json
import azure.functions as func
import pandas as pd 


from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from pandas import errors as er
from io import BytesIO


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        path = body['path']
        container = body['container']
    except KeyError:
         return func.HttpResponse(
            json.dumps(
                {
                    "Result":"Faltan parametros en el request",
                    "Status":403
                }
            )
        )
    try: 

        file_content = readFile(container,path)
        df_source = pd.read_csv(BytesIO(file_content),sep=',')

        #Generar dataframe de person
        df_person = df_source[["first_name","last_name"]].copy()
        df_person.insert(0, 'id_person', range(1, 1 + len(df_person)))
        person_file = df_person.to_csv(index=False,sep=',',encoding = "utf-8")
        uploadFile('person',person_file)

        #Generar dataframe de address_person
        df_address_person = df_source[["address","city","county"]].copy()
        df_address_person.insert(0, 'id_address', range(1, 1 + len(df_address_person)))
        df_address_person_all = pd.concat([df_person['id_person'],df_address_person], axis = 1)
        df_address_person_all = df_address_person_all[['id_address', 'address', 'city', 'county', 'id_person']]
        address_person_file = df_address_person_all.to_csv(index=False,sep=',',encoding = "utf-8")
        uploadFile('address_person',address_person_file)

        #Generar dataframe de person_information
        df_person_information = df_source[["phone1","phone2","email"]].copy() 
        df_person_information.insert(0, 'id_information', range(1, 1 + len(df_person_information)))
        df_person_information_all = pd.concat([df_person['id_person'],df_person_information], axis = 1)
        df_person_information_all = df_person_information_all[['id_information', 'phone1', 'phone2', 'email', 'id_person']]
        person_information_file = df_person_information_all.to_csv(index=False,sep=',',encoding = "utf-8")
        uploadFile('person_information',person_information_file)

        #Generar dataframe company
        df_company = df_source[["company_name","state","zip","web"]].copy() 
        df_company.insert(0, 'id_company', range(1, 1 + len(df_company)))
        df_company_all = pd.concat([df_person['id_person'],df_company], axis = 1)
        df_company_all = df_company_all[['id_company', 'company_name', 'state', 'zip', 'web', 'id_person']]
        company_file = df_company_all.to_csv(index=False,sep=',',encoding = "utf-8")
        uploadFile('company',company_file)

        return func.HttpResponse(
                json.dumps(
                    {
                        "Result":"Success.",
                        "Status":200
                    }
                )
            ) 

    except ResourceNotFoundError as e:
        return func.HttpResponse(
            json.dumps(
                {
                    "Result":"Archivo no encontrado en Datalake.",
                    "Status":403
                }
            )
        )
    
    except er.ParserError as e: 
        return func.HttpResponse(
            json.dumps(
                {
                    "Result": "Archivo invalido.",
                    "Errors": "Una o mas lineas contienen mas campos de los especificados en el encabezado.",
                    "Status": 200
                }
            )
        )
    
    # Ups! something else
    except Exception as e:
        return func.HttpResponse(
            json.dumps(
                {
                    "Result":f"Error inesperado:{e.args}.",
                    "Status":500
                }
            )
        )
def readFile(container, path_blob):

    connect_str="DefaultEndpointsProtocol=https;AccountName=adlspruebatecnica;AccountKey=FmuC4mPeaYTxfGA9EbqoB7B/MtoMydFoTGwVgBtH9AGFVcNVKTah1iwsjjmAJW319jzTuvbKGzK2+AStAcXD6Q==;EndpointSuffix=core.windows.net"
    
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    
    blob_client = blob_service_client.get_blob_client(container=container,blob=path_blob)
    
    file_content = blob_client.download_blob().content_as_bytes()

    return file_content


def uploadFile(name_file,file):

    # Connect to storage
    blob_service_client  = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=adlspruebatecnica;AccountKey=FmuC4mPeaYTxfGA9EbqoB7B/MtoMydFoTGwVgBtH9AGFVcNVKTah1iwsjjmAJW319jzTuvbKGzK2+AStAcXD6Q==;EndpointSuffix=core.windows.net")
    #Conteiner Path 
    container_client_clean = blob_service_client.get_container_client(f'tranform')

    # Name File 
    blob_client_clean = container_client_clean.get_blob_client(f'{name_file}.csv')
        
    # Upload to blob storage
    blob_client_clean.upload_blob( file, blob_type="BlockBlob", overwrite=True )  