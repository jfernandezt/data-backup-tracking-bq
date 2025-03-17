from datetime import datetime
from google.cloud import bigquery, storage
from datetime import datetime
from sendAlert import function_alerts
import json
import os


def get_list_table_backup(path_list):
    try:
        with open(path_list) as f:
            data = json.load(f)
            return data
    except FileNotFoundError:
        print("El archivo de configuracion:{} no fue encontrado.".format(path_list))
        exit(1)

def get_dll_table(client, project_id, dataset_id, table_id):
    file_template_dd = open ('dll_template.sql', mode='r')
    sql_dll_template = file_template_dd.read()
    sql_dll = sql_dll_template.format(project_id=project_id, dataset_id=dataset_id, table_id=table_id)
    query_job = client.query(sql_dll)
    result = query_job.result()
    for row in result:
        str_dll = row.ddl
    return str_dll

def count_table_rows(client, project_id, dataset_id, table_id):
    query = f"""
    SELECT COUNT(*) as row_count
    FROM `{project_id}.{dataset_id}.{table_id}`
    """
    query_job = client.query(query)
    result = query_job.result()
    for row in result:
        return row.row_count

def export_tables_to_gcs(bucket_id, project_id, dataset_id, table_id, location_id, fec_backup, tipo_carga, today):
    destination_uri = f"gs://{bucket_id}/{project_id}/{dataset_id}/{table_id}/fec_backup={fec_backup}/{dataset_id}_{table_id}_*.parquet"

    try:
        client = bigquery.Client(project=project_id)
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        str_dll = get_dll_table(client, project_id, dataset_id, table_id)
        extract_job = client.extract_table(
            table_ref,
            destination_uri,
            location=location_id,  # Ubicación de la tabla
            job_config=bigquery.job.ExtractJobConfig(destination_format="PARQUET")
        )

        extract_job.result()  # Esperar a que el trabajo finalice
        count_table = count_table_rows(client, project_id, dataset_id, table_id)
        status = 'SUCCESSFUL'
    except Exception as e:
        count_table = None
        status = 'ERROR:{}'.format(e)

    frecuencia = '{}:{}'.format(tipo_carga, today)
    hora = datetime.now()
    str_fechahora = '{} {}'.format(fec_backup, hora.strftime('%H:%M:%S'))

    export_result = {'ddl_table': str_dll,
                     'bucket_id': bucket_id, 
                     'project_id': project_id, 
                     'dataset_id': dataset_id, 
                     'table_id': table_id,
                     'tipo_carga': tipo_carga,
                     'frecuencia': frecuencia,
                     'location_id': location_id,
                     'count_table': count_table,
                     'status': status,
                     'fec_backup': fec_backup,
                     'fec_hora_backup': str_fechahora}  
    return export_result

def generate_backup_log(project_id, dataset_id, table_id, list_dict):
    client = bigquery.Client(project=project_id)
    with open ("respuesta_back.txt", "w") as jsonwrite:
        for item in list_dict:
            jsonwrite.write(json.dumps(item) + '\n')     #newline delimited json file
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = True
    with open("respuesta_back.txt", "rb") as source_file:
        job = client.load_table_from_file(
            source_file,
            table_ref,
            location="us",  # Must match the destination dataset location.
            job_config=job_config,
        )  # API request
    job.result()  # Waits for table load to complete.
    print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))


def generate_backup_to_storage(path_list, fec_backup):
    tables = get_list_table_backup(path_list)
    results = []
    date_fec_carga = datetime.strptime(fec_backup, '%Y-%m-%d').date()
    
    try:
        carga = tables['daily']
        today = date_fec_carga.strftime('%d')
        for t in carga['tables']:
            result = export_tables_to_gcs(t['bucket_id'], t['project_id'], t['dataset_id'], t['table_id'], t['location_id'], fec_backup, 'daily', today)
            results.append(result)
    except Exception as e:
        print('No existe configuracion para backup diario')

    try:
        carga = tables['weekly']
        frecuency = carga['frecuency']
        today = date_fec_carga.strftime('%A')
        if frecuency.lower()==today.lower():            
            for t in carga['tables']:
                result = export_tables_to_gcs(t['bucket_id'], t['project_id'], t['dataset_id'], t['table_id'], t['location_id'], fec_backup, 'weekly', today)
                results.append(result)
        else:
            print('No hay cargas semanales para el dia de hoy.')
    except Exception as e:
        print('No existe configuracion para backup semanal')

    try:
        carga = tables['monthly']
        frecuency = carga['frecuency']
        today = date_fec_carga.strftime('%d')
        if int(frecuency)==int(today):  
            for t in carga['tables']:
                result = export_tables_to_gcs(t['bucket_id'], t['project_id'], t['dataset_id'], t['table_id'], t['location_id'], fec_backup, 'monthly', today)
                results.append(result)
        else:
            print('No hay cargas mensuales para el dia de hoy.')
    except Exception as e:
        print('No existe configuracion para backup mensual')
    return results

def generate_reporte(project_id):
    client = bigquery.Client(project=project_id)
    f_config_reporte = open('reporte.json') 
    data = json.load(f_config_reporte)
    #lista daily
    query_job = client.query(data['sql_lista_daily'])
    result = query_job.result()
    for row in result:
        lista_daily = row.lista
    str_query_daily = data['sql_reporte_daily'].format(lista=lista_daily)
    print(str_query_daily)
    #lista weekly
    query_job = client.query(data['sql_lista_weekly'])
    result = query_job.result()
    for row in result:
        lista_weekly = row.lista    
    str_query_weekly = data['sql_reporte_weekly'].format(lista=lista_weekly)
    print(str_query_weekly)
    #lista monthly
    query_job = client.query(data['sql_lista_monthly'])
    result = query_job.result()
    for row in result:
        lista_monthly = row.lista    
    str_query_monthly = data['sql_reporte_monthly'].format(lista=lista_monthly)
    print(str_query_monthly)
    parameters = {'project': project_id,
                  'from_email': {'name': 'Backups Datalake',
                                 'email': 'comunicaciones@interseguro.com.pe'
                                },
                  'to_email':[{
                               'email': 'john.huerta@interseguro.com.pe',
                               'name': 'John Huerta'
                               }
                  ],
                  'subject': 'Backup Datalake',
                  'html_content': '<html>Se detalla los backups:</html>',
                  'sql_daily': str_query_daily,
                  'sql_weely': str_query_weekly,
                  'sql_monthly': str_query_monthly
                  }    
    function_alerts(parameters)

def main():
    project_id = 'iter-data-storage-pv'
    dataset_id = 'temp'
    table_id = 'log_backup_tabla'
    rpta = generate_backup_to_storage('table_backup.json', '2025-03-08')
    generate_backup_log(project_id, dataset_id, table_id, rpta)  
    generate_reporte(project_id)
  

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/desa/Interseguro/Desarrollo/account_services/iter-data-storage-pv.json"

if __name__ == "__main__":
    main()