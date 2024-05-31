import json
import ast
from collections import defaultdict


source_schema = "RPT_EDM"
target_schema = "WC_DATA_MART"

unknown_schema_name = "RPT_EDM"

def get_crawler_in_json():
    crawler_json = defaultdict(list)
    with open('crawler.txt','r') as file:
        datas = file.readlines()
        
    for data in datas:
        data = data.strip()
        table_list = ast.literal_eval(data)
        for table in table_list:
            if not table:
                continue
            if '.' not in table:
                if table.lower().endswith('dim') or table.lower().endswith('fact'):
                    crawler_json['WC_DATA_MART'].append(table)
                elif table.lower().endswith('coverage'):
                    crawler_json['RPT_EDM'].append(table)
                else:
                    crawler_json['unknown_schema'].append(table)
            else:
                if 'v2' in table.lower():
                    table_name = table.lower().split('v2.')[1]
                    if table_name.lower().endswith('dim') or table_name.lower().endswith('fact'):
                        crawler_json['v2'].append(f'WC_DATA_MART.v2.{table_name}')
                    else:
                        crawler_json['v2'].append(f'v2.{table_name}')
                
                elif 'v3' in table.lower():
                    table_name = table.lower().split('v3.')[1]
                    if table_name.lower().endswith('dim') or table_name.lower().endswith('fact'):
                        crawler_json['v3'].append(f'WC_DATA_MART.v3.{table_name}')
                    else:
                        crawler_json['v3'].append(f'v3.{table_name}')

                else:
                    *schemas, table_name = table.split('.')

                    if any('source' in element.lower() for element in schemas):
                        crawler_json[source_schema].append(table_name)
                    elif any('target' in element.lower() for element in schemas):
                        crawler_json[target_schema].append(table_name)
                    else:
                        crawler_json['unknown_schema'].append(table_name)


    return crawler_json




def save_sql_query(crawler_dict, sql_server=True):
    sql_text = ''
    dot = '..' if sql_server else '.'
    for schema,tables in crawler_dict.items():
        if schema not in ['v2','v3','unknown_schema']:
            tables = set(tables)
            for table in tables:
                sql_text += f"select '{table.lower()}' as name, count(*) as total_count,(select count(*) from {schema}{dot}{table} where cast(modified_date as date) = cast((select max(modified_date) from {schema}{dot}{table}) as date)) as max_modified_date_count from {schema}{dot}{table} union all\n"
            

    for schema, tables in crawler_dict.items():
        if not schema == "unknown_schema":
            continue
        sql_text += '\n-- UNKNOWN SCHEMAS, SO PLEASE DOUBLE CHECK THESE !!\n\n'
        tables = set(tables)
        for table in tables:
            sql_text += f"select '{table.lower()}' as name, count(*) as total_count,(select count(*) from {unknown_schema_name}{dot}{table} where cast(modified_date as date) = cast((select max(modified_date) from {unknown_schema_name}{dot}{table}) as date)) as max_modified_date_count from {unknown_schema_name}{dot}{table} union all\n"
            


    sql_text += '-- V2 and V3 SCHEMAS, SO PLEASE DOUBLE CHECK THESE !!\n'
    for v2_v3_schema, tables in crawler_dict.items():
        if not v2_v3_schema in ['v2','v3']:
            continue
        tables = set(tables)
        replacer = 'v2_' if v2_v3_schema == 'v2' else 'v3_'
        replaced = 'v2.' if v2_v3_schema == 'v2' else 'v3.'
        for table in tables:
            sql_text += f"select '{table.lower()}' as name, count(*) as total_count,(select count(*) from {table} where cast(modified_date as date) = cast((select max(modified_date) from {table}) as date)) as max_modified_date_count from {table} union all\n" if sql_server else f"select '{table.lower()}' as name, count(*) as total_count,(select count(*) from {table.replace(replaced,replacer)} where cast(modified_date as date) = cast((select max(modified_date) from {table.replace(replaced,replacer)}) as date)) as max_modified_date_count from {table.replace(replaced,replacer)} union all\n"
            


    sql_text = sql_text[:-10] + 'order by name asc;'

    with open('output.txt','w') as file:
        file.write(sql_text)


if __name__ == "__main__":
    crawler_dict = get_crawler_in_json()
    save_sql_query(crawler_dict=crawler_dict,sql_server=False)