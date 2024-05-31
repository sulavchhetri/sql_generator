import ast
import argparse
from collections import defaultdict

def get_crawler_in_json(source_schema, target_schema, unknown_schema_name):
    crawler_json = defaultdict(list)
    with open('crawler.txt', 'r') as file:
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


def save_sql_query(crawler_dict, sql_server, unknown_schema_name):
    sql_text = ''
    dot = '..' if sql_server else '.'
    for schema, tables in crawler_dict.items():
        if schema not in ['v2', 'v3', 'unknown_schema']:
            tables = set(tables)
            for table in tables:
                sql_text += f"select '{table.lower()}' as name, count(*) from {schema}{dot}{table} union all\n"

    for schema, tables in crawler_dict.items():
        if schema != "unknown_schema":
            continue
        sql_text += '\n-- UNKNOWN SCHEMAS, SO PLEASE DOUBLE CHECK THESE !!\n\n'
        tables = set(tables)
        for table in tables:
            sql_text += f"select '{table.lower()}' as name, count(*) from {unknown_schema_name}{dot}{table} union all\n"

    sql_text += '\n-- V2 and V3 SCHEMAS, SO PLEASE DOUBLE CHECK THESE !!\n\n'
    for v2_v3_schema, tables in crawler_dict.items():
        if v2_v3_schema not in ['v2', 'v3']:
            continue
        tables = set(tables)
        replacer = 'v2_' if v2_v3_schema == 'v2' else 'v3_'
        replaced = 'v2.' if v2_v3_schema == 'v2' else 'v3.'
        for table in tables:
            sql_text += f"select '{table.lower()}' as name, count(*) from {table} union all\n" if sql_server else f"select '{table.lower()}' as name, count(*) from {table.replace(replaced, replacer)} union all\n"

    sql_text = sql_text[:-10] + 'order by name asc;'

    with open('output.txt', 'w') as file:
        file.write(sql_text)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process crawler data and generate SQL queries.")
    parser.add_argument('--source_schema', type=str, required=True, help='Source schema name')
    parser.add_argument('--target_schema', type=str, required=True, help='Target schema name')
    parser.add_argument('--default', type=str, default='', help='Default schema name for unknown tables')
    parser.add_argument('--sql_server', type=bool, default=True, help='Set to True for SQL Server format, False for Snowflake format')

    args = parser.parse_args()

    crawler_dict = get_crawler_in_json(args.source_schema, args.target_schema, args.default)
    save_sql_query(crawler_dict=crawler_dict, sql_server=args.sql_server, unknown_schema_name=args.default)
