        
def rearrange_columns_using_sftable(snowflake_table,schema,csv_df,project_details='Default'):
    """This function uses snowflake connector to rearranges columns of the dataframe based on the original order in the table"""
    try:          
        if (snowflake_table) and (schema):
            sqlstmnt = 'SELECT COLUMN_NAME FROM "VAW_AMER_DEV_PUB"."INFORMATION_SCHEMA"."COLUMNS" '\
                        "WHERE TABLE_NAME = '"+ str(snowflake_table) +"'"\
                        "AND TABLE_SCHEMA = '" + str(schema)+"'"\
                        "ORDER BY ORDINAL_POSITION;"
            conn = connect_to_snowflake(schema)
            table_df = execute_sqlstmt(conn,sqlstmnt)
            order_of_columns = list(table_df['COLUMN_NAME'])

            
            csv_df_columns = list(csv_df.columns)

            check =  all(item in csv_df_columns for item in order_of_columns)

            if not check:
                missing_columns_in_csv_df = list(set(order_of_columns).difference(csv_df_columns))
                missing_columns_in_table = list(set(csv_df_columns).difference(order_of_columns))
                if missing_columns_in_csv_df:
                    raise Exception(f"Following columns are missing in csv_df: {missing_columns_in_csv_df}")
                elif missing_columns_in_table:
                    raise Exception(f"Following columns are missing in table: {missing_columns_in_table}")

            else:
                csv_df = csv_df.reindex(columns=order_of_columns)
                print(f"The order of columns in csv_df {csv_df_columns} have been rearranged to match the order in table {order_of_columns}")
                return csv_df
        else:
            raise Exception(f"Following snowflake table name and schema name are missing or doest not exists")
    except Exception as e:
        print(f"rearrange_columns_using_sftable failed: {str(e)}")
        
        
        
def rearrange_columns_using_dataset(dataset,csv_df,project_details='Default'):
    """This function uses bultin dataiku object to rearranges columns of the dataframe based on the original order in the table"""
    try:
        client = dataiku.api_client()
        if project_details=='Default':
            project = client.get_default_project()
        else:
            project = client.get_project(project_details)
            
        schema = project.get_dataset(dataset).get_schema()
        order_of_columns = [column['name'] for column in schema['columns']]
        csv_df_columns = list(csv_df.columns)

        check =  all(item in csv_df_columns for item in order_of_columns)

        if not check:
            missing_columns_in_csv_df = list(set(order_of_columns).difference(csv_df_columns))
            missing_columns_in_dataset = list(set(csv_df_columns).difference(order_of_columns))
            if missing_columns_in_csv_df:
                raise Exception(f"Following columns are missing in csv_df: {missing_columns_in_csv_df}")
            elif missing_columns_in_dataset:
                raise Exception(f"Following columns are missing in dataset: {missing_columns_in_dataset}")
                
        else:
            csv_df = csv_df.reindex(columns=order_of_columns)
            print(f"The order of columns in csv_df {csv_df_columns} have been rearranged to match the order in dataset {order_of_columns}")
            return csv_df
    except Exception as e:
        print(f"rearrange_columns failed: {str(e)}")