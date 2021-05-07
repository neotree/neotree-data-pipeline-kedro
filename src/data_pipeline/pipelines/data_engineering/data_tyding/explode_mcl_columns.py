from conf.common.sql_functions import create_exploded_table,append_data
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
# this function explodes all mcl columns and creates respective tables
# This input is a set of columns that need to be exploded
# no output but tables in postgres are created, tables names are the same as mcl column titles


def explode_column(df, mcl,db):
    created_tables = []
    for c in mcl:
        # loop to explode all mcl columns in list 
        parent_column = None
        column = None
        if str(c).endswith('Oth'): 
            column = (c + '.value')
            #Parent Column To Be Used For Appending Data To An Existing Table Rather Than Creating A New Table
            parent_column = (str(c).replace("Oth","")+'.label');

        else:
            column = (c + '.label')
            parent_column = column
        if parent_column is not None and column is not None:
            mcl_column = df[[column]] 
            mcl_column_exp = mcl_column.explode(column)
            mcl_column_exp['uid'] = df['uid']
            mcl_column_exp['facility'] = df['facility']
            mcl_column_exp[parent_column] = mcl_column_exp[column]
            #Drop All Rows With Label Other, The Values Will Be Collected In The Method Explode_Other_Columns
            mcl_column_exp = mcl_column_exp.loc[(mcl_column_exp[column] != "Other") &
             (mcl_column_exp[column].notna()) & (mcl_column_exp[column] is not None)]
            mcl_column_exp.set_index('uid')
            column_name = ("exploded_"+db+parent_column)
            schema = 'derived'
            if str(c).endswith('Oth'):
                mcl_column_exp.drop(column,axis='columns',inplace=True)
            mcl_column_exp.reindex(columns =mcl_column_exp.columns)
            #Check If Table Has Already been Created: To Be Used To Append 'Other' Values
            if column_name in created_tables:
               append_data(mcl_column_exp, column_name) 
            else: 
               create_exploded_table(mcl_column_exp, column_name)
            #To Be Used To Track Already Created Tables So As To Avoid Trying To Recreate A Table Or Append Data To A Non Existing Table    
            created_tables.append(column_name)

            

