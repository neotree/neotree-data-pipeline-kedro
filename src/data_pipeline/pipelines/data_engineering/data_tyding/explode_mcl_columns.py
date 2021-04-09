from conf.common.sql_functions import create_table,append_data
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
# this function explodes all mcl columns and creates respective tables
# This input is a set of columns that need to be exploded
# no output but tables in postgres are created, tables names are the same as mcl column titles


def explode_column(df, mcl):
    
    for c in mcl:
        # loop to explode all mcl columns in list 
        if str(c).endswith('Oth'): 
            pass;
        else:
            column = (c + '.label')
            mcl_column = df[[column]] 
            mcl_column_exp = mcl_column.explode(column)
            mcl_column_exp['uid'] = df['uid']
            #Drop All Rows With Label Other, The Values Will Be Collected In The Method Explode_Other_Columns
            mcl_column_exp = mcl_column_exp.loc[(mcl_column_exp[column] != "Other") & (mcl_column_exp[column].notna())]
            mcl_column_exp.set_index('uid')
            mcl_column_exp.reindex(columns =mcl_column_exp.columns)
            column_name = ("exploded_"+column)
            schema = 'derived'
            table_exists(schema,column_name)
            create_table(mcl_column_exp, column_name)

def explode_other_column(df,mcl):

    for c in mcl:
        if str(c).endswith('Oth'):
            column = (c + '.value')
            #Parent Column To Be Used For Appending Data To An Existing Table Rather Than Creating A New Table
            parent_column = (str(c).replace("Oth","")+'.label');
            mcl_column = df[[column]] 
            mcl_column_exp = mcl_column.explode(column)
            mcl_column_exp['uid'] = df['uid']
            mcl_column_exp[parent_column] = mcl_column_exp[column]
            mcl_column_exp = mcl_column_exp.loc[(mcl_column_exp[column].notna())]
            mcl_column_exp.set_index('uid')
            mcl_column_exp.drop(column,axis='columns',inplace=True)
            mcl_column_exp.reindex(columns =mcl_column_exp.columns)
            column_name = ("exploded_"+parent_column)
            schema = 'derived'
            table_exists(schema,column_name)
            append_data(mcl_column_exp, column_name)
        else:
            pass;