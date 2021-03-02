from conf.common.sql_functions import create_table
# this function explodes all mcl columns and creates respective tables
# This input is a set of columns that need to be exploded
# no output but tables in postgres are created, tables names are the same as mcl column titles


def explode_column(df, mcl):
    
    for c in mcl:
        # loop to explode all mcl columns in list
        column = (c + '.label')
        mcl_column = df[[column]]
        mcl_column_exp = mcl_column.explode(column)
        column_name = ("exploded_"+column)
        create_table(mcl_column_exp, column_name)
