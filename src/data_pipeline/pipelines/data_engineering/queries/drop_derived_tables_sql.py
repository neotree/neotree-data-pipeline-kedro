def drop_derived_tables():
    return ''' DROP SCHEMA IF EXISTS derived CASCADE;
               CREATE SCHEMA derived;'''