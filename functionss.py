from config import *
import time
from datetime import datetime
import os
import io
import glob
import pandas as pd
import numpy as np

#search for year in filepath
def get_census_year(filepath):
    if '2020' in filepath:
        return 2020
    elif '2010' in filepath:
        return 2010
    else:
        return None

# extract necessary fields from the degauss file into a dataframe based on column headers and file name
# "cleans" the dataframe to standardize expected values and formats
# creates "degauss" table in schema and inserts dataframe
#*** Note - input file expects to be in csv format. If dsv, a sep = " " argument can ber added to the read_csv statement
#*** Note - information loss possible if the Degauss file has their census block group value saved in scientific notation. Conversion from scientific notation to integer must happen prior to running
# args - ini_file_name --> database connection file, filepath --> location of degauss file, site --> site that we are loading degauss file for
def clean_and_load_degauss_to_database(ini_file_name, filepath, site):
    #confirm and open file
    if os.path.isfile(filepath):
        df_raw = pd.read_csv(filepath)
        #determine census_year
        if get_census_year(filepath) == 2020:  
            df = pd.DataFrame()
            patid = [col for col in df_raw.columns if col.lower().strip() == 'patid' or col.lower().strip() == 'id']
            census_tract = [col for col in df_raw.columns if 'tract_id_2020' in col.lower()]
            census_block_group = [col for col in df_raw.columns if 'block_group_id_2020' in col.lower()]
            df['patid'] = df_raw[patid]
            df['census_tract'] = df_raw[census_tract]
            df['census_block_group'] = df_raw[census_block_group]
            df['census_year'] = 2020
        elif get_census_year(filepath) == 2010:
            df = pd.DataFrame()
            patid = [col for col in df_raw.columns if col.lower().strip() == 'patid' or col.lower().strip() == 'id']
            census_tract = [col for col in df_raw.columns if 'tract_id_2010' in col.lower()]
            census_block_group = [col for col in df_raw.columns if 'block_group_id_2010' in col.lower()]
            df['patid'] = df_raw[patid]
            df['census_tract'] = df_raw[census_tract]
            df['census_block_group'] = df_raw[census_block_group]
            df['census_year'] = 2010
        else:
            df = pd.DataFrame()
            patid = [col for col in df_raw.columns if col.lower().strip() == 'patid' or col.lower().strip() == 'id']
            census_tract = [col for col in df_raw.columns if 'tract_id_2020' in col.lower()]
            df['patid'] = df_raw[patid]
            if len([col for col in df_raw.columns if '2020' in col.lower()]) > 0:
                census_tract = [col for col in df_raw.columns if 'tract_id_2020' in col.lower()]
                census_block_group = [col for col in df_raw.columns if 'block_group_id_2020' in col.lower()]
                df['census_tract'] = df_raw[census_tract]
                df['census_block_group'] = df_raw[census_block_group]
                df['census_year'] = 2020
            elif len(col for col in df_raw.columns if '2010' in col.lower()) > 0:
                census_tract = [col for col in df_raw.columns if 'tract_id_2010' in col.lower()]
                census_block_group = [col for col in df_raw.columns if 'block_group_id_2010' in col.lower()]
                df['census_tract'] = df_raw[census_tract]
                df['census_block_group'] = df_raw[census_block_group]
                df['census_year'] = 2010
            else:
                census_tract = [col for col in df_raw.columns if 'tract_id' in col.lower()]
                census_block_group = [col for col in df_raw.columns if 'block_group_id' in col.lower()]
                df['census_tract'] = df_raw[census_tract]
                df['census_block_group'] = df_raw[census_block_group]
                df['census_year'] = None

        # CLEANING
        if len([col for col in df_raw.columns if col.lower().strip() if 'start' in col.lower()]) > 0:
            start_date = [col for col in df_raw.columns if col.lower().strip() == 'start_date']
            df['start_date'] = df_raw[start_date]
            df['start_date'] = pd.to_datetime(df['start_date'], yearfirst = True).dt.date
        else:
            df['start_date'] = None
        
        if len([col for col in df_raw.columns if col.lower().strip() if 'end' in col.lower()]) > 0:
            end_date = [col for col in df_raw.columns if col.lower().strip() == 'end_date']
            df['end_date'] = df_raw[end_date]
            df['end_date'] = pd.to_datetime(df['end_date'], yearfirst = True).dt.date
        else:
            df['end_date'] = None

        df = df.dropna(subset =['census_tract','census_block_group'], how='all')
        df = df.dropna(subset =['patid'])

        df['patid'] = df['patid'].astype('str')
        df['patid'] = df['patid'].str.strip()
        df = df.replace({np.NaN: None})

        for index, row in df.iterrows():       
            if (not (pd.isnull(row['census_tract']))) and (row['census_tract'] != None):
                row['census_tract'] = str(row['census_tract'])
                row['census_tract'] = row['census_tract'].strip()
                row['census_tract'] = int(float(row['census_tract']))

            if (not (pd.isnull(row['census_block_group']))) and (row['census_block_group'] != None):
                row['census_block_group'] = str(row['census_block_group'])   
                row['census_block_group'] = row['census_block_group'].strip()
                row['census_block_group'] = int(float(row['census_block_group']))

        #CONNECT TO DB, CREATE TABLE, AND INSERT DATA
        conn = get_db_connection(ini_file_name)
        cur = conn.cursor()

        create_table = """create table if not exists {}_pcornet.degauss (
            patid VARCHAR(256),
            census_tract VARCHAR(256),
            census_block_group VARCHAR(256),
            start_date date,
            end_date date,
            census_year integer
            );
        """.format(site)
        cur.execute(create_table)

        if len(df) > 0:
            df_columns = list(df)
            columns = ",".join(df_columns)

            # create VALUES('%s', '%s",...) one '%s' per column
            values = "VALUES({})".format(",".join(["%s" for _ in df_columns])) 
            print(columns)
            print(values)
            #create INSERT INTO table (columns) VALUES('%s',...)
            insert_stmt = "INSERT INTO {}_pcornet.degauss ({}) {}".format(site,columns,values)

            cur = conn.cursor()
            psycopg2.extras.execute_batch(cur, insert_stmt, df.values)
            conn.commit()

            #REMOVE UNWANTED DECIMALS
            clean_decimal = """begin;UPDATE {}_pcornet.degauss Set census_block_group = ((census_block_group::numeric)::bigint)::varchar;commit;""".format(site)
            cur.execute(clean_decimal)
            cur.close()
            return 'successfully inserted data'
    
    else:
        return 'invalid file'

#drop patids from degauss table that do not exist in demographic table
def drop_orphan_patid(site, ini_file_name):
    
    conn = get_db_connection(ini_file_name)
    cur = conn.cursor()

    drop_orphans = """ begin;
        delete from {}_pcornet.degauss 
        where patid not in (select patid from {}_pcornet.demographic);
        commit;
        """.format(site,site)
    cur.execute(drop_orphans)
    cur.close()

#if there are no dates in degauss table then simplify to only have 1 degauss record per patid
def drop_duplicate_patid(site, ini_file_name):
    conn = get_db_connection(ini_file_name)
    cur = conn.cursor()

    no_dates = """select count(*) from {}_pcornet.degauss where start_date is null;""".format(site)
    all_records = """select count(*) from {}_pcornet.degauss;""".format(site)
    cur.execute(no_dates)
    null_date_count = cur.fetchone()[0]
    cur.execute(all_records)
    all_records_count = cur.fetchone()[0]

    if null_date_count == all_records_count:
        first_patid = """begin;
            create table if not exists {}_pcornet.degauss_rank as 
            select *, row_number() over(partition by patid order by patid) as row_num
            from {}_pcornet.degauss;
            commit;
            
            begin;
            delete from {}_pcornet.degauss_rank
            where row_num <> 1;
            commit;

            begin;
            drop table {}_pcornet.degauss;
            commit;

            begin;
            ALTER TABLE {}_pcornet.degauss_rank RENAME TO degauss;
            commit;
        """.format(site,site,site,site,site)
        cur.execute(first_patid)
        cur.close()

#create table that joins together lds_address_history AND degauss table
def link_to_address_id(site, ini_file_name):
    
    conn = get_db_connection(ini_file_name)
    cur = conn.cursor()
    query = """create sequence if not exists {}_pcornet.degauss_seq;

        begin;
        create table {}_pcornet.degauss_lds_join as 
        select 
            dg.PATID as dg_patid,
            dg.census_tract,
            dg.census_block_group,
            coalesce(dg.census_block_group, dg.census_tract) as census_code,
            dg.start_date,
            dg.end_date,
            dg.census_year,
            coalesce(lds.ADDRESSID, 'degauss_' || nextval('{}_pcornet.degauss_seq')) as addressid,
            lds.patid as lds_patid,
            lds.ADDRESS_PERIOD_START,
            lds.ADDRESS_PERIOD_END
        from
            {}_pcornet.degauss dg
        left join
            {}_pcornet.lds_address_history lds
            on dg.patid = lds.patid;
        commit;

     """.format(site,site,site,site,site)
    cur.execute(query)
    cur.close()

# create private address geocode, lds_address_history, and private_address_history records
# for degauss records that do not have a matching patid in the lds_address_history table
def no_patid_no_date(site, ini_file_name):
    
    conn = get_db_connection(ini_file_name)
    cur = conn.cursor()

    query = """begin;
        insert into {}_pcornet.lds_address_history (
            ADDRESSID,
            PATID,
            ADDRESS_USE,
            ADDRESS_TYPE,
            ADDRESS_PREFERRED,
            ADDRESS_PERIOD_START,
            ADDRESS_PERIOD_END
        )
        select
            addressid as ADDRESSID,
            dg_patid as PATID,
            'HO' as ADDRESS_USE,
            'NI' as ADDRESS_TYPE,
            'Y' as ADDRESS_PREFERRED,
            coalesce(start_date,'9999-12-31')::timestamp as ADDRESS_PERIOD_START,
            coalesce(end_date, '9999-12-31')::timestamp as ADDRESS_PERIOD_END
        from
            {}_pcornet.degauss_lds_join dg
        where
            lds_patid is null;
        commit;

        begin;
        insert into {}_pcornet.private_address_history (
            ADDRESSID,
            PATID,
            ADDRESS_USE,
            ADDRESS_TYPE,
            ADDRESS_PREFERRED,
            ADDRESS_PERIOD_START,
            ADDRESS_PERIOD_END
        )
        select
            addressid as ADDRESSID,
            dg_patid as PATID,
            'HO' as ADDRESS_USE,
            'NI' as ADDRESS_TYPE,
            'Y' as ADDRESS_PREFERRED,
            coalesce(start_date, '9999-12-31')::timestamp as ADDRESS_PERIOD_START,
            coalesce(start_date, '9999-12-31')::timestamp as ADDRESS_PERIOD_END
        from
            {}_pcornet.degauss_lds_join dg
        where
            lds_patid is null;
        commit;

        begin;
        create sequence if not exists {}_pcornet.geocode_seq;
        commit;

        begin;
        insert into {}_pcornet.private_address_geocode (
            geocodeid,
            addressid,
            geocode_state,
            geocode_county,
            geocode_tract,
            geocode_group,
            geocode_block,  
            geocode_custom_text,
            shapefile
        )
        select 
            'degauss_' || nextval('{}_pcornet.geocode_seq') as geocodeid,
            addressid as addressid,
            substring(census_code,1,2) as geocode_state,
            substring(census_code,3,3) as geocode_county,
            substring(census_code,6,6) as geocode_tract, 
            case 
                when length(census_code) < 12 then NULL
                else substring(census_code,12,1) 
            end as geocode_group,
            NULL as geocode_block,
            census_year::varchar as geocode_custom_text,
            census_year::varchar as shapefile
        from
            {}_pcornet.degauss_lds_join dg
        where
            lds_patid is null;
        commit;
    """.format(site,site,site,site,site,site,site,site)

    cur.execute(query)
    cur.close()

# create private address geocode, and private_address_history records
# for degauss records that have a matching patid in the lds_address_history table but do not have date ranges
def yes_patid_no_date(site, ini_file_name):
    
    conn = get_db_connection(ini_file_name)
    cur = conn.cursor()

    query = """begin;
       create table {}_pcornet.degauss_max_date as 
        select 
            distinct on (dg_patid) dg_patid, 
            addressid, 
            census_tract,
            census_block_group,
            census_code,
            start_date,
            end_date,
            census_year,
            lds_patid,
            ADDRESS_PERIOD_START,
            ADDRESS_PERIOD_END
        from {}_pcornet.degauss_lds_join
        order by dg_patid, ADDRESS_PERIOD_START desc;
        COMMIT;

        begin;
        insert into {}_pcornet.private_address_history (
        ADDRESSID,
        PATID,
        ADDRESS_USE,
        ADDRESS_TYPE,
        ADDRESS_PREFERRED,
        ADDRESS_PERIOD_START,
        ADDRESS_PERIOD_END
        )
        select 
            ADDRESSID,
            PATID,
            ADDRESS_USE,
            ADDRESS_TYPE,
            ADDRESS_PREFERRED,
            ADDRESS_PERIOD_START,
            ADDRESS_PERIOD_END
        from
            {}_pcornet.lds_address_history
        where addressid in (select addressid from {}_pcornet.degauss_max_date where start_date is null and lds_patid is not null);
        commit;
        
        begin;
        create sequence if not exists {}_pcornet.geocode_seq;
        commit;

        begin;
        insert into {}_pcornet.private_address_geocode (
            geocodeid,
            addressid,
            geocode_state,
            geocode_county,
            geocode_tract,
            geocode_group,
            geocode_block,  
            geocode_custom_text,
            shapefile
        )
        select 
            'degauss_' || nextval('{}_pcornet.geocode_seq') as geocodeid,
            addressid as addressid,
            substring(census_code,1,2) as geocode_state,
            substring(census_code,3,3) as geocode_county,
            substring(census_code,6,6) as geocode_tract, 
            case 
                when length(census_code) < 12 then NULL
                else substring(census_code,12,1) 
            end as geocode_group,
            NULL as geocode_block,
            census_year::varchar as geocode_custom_text,
            census_year::varchar as shapefile
        from
            {}_pcornet.degauss_max_date dg
        where
            start_date is null
            and lds_patid is not null;
        commit;
    """.format(site,site,site,site,site,site,site,site,site)

    cur.execute(query)
    cur.close()
     
# create private address geocode, and private_address_history records
# for degauss records that have a matching patid in the lds_address_history table and have date ranges in source file
def yes_patid_yes_date(site, ini_file_name):
    
    conn = get_db_connection(ini_file_name)
    cur = conn.cursor()

    query = """begin;
        insert into {}_pcornet.private_address_history (
        ADDRESSID,
        PATID,
        ADDRESS_USE,
        ADDRESS_TYPE,
        ADDRESS_PREFERRED,
        ADDRESS_PERIOD_START,
        ADDRESS_PERIOD_END
        )
        select 
            lds.ADDRESSID,
            lds.PATID,
            lds.ADDRESS_USE,
            lds.ADDRESS_TYPE,
            lds.ADDRESS_PREFERRED,
            lds.ADDRESS_PERIOD_START,
            lds.ADDRESS_PERIOD_END
        from
             {}_pcornet.degauss_lds_join degauss
        inner join
            {}_pcornet.lds_address_history lds
            on degauss.dg_patid = lds.patid
            and degauss.start_date = lds.ADDRESS_PERIOD_START
            and degauss.addressid = lds.addressid
        where 
            degauss.start_date is not null;
        commit;
        
        begin;
        create sequence if not exists {}_pcornet.geocode_seq;
        commit;

        begin;
        insert into {}_pcornet.private_address_geocode (
            geocodeid,
            addressid,
            geocode_state,
            geocode_county,
            geocode_tract,
            geocode_group,
            geocode_block,  
            geocode_custom_text,
            shapefile
        )
        select 
            'degauss_' || nextval('{}_pcornet.geocode_seq') as geocodeid,
            degauss.addressid as addressid,
            substring(census_code,1,2) as geocode_state,
            substring(census_code,3,3) as geocode_county,
            substring(census_code,6,6) as geocode_tract, 
            case 
                when length(census_code) < 12 then NULL
                else substring(census_code,12,1)
            end as geocode_group,
            NULL as geocode_block,
            census_year::varchar as geocode_custom_text,
            census_year::varchar as shapefile
        from
             {}_pcornet.degauss_lds_join degauss
        inner join
            {}_pcornet.lds_address_history lds
            on degauss.dg_patid = lds.patid
            and degauss.start_date = lds.ADDRESS_PERIOD_START
            and degauss.addressid = lds.addressid
        where
            start_date is not null
            and lds_patid is not null;
        commit;
    """.format(site,site,site,site,site,site,site,site)

    cur.execute(query)
    cur.close()

def delete_temp_tables(site, ini_file_name):
    conn = get_db_connection(ini_file_name)
    cur = conn.cursor()

    query = """begin;
        drop table if exists {}_pcornet.degauss;
        commit;
        begin;
        drop table if exists {}_pcornet.degauss_rank;
        commit;
        begin;
        drop table if exists {}_pcornet.degauss_lds_join;
        commit;
        begin;
        drop table if exists {}_pcornet.degauss_max_date;
        commit;
    """.format(site,site,site,site)

    cur.execute(query)
    cur.close()
