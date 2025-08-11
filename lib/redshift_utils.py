import os
import psycopg2
import csv
import gzip
import decimal
import glob

import s3_utils as s3


# SET UP LOGGING
import logging
from pythonjsonlogger import jsonlogger


formatter = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(message)s')
logHandler = logging.StreamHandler()
logHandler.setFormatter(formatter)
logger = logging.getLogger('rs_logger')
logger.propagate = False
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)
logger.info('Starting job')

'''Functions for connecting to redshift, reading sql and writing to csv and gzip files.

ENVIRONMENT VARIABLE SETUP
-----------------------------
The environment variable REDSHIFT_CONNECTION should look like this:

'ENDPOINT=ds-redshift-psbx-dsa.cblrlw3ocr3v.us-west-2.redshift.amazonaws.com;PORT=5439;DB=cust_analytics_prd;USER=YOURUSERNAME;PASS=YOURPASSWORD'

Where YOURUSERNAME is your username, and YOURPASSWORD is your password.  You must have this set in your environment variables to run this code.

TESTING QUERY function
----------------------------
Below are some test for the querying function.  If you would like to run the tests, change run_tests = True, then from this directory type `python redshift_utils`

'''

run_tests = False

def read_sql_file(sql_filename):
    ''' Given a sql file, read it and return a string for execution'''
    with open(sql_filename, "rw+") as sql_file:
        sql_select = ' '.join(sql_file.readlines())
    return sql_select


def get_rs_conn_string():
    ''' Gets Redshift connection String '''
    redshift = os.environ['REDSHIFT_CONNECTION']
    creds = redshift.split(';')
    endpoint = creds[0].split('=')[1]
    port = creds[1].split('=')[1]
    usr = creds[3].split('=')[1]
    pswd = creds[4].split('=')[1]
    conn_string = "dbname='cust_analytics_prd' port='{0}' user='{1}' password='{2}' host='{3}'".format(port, usr, pswd, endpoint)
    return conn_string


def write_data_to_csv(csvfilename, header, data_rows, delimiter):
    ''' Writes data to a csv.  'header' is a list of ordered header values, and data_rows is a list of lists, containing the column values, in the order of the header.  Selecting a delimiter is required (e.g., delimiter = ',').'''
    with open(csvfilename, 'wb') as f:
        writer = csv.writer(f, delimiter = delimiter)
        writer.writerow(header)
        writer.writerows(data_rows)


def write_data_to_gzip(csvfilename, header, data_rows, delimiter):
    ''' Writes data to a gzip file, recommended for large data sets. 'header' is a list of ordered header values, and data_rows is a list of lists, containing the column values, in the order of the header.  Selecting a delimiter is required (e.g., delimiter = ','). '''

    def make_str_from_list(mylist, delimiter):
        mystring = ''
        for item in mylist:
            mystring = mystring + str(item) + delimiter
        return mystring[:-1] + '\n'

    with gzip.open(csvfilename, 'wb') as g:
        myheader = make_str_from_list(header, delimiter)
        g.write(myheader)
        for row in data_rows:
            myrow = make_str_from_list(row, delimiter)
            g.write(myrow)


def execute_rs_query(sql, return_data=False, return_csv=False, csvfilename='', delimiter='|', compression=False):
    ''' Executes the redshift query. Optionally you can:
    1. Return the data as a list of tuples, as well as header information (return_data = True).
    2. Return a csv (return_csv = True) and csvfilename = 'myfilename.csv' with a delimiter of your choosing (delimiter = 'mydelimiter').  If there is no return, then the code will tell you there is no data to write.
    3. Choose if you want a compressed gzip format csv file (compression = True).

    Example usage:
    1. for a query with no return (e.g., it creates a table) you can use it by typing:

    execute_rs_query(sql)

    2. If you want to return data you will need to run:

    data_rows, header = execute_rs_query(sql, return_data = True)

    3. If you want to return data AND write a comma separated file you can run:

    data_rows, header = (sql, return_data = True, return_csv = True, csvfilename = 'mysuperfile.csv', delimiter = ',')

    4. Finally, if you want to do 3, but want the file gzipped you can run:

    data_rows, header = (sql, return_data = True, return_csv = True, csvfilename = 'mysuperfile.csv', delimiter = ',', compression = True)

    '''
    conn_string = get_rs_conn_string()
    data_rows = []; column_names = []; header = []
    try:
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                if return_data or return_csv:
                    header = [desc[0] for desc in cursor.description]
                    for row in cursor:
                        data_rows.append(row)
                conn.commit()
    except Exception as e:
        logger.error('SQL error: {}'.format(e))
    if return_csv:
        if data_rows == []:
            print('You selected to write to a file, but there is no data to write!')
        else:
            if compression:
                write_data_to_gzip(csvfilename, header, data_rows, delimiter)
            else:
                write_data_to_csv(csvfilename, header, data_rows, delimiter)
    if return_data:
        return data_rows, header
    else:
        return


def drop_table(table_name):
    sql = '''DROP TABLE IF EXISTS {0};'''.format(table_name)
    execute_rs_query(sql)
    return


def csv_to_rs(csv_filename, bucket, s3_path, sql, csv_filepath='', test=False, **kwargs):
    ''' Script to upload csv to Redshift via S3

    1. The bucket is the s3 bucket name (e.g., 'persis-datalab-team')
    2. s3_path is the path leading up to the file (e.g, 'DMP/')
    3. The filename is the name of the file you would like to load to s3 (e.g., 'liveramp_outgoing.csv.gz').
    4. sql is the sql string that you want to execute. An example of how the file should be structured is located in liveramp/run-jobs/create-tables/code/clv_to_redshift.sql.  Please include the curly brackets with the appropriate numbers in your script.
    5. test indicates whether or not the run is a test
    6. **kwargs are variables to match the AWS credentials needed to access S3 (region_name, environment, profile_name)'''
    if test:
        suffix = '_test'
    else:
        suffix = ''
    s3_location = 's3://' + bucket + '/' + s3_path + csv_filename
    logger.info('s3_location is {0}'.format(s3_location))
    logger.info('Loading {0} to s3.'.format(csv_filename))
    s3.upload_file_to_s3(bucket, s3_path, csv_filename, csv_filepath, **kwargs)
    cred_str = s3.get_temp_creds(**kwargs)
    mysql = sql.format(suffix, s3_location, cred_str)
    execute_rs_query(mysql)


def vacuum_table(table_name):
    """ Runs vacuum operation on Redshift table

    Parameters
    ----------
    table_name : str
        schema and table name to be vacuumed ('my_schema.table_name')

    Returns
    -------
    None
    """
    conn_string = get_rs_conn_string()
    try:
        with psycopg2.connect(conn_string) as conn:
            old_isolation_level = conn.isolation_level
            conn.set_isolation_level(0)
            with conn.cursor() as cursor:
                sql = 'vacuum {};'.format(table_name)
                cursor.execute(sql)
                conn.commit()
            conn.set_isolation_level(old_isolation_level)
    except Exception as e:
        logger.error('SQL error: {}'.format(e))


#===================================================
''' Test runs for rs execution below '''

if run_tests == True:

    def test_data_return(data_rows, header):
        '''Tests if data_rows and header contain something.'''
        if len(data_rows) > 0:
            print ('Got data')
        else:
            print ('Failed to get data')
        if len(header) > 0:
            print ('Got header')
        else:
            print ('Failed to get headers')

    def test_file_write(filename):
        ''' Tests to see if filename exists'''
        list_of_dir_files = glob.glob('*')
        num_lines = sum(1 for line in open(filename, 'rb'))
        if filename in list_of_dir_files:
            if filename[-3:] == 'csv':
                    print '{0} was written and contains {1} lines.'.format(filename, num_lines)
            elif filename[-3:] == '.gz':
                i = 0
                with gzip.open(filename, 'r') as f:
                    for lin in f:
                        i = i + 1
                print '{0} was written and contains {1} lines.'.format(filename, i)
            else:
                print 'Sorry, file must either end in ".csv" or ".gz".'
        else:
            print 'Sorry, no file was written.'

    sql = 'select TOP 10 * FROM analytics_user_vws.liveramp_trans;'

    print ('Running first with no return')
    execute_rs_query(sql)
    print ('Success!')

    print ('Now running with returning data')
    data_rows, header = execute_rs_query(sql, return_data=True)
    test_data_return(data_rows, header)

    print ('Now running and returning a csv file')
    filename = 'mysuperfile.csv'
    try:
        os.remove(filename)
    except Exception as e:
        print (e)
    data_rows, header = execute_rs_query(sql, return_data=True, return_csv=True, csvfilename=filename, delimiter=',')
    test_data_return(data_rows, header)
    test_file_write(filename)

    print ('Now running and returning a gzip file')
    filename = 'mysuperfile.gz'
    try:
        os.remove(filename)
    except Exception as e:
        print (e)
    data_rows, header = execute_rs_query(sql, return_data=True, return_csv=True, csvfilename=filename, delimiter=',', compression=True)
    test_data_return(data_rows, header)
    test_file_write(filename)
