import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import boto3


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def get_endpoint(config, KEY, SECRET):
    # returns DB_ENDPOINT
    endpoint = None

    try:
        # get vars
        [HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT] = config['CLUSTER'].values()
        # get redshift client object
        redshift = boto3.client('redshift',
                                region_name='us-west-2',
                                aws_access_key_id=KEY,
                                aws_secret_access_key=SECRET)
        cluster_props = redshift.describe_clusters(ClusterIdentifier=HOST)['Clusters'][0]
        endpoint = cluster_props['Endpoint']['Address']
    except Exception as e:
        print('unable to get endpoint')
        print(e)
    
    return endpoint


def main():

    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    secret_config = configparser.ConfigParser()
    secret_config.read('secret.cfg')

    [KEY, SECRET] = secret_config['AWS'].values()
    
    # need to also get the cluster endpoint
    DB_ENDPOINT = get_endpoint(config, KEY, SECRET)

    DB_NAME = config['CLUSTER']['DB_NAME']
    DB_USER = config['CLUSTER']['DB_USER']
    DB_PASSWORD = config['CLUSTER']['DB_PASSWORD']
    DB_PORT = config['CLUSTER']['DB_PORT']
    conn = psycopg2.connect(f"host={DB_ENDPOINT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} port={DB_PORT}")
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    #insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()