import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import boto3
import datetime


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    
    
def create_redshift_cluster(config:configparser.ConfigParser, KEY, SECRET, create_iam=False):
    '''
    checks if redshift cluster exists, if not, stands one up
    INPUTS:
        config: configparser object that already read the dwh.cfg file
        create_iam: if you want to explicitly tell the function to make a new iam role
    RETURNS:
        status: may be one of the following: 'creating', 'available', 'error'
            - if 'creating', wait a few minutes and run it again once the cluster becomes available
    '''
    status = 'error'
    
    # parse inputs; I get ARN later
    [HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT] = config['CLUSTER'].values()
    
    # get redshift client object
    redshift = boto3.client('redshift',
                            region_name='us-west-2',
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)
    # get iam client object
    iam = boto3.client('iam',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,
                       region_name='us-west-2')
    
    # check if cluster is already available
    try:
        cluster_props = redshift.describe_clusters(ClusterIdentifier=HOST)['Clusters'][0]
        cluster_status = cluster_props['ClusterStatus']

        if cluster_status.lower() == 'available':
            # cluster is already available, return to main
            status = 'available'
            return status

        elif cluster_status.lower() == 'creating':
            # cluster has already been created but is not yet available, return to main
            status = 'creating'
            return status
        
        else:
            status = 'error'
            print(f'create_redshift_cluster got unexpected value {cluster_status}')
            return status
        
    except Exception as e:
        # normal behavior, this should mean that the cluster was not found
        print(f'Redshift cluster {HOST} not found, creating it')
        
    # read in ARN role
    try:
        ARN = config['IAM_ROLE']['ARN']
    except Exception as e:
        print('No ARN found in the the configparser. Will have to create IAM role')
        create_iam = True
              
    if not ARN:
        print('ARN string blank. Will have to create IAM role')
        create_iam = True
        
    # create iam role if necessary
    if create_iam:
        DWH_IAM_ROLE_NAME = 'ithinkthereforeIAM'
        # create IAM role and attach read only access policy
        try:
            print('Creating a new IAM Role')
            dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version':'2012-10-17'})
            )
            print('Attaching Policy')
            iam.attach_role_policy(RoleName = DWH_IAM_ROLE_NAME,
                                   PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
                                  )['ResponseMetadata']['HTTPStatusCode']
            ARN = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
            print(f'new ARN is: {ARN}')
        except Exception as e:
            print(e)
    
    # PROCEED TO CREATE REDSHIFT CLUSTER AND PRINT TIMESTAMP
    try:
        response = redshift.create_cluster(        
            # add parameters for hardware
            ClusterType='multi-node',
            NodeType='dc2.large',
            NumberOfNodes=4,

            # add parameters for identifiers & credentials
            DBName=DB_NAME,
            ClusterIdentifier=HOST,
            MasterUsername='master',
            MasterUserPassword='S1deBurn52015!',

            # add parameter for role (to allow s3 access)
             IamRoles=[ARN]
        )
    except Exception as e:
        print(e)
        print('create_redshift_cluster failed to create the redshift cluster')
        print(f'HOST: {HOST}, DB_NAME: {DB_NAME}')
        status = 'error'
        return status
    
    print(f'Started up cluster. Time is: {datetime.datetime.now().strftime("%H:%M:%S")}')
    status = 'creating'
    return status
    

def open_TCP_port(config, KEY, SECRET):
    # open DB_PORT
    # only run if the cluster is available

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

        # get ec2 client object
        ec2 = boto3.resource('ec2',
                    region_name='us-west-2',
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET)
        
        vpc = ec2.Vpc(id=cluster_props['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        
        defaultSg.authorize_ingress(
            GroupName= defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DB_PORT),
            ToPort=int(DB_PORT)
        )
        print('opened TCP port')

    except Exception as e:
        print(e)
        
    return endpoint

def main():
    ####################### INGESTION ##########################
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    secret_config = configparser.ConfigParser()
    secret_config.read('secret.cfg')

    [KEY, SECRET] = secret_config['AWS'].values()


    ####################### IaS ##########################
    # create redshift cluster if not exists
    status = create_redshift_cluster(config, KEY, SECRET)

    if status == 'available':
        # proceed
        print('Redshift cluster identified')
        pass
    elif status == 'creating':
        print('Redshift cluster is being created. Try again shortly.')
        return
    elif status == 'error':
        print('status is error, check logs')
        return

    # open TCP port and get endpoint
    DB_ENDPOINT = open_TCP_port(config, KEY, SECRET)
    if DB_ENDPOINT is None:
        print('Failed to get DB_ENDPOINT')
        return

    ####################### PostgreSQL ##########################
    # connect to RedShift postgresql
    #conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    DB_NAME = config['CLUSTER']['DB_NAME']
    DB_USER = config['CLUSTER']['DB_USER']
    DB_PASSWORD = config['CLUSTER']['DB_PASSWORD']
    DB_PORT = config['CLUSTER']['DB_PORT']
    conn = psycopg2.connect(f"host={DB_ENDPOINT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} port={DB_PORT}")
    cur = conn.cursor()

    print('dropping tables')
    drop_tables(cur, conn)
    print('creating tables')
    create_tables(cur, conn)

    conn.close()
    
    
if __name__ == "__main__":
    main()
    #test()
    
    
