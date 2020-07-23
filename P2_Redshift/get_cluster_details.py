import pandas as pd
import boto3
import json
import psycopg2

from botocore.exceptions import ClientError
import configparser

def get_mycluster_props(redshift, DWH_CLUSTER_IDENTIFIER):
    '''
    gets Redshift cluster properties
    '''

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    return myClusterProps, DWH_ENDPOINT

def open_ports(ec2, myClusterProps, DWH_PORT):
    '''
    Update clusters security group to allow access through redshift port
    '''

    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)
    
def main():
    
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")
    DWH_IAM_ROLENAME      = config.get("DWH", "DWH_IAM_ROLENAME")
    #(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

    df = pd.DataFrame({"Param":
                    ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLENAME"],
                "Value":
                    [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLENAME]
                })
    print('Printing Cluster related properties')
    print(df)


    ec2 = boto3.resource('ec2',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                    )

    iam = boto3.client('iam',aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name='us-west-2'
                    )

    redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    myClusterProps,DWH_ENDPOINT = get_mycluster_props(redshift, DWH_CLUSTER_IDENTIFIER)
    
    config.set("CLUSTER", "host", DWH_ENDPOINT)

    with open('dwh.cfg', 'w+') as configfile:
        config.write(configfile)
    print('Successfully updated DWH Endpoint in dwh.cfg file')
    #open_ports(ec2, myClusterProps, DWH_PORT)
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connected')
    conn.close()

if __name__ == "__main__":
    main()