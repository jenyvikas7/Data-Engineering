import pandas as pd
import boto3
import json
import psycopg2

from botocore.exceptions import ClientError
import configparser

def create_iam_role(iam, DWH_IAM_ROLENAME):
    '''
    Creates IAM Role for Redshift to allow it to use other AWS services
    '''

    try:
        print("Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLENAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)
        
    print('IAM role created')
    print("Attaching Policy")

    iam.attach_role_policy(RoleName=DWH_IAM_ROLENAME,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess"
                        )['ResponseMetadata']['HTTPStatusCode']
    print('Policy successfully attached')
    print("Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLENAME)['Role']['Arn']
    print(roleArn)
    print('IAM role ARN retrieved')
    return roleArn

def create_cluster(redshift, roleArn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD):
    '''
    Creates a Redshift cluster
    '''

    try:
        response = redshift.create_cluster(        
            
            NumberOfNodes=int(DWH_NUM_NODES),
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)
    print('Redshift Cluster successfully created')
        

def main():
    
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    print('dwh.cfg file successfully read')

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

    (DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

    df = pd.DataFrame({"Param":
                    ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLENAME"],
                "Value":
                    [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLENAME]
                })
    print('Dataframe for Cluster related properties')
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

    roleArn = create_iam_role(iam, DWH_IAM_ROLENAME)
    
    print('Setting the value of IAM Role ARN in dwh.cfg file')
    config.set("IAM_ROLE", "ARN", roleArn)

    with open('dwh.cfg', 'w+') as configfile:
        config.write(configfile)

    print('Successfully updated the value of IAM Role ARN in dwh.cfg file')
    create_cluster(redshift, roleArn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD)


if __name__ == "__main__":
    main()