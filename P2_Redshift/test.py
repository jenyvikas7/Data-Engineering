import configparser
import psycopg2
from sql_queries import test_queries


def start_test(cur, conn):
    """
    Get the number of records in each table
    """
    for query in [test_staging_events,test_staging_songs,test_songplays,test_users,test_songs,test_artists,test_time]:
        print('Running the query: {}'.format(query))
        cur.execute(query)
        results = cur.fetchone()

        for row in results:
            print("   ", row)

def main():
    """
    Testing
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    start_test(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()