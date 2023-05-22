import sqlite3

# The intention of this class is to manage our CMS database after we scrape the data.
class db_manager:
    def __init__(self, db_path):
        self.db_path = db_path

    # This executes a SQL statement against our database.
    def db_write(self, sql_statement):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(sql_statement)
        conn.commit()
        conn.close()

    # This appends records to our database.
    def db_append(self, table_name, data_frame):
        conn = sqlite3.connect(self.db_path)
        data_frame.to_sql(table_name, conn, schema='main', if_exists='append', index=False)
        conn.commit()
        conn.close()

    # This table will house the CMS data
    def create_table_stg_cms_data(self):
        sql_statement = '''DROP TABLE IF EXISTS STG_CMS_DATA;'''
        self.db_write(sql_statement)
        sql_statement = '''
            CREATE TABLE IF NOT EXISTS STG_CMS_DATA (
            UUID                    TEXT,
            PROVNUM                 TEXT,
            PROVNAME                TEXT,
            CITY                    TEXT,
            STATE                   TEXT,
            COUNTY_NAME             TEXT,
            COUNTY_FIPS             TEXT,
            CY_QTR                  TEXT,
            WORKDATE                DATE,
            MDSCENSUS               INTEGER,
            HRS_RNDON               FLOAT,
            HRS_RNDON_EMP           FLOAT,
            HRS_RNDON_CTR           FLOAT,
            HRS_RNADMIN             FLOAT,
            HRS_RNADMIN_EMP         FLOAT,
            HRS_RNADMIN_CTR         FLOAT,
            HRS_RN                  FLOAT,
            HRS_RN_EMP              FLOAT,
            HRS_RN_CTR              FLOAT,
            HRS_LPNADMIN            FLOAT,
            HRS_LPNADMIN_EMP        FLOAT,
            HRS_LPNADMIN_CTR        FLOAT,
            HRS_LPN                 FLOAT,
            HRS_LPN_EMP             FLOAT,
            HRS_LPN_CTR             FLOAT,
            HRS_CNA                 FLOAT,
            HRS_CNA_EMP             FLOAT,
            HRS_CNA_CTR             FLOAT,
            HRS_NATRN               FLOAT,
            HRS_NATRN_EMP           FLOAT,
            HRS_NATRN_CTR           FLOAT,
            HRS_MEDAIDE             FLOAT,
            HRS_MEDAIDE_EMP         FLOAT,
            HRS_MEDAIDE_CTR         FLOAT   
            );  '''
        self.db_write(sql_statement)