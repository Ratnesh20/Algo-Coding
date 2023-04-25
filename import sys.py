import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import lit
from pyspark.sql import functions as f
from pyspark.sql.types import *
import logging
from pyspark.sql import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME","HUDI_BUCKET_NAME","HUDI_DATABASE","HUDI_CONNETION_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

hudi_bucket_name = args["HUDI_BUCKET_NAME"]
hudi_database = args["HUDI_DATABASE"]
hudi_connection_name = args["HUDI_CONNETION_NAME"]
glue_job_name = args["JOB_NAME"]

dim_replace_null_cols = [
    'mediatype',
    'equipmentid',
    'channeltype',
    "paymenttype1",
    "paymenttype2",
    "paymenttype3"
]


mapping = [
    ("busnumber","string","busnumber","string"),
    ("agencyid","string","agencyid","string"),
    ("cardeid","string","cardeid","string"),
    ("cardtype","string","cardtype","string"),
    ("manufacturerid","string","manufacturerid","string"),
    ("seqnumber","string","seqnumber","string"),
    ("track2data","string","track2data","string"),
    ("cashboxid","string","cashboxid","string"),
    ("direction","string","direction","string"),
    ("drivernumber","string","drivernumber","string"),
    ("fareboxid","string","fareboxid","string"),
    ("fareboxtimestamp","timestamp","fareboxtimestamp","timestamp"),
    ("fareboxtype","string","fareboxtype","string"),
    ("fareboxversion","string","fareboxversion","string"),
    ("faresetid","string","faresetid","string"),
    ("latitude","string","latitude","string"),
    ("longitude","string","longitude","string"),
    ("locationnumber","string","locationnumber","string"),
    ("mediaattributes","string","mediaattributes","string"),
    ("barcodeid","string","barcodeid","string"),
    ("passengercount","long","passengercount","long"),
    ("scantime","timestamp","scantime","timestamp"),
    ("validity","string","validity","string"),
    ("originaldirection","string","originaldirection","string"),
    ("originalroute","string","originalroute","string"),
    ("probeid","string","probeid","string"),
    ("probedtimestamp","timestamp","probedtimestamp","timestamp"),
    ("rawfilename","string","rawfilename","string"),
    ("cancountasridership","boolean","cancountasridership","boolean"),
    ("designationnumber","string","designationnumber","string"),
    ("earnedpoints","long","earnedpoints","long"),
    ("expirationtimestamp","timestamp","expirationtimestamp","timestamp"),
    ("farededuction","double","farededuction","double"),
    ("firstuseflag","boolean","firstuseflag","boolean"),
    ("groupnumber","string","groupnumber","string"),
    ("impliedvalue","double","impliedvalue","double"),
    ("key","string","key","string"),
    ("keydescription","string","keydescription","string"),
    ("ktfare","string","ktfare","string"),
    ("paygotype","string","paygotype","string"),
    ("pendingrecharge","long","pendingrecharge","long"),
    ("productid","string","productid","string"),
    ("remainingvalue","double","remainingvalue","double"),
    ("restoredflag","boolean","restoredflag","boolean"),
    ("starttimestamp","timestamp","starttimestamp","timestamp"),
    ("ttp","string","ttp","string"),
    ("routenumber","string","routenumber","string"),
    ("runnumber","string","runnumber","string"),
    ("statusflag","string","statusflag","string"),
    ("stopnumber","string","stopnumber","string"),
    ("tenantid","string","tenantid","string"),
    ("tenantname","string","tenantname","string"),
    ("thirdpartybillingcode","string","thirdpartybillingcode","string"),
    ("transactionseq","string","transactionseq","string"),
    ("transactiontimestamp","timestamp","transactiontimestamp","timestamp"),
    ("transactiontypeid","string","transactiontypeid","string"),
    ("transactiontypename","string","transactiontypename","string"),
    ("trip","string","trip","string"),
    ("range","boolean","range","boolean"),
    ("autoloadtype","string","autoloadtype","string"),
    ("autoloadname","string","autoloadname","string"),
    ("packageid","string","packageid","string"),
    ("loadsequencenumber","string","loadsequencenumber","string"),
    ("productvalueafterautoload","string","productvalueafterautoload","string"),
    ("autoloadreturncode","string","autoloadreturncode","string"),
    ("barcodeprinterfirmwareversion","string","barcodeprinterfirmwareversion","string"),
    ("barcodeprintermodulefirmwareversion","string","barcodeprintermodulefirmwareversion","string"),
    ("billvalidatorfirmwarebase","string","billvalidatorfirmwarebase","string"),
    ("billvalidatorfirmwareversion","string","billvalidatorfirmwareversion","string"),
    ("billvalidatorfirmwareversion2","string","billvalidatorfirmwareversion2","string"),
    ("billvalidatorserialnumber","string","billvalidatorserialnumber","string"),
    ("bootloadversion","string","bootloadversion","string"),
    ("coinvalidatorfirmwareversion","string","coinvalidatorfirmwareversion","string"),
    ("fareboxfirmwarebase","string","fareboxfirmwarebase","string"),
    ("fareboxfirmwareversion","string","fareboxfirmwareversion","string"),
    ("fareboxserialnumber","string","fareboxserialnumber","string"),
    ("firmwareupgradefpgaversion","string","firmwareupgradefpgaversion","string"),
    ("firmwareupgrademajorversion","string","firmwareupgrademajorversion","string"),
    ("firmwareupgradestatus","string","firmwareupgradestatus","string"),
    ("lidfirmwarebase","string","lidfirmwarebase","string"),
    ("lidfirmwareversion","string","lidfirmwareversion","string"),
    ("lidserialnumber","string","lidserialnumber","string"),
    ("magneticcardreaderfirmwarebase","string","magneticcardreaderfirmwarebase","string"),
    ("magneticcardreaderfirmwareversion","string","magneticcardreaderfirmwareversion","string"),
    ("muxversion","string","muxversion","string"),
    ("ocufirmwarebase","string","ocufirmwarebase","string"),
    ("ocufirmwareversion","string","ocufirmwareversion","string"),
    ("ocuserialnumber","string","ocuserialnumber","string"),
    ("pedestalboardfirmwarebase","string","pedestalboardfirmwarebase","string"),
    ("pedestalboardfirmwareversion","string","pedestalboardfirmwareversion","string"),
    ("pedestalboardserialnumber","string","pedestalboardserialnumber","string"),
    ("rfipaddress","string","rfipaddress","string"),
    ("rfmacaddress","string","rfmacaddress","string"),
    ("rfversion","string","rfversion","string"),
    ("smartcardfirmwarebase","string","smartcardfirmwarebase","string"),
    ("smartcardreaderfirmwarebase","string","smartcardreaderfirmwarebase","string"),
    ("smartcardreaderfirmwareversion","string","smartcardreaderfirmwareversion","string"),
    ("smartcardreaderserialnumber","string","smartcardreaderserialnumber","string"),
    ("trimfirmwarebase","string","trimfirmwarebase","string"),
    ("trimfirmwareversion","string","trimfirmwareversion","string"),
    ("trimserialnumber","string","trimserialnumber","string"),
    ("trimstockversion","string","trimstockversion","string"),
    ("tscrfirmwareversion","string","tscrfirmwareversion","string"),
    ("wifimodulefirmwareversion","string","wifimodulefirmwareversion","string"),
    ("cmd","string","cmd","string"),
    ("trimsoftwareversion","string","trimsoftwareversion","string"),
    ("trimequipmentnumber","string","trimequipmentnumber","string"),
    ("readcount","long","readcount","long"),
    ("misreadcount","long","misreadcount","long"),
    ("writecount","long","writecount","long"),
    ("badverifycount","long","badverifycount","long"),
    ("printcount","long","printcount","long"),
    ("issuecount","long","issuecount","long"),
    ("jamcount","long","jamcount","long"),
    ("cummulativereadcount","long","cummulativereadcount","long"),
    ("cummulativemisreadcount","long","cummulativemisreadcount","long"),
    ("cummulativewritecount","long","cummulativewritecount","long"),
    ("cummulativebadverifycount","long","cummulativebadverifycount","long"),
    ("cummulativeprintcount","long","cummulativeprintcount","long"),
    ("cummulativeissuecount","long","cummulativeissuecount","long"),
    ("cummulativejamcount","long","cummulativejamcount","long"),
    ("cummulativecyclecount","long","cummulativecyclecount","long"),
    ("coldstart","long","coldstart","long"),
    ("warmstart","long","warmstart","long"),
    ("partnumber","long","partnumber","long"),
    ("powerdown","long","powerdown","long"),
    ("reset","long","reset","long"),
    ("productname","string","productname","string"),
    ("productcategory","string","productcategory","string"),
    ("is_ridership","boolean","is_ridership","boolean"),
    ("organizationname","string","organizationname","string"),
    ("mediatype","string","mediatype","string"),
    ("customertype","string","customertype","string"),
    ("vehicletype","string","vehicletype","string"),
    ("grp","string","grp","string"),
    ("des","string","des","string"),
    ("eff_start_date","timestamp","eff_start_date","timestamp"),
    ("eff_end_date","timestamp","eff_end_date","timestamp"),
    ("inserttimestamp","timestamp","inserttimestamp","timestamp"),
    ("is_current","boolean","is_current","boolean")
]

ttpridership_mapping = [
    ('TenantName','string','TenantName','string'),
    ('legacyttp','int','legacyttp','string'),
    ('LegacyGroup','int','LegacyGroup','string'),
    ('legacydesignator','int','legacydesignator','string'),
    ('productname','string','productname','string'),
    ('productcategory','string','productcategory','string'),
    ('ridership','string','ridership','boolean')
]

dim_replace_null_cols = [
     'customertype',
     'mediatype',
     'trip',
     'tenantid',
     'routenumber',
     'runnumber',
     'organizationname',
     'tenantname',
     'productname',
     'productcategory',
     'stopnumber'
]

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

def sparkSqlQuery1(query, mapping):
    for alias, frame in mapping.items():
        frame.createOrReplaceTempView(alias)
    return spark.sql(query)

def update_hudi_table(dataFrame:DynamicFrame, tablename:str, tabletype:str, recordkey:str, precombinekey:str, database:str=hudi_database, connection:str=hudi_connection_name):
    """Creates Hudi table and stores the DynamicFrame to Hudi Table.

    Args:   
        dataFrame (DynamicFrame): DynamicFrame with data which is to be stored in <tableName>.
        tablename (str): tablename in S3 and Glue Catalog
        tabletype (str): can be staging, fact, dimension.
        recordkey (str): Hudi key for identifying unique records
        precombinekey (str): When two rows have same rowkey values, pick the largest value for the precombine field
    """
        
    if tabletype == 'staging':
        tablepath = hudi_bucket_name + "staging"
    elif tabletype == 'dimension':
        tablepath = hudi_bucket_name + "dimension"
    elif tabletype == 'fact':
        tablepath = hudi_bucket_name + "fact"
    elif tabletype == 'config':
        tablepath = hudi_bucket_name + "config"
    
    logging.info(print(f"Started loading Hudi table {tablepath}/{tablename}"))    
    logging.info(print('Total records inserted ',dataFrame.count()))
    
    glueContext.write_dynamic_frame.from_options(
            frame=dataFrame,
            connection_type="custom.spark",
            connection_options={
            "hoodie.datasource.hive_sync.table": tablename, # Hudi table name cataloged in Glue
            "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.NonPartitionedExtractor",
            "className": "org.apache.hudi",
            "hoodie.datasource.hive_sync.use_jdbc": "false",
            "hoodie.datasource.write.recordkey.field": recordkey, # Hudi key for identifying unique records
            "hoodie.table.name": tablename, # Hudi table name cataloged in Glue
            "hoodie.consistency.check.enabled": "true",
            "path": f"{tablepath}/{tablename}",
            "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
            "hoodie.datasource.write.hive_style_partitioning": "false",
            "hoodie.datasource.write.precombine.field": precombinekey, #When two records have the same key value, we will pick the one with the largest value for the precombine field
            "hoodie.upsert.shuffle.parallelism" : "10",
            "hoodie.datasource.hive_sync.enable": "true",
            "hoodie.datasource.hive_sync.database": hudi_database, # Hudi database name cataloged in Glue
            "hoodie.datasource.write.operation": "upsert",
            "hoodie.datasource.write.row.writer.enable" : "true",
            "hoodie.enable.data.skipping": "true",
            "hoodie.metadata.enable": "false",
            "hoodie.metadata.index.column.stats.enable": "true",
            "hoodie.datasource.write.table.type" : "COPY_ON_WRITE",
            "connectionName": hudi_connection_name,
            },
            transformation_ctx="hudi_config",
        )
        
    logging.info(print(f"Completed loading Hudi table {tablepath}/{tablename}"))   

def read_hudi_table(tablename:str, database:str=hudi_database) -> DataFrame:
    
    logging.info(print(f"Reading table:'{tablename}' from DB:'{hudi_database}'"))
    try:
        df = glueContext.create_data_frame.from_catalog(
                database = database,
                table_name = tablename
            )
        return df
    except Exception as e:
        logging.info(print(f"[ERROR] while Reading TABLE:'{tablename}' from DB:'{hudi_database}'\n{e}"))
        return None

## Enrichment Start
def enrichment(walletorgmedia, ttpridership, dy_dim_product, dy_incremental):
    enrichment_sql = """
        select distinct 
        kps.* 
        ,case when kps.transactiontypeid = 19 and keyDescription = 'PRE' then 'Preset'
              when kps.transactiontypeid not in (15,18,29,112) and enr_typ2scdttp.ttp is null then enr_ttp.productname
              when kps.transactiontypeid not in (15,18,29,112) and enr_typ2scdttp.eff_end_date > cast(kps.transactiontimestamp as date) then enr_ttp.productname
              when kps.transactiontypeid not in (15,18,29,112) and cast(kps.transactiontimestamp as date) between enr_typ2scdttp.eff_start_date and enr_typ2scdttp.eff_end_date then enr_typ2scdttp.productname
              when kps.transactiontypeid in (15,18,29,112) and enr_typ2scd.eff_end_date is null then enr_grp_des.productname
              when kps.transactiontypeid in (15,18,29,112) and enr_typ2scd.eff_end_date > cast(kps.transactiontimestamp as date) then enr_grp_des.productname
              when kps.transactiontypeid in (15,18,29,112) and cast(kps.transactiontimestamp as date) between enr_typ2scd.eff_start_date and enr_typ2scd.eff_end_date then enr_typ2scd.productname
              end as productname
        ,case when kps.groupnumber = '0' and kps.designationnumber not in ('30','31') and keyDescription <> 'PRE' then 'Transfer'
              when kps.groupnumber = '0' and kps.designationnumber not in ('30','31') and keyDescription = 'PRE' then 'Preset'
              when kps.groupnumber = '0' and kps.designationnumber = '30'  then 'Capped Ride'
              when kps.groupnumber = '0' and kps.designationnumber = '31'  then 'Bonus Ride'
              when kps.groupnumber = '1' then 'Period Pass'
              when kps.groupnumber = '2' then 'Stored Ride'
              when kps.groupnumber = '3' then 'Stored Value'
              when kps.groupnumber = '4' then 'Third Party'
              when kps.groupnumber = '6' then 'Key'
              when kps.groupnumber = '7' then 'Employee' end as productcategory
        ,case when kps.transactiontypeid = 19 and keyDescription = 'PRE' Then True
              when kps.transactiontypeid = 19 and keyDescription = 'DMP' Then False
              when kps.transactiontypeid not in (15,18,29,112) and enr_typ2scdttp.ttp is null then enr_ttp.ridership
              when kps.transactiontypeid not in (15,18,29,112) and enr_typ2scdttp.eff_end_date > cast(kps.transactiontimestamp as date) then enr_ttp.ridership
              when kps.transactiontypeid not in (15,18,29,112) and cast(kps.transactiontimestamp as date) between enr_typ2scdttp.eff_start_date and enr_typ2scdttp.eff_end_date then enr_typ2scdttp.is_ridership
              when kps.transactiontypeid in (15,18,29,112) and enr_typ2scd.eff_end_date is null then enr_grp_des.ridership
              when kps.transactiontypeid in (15,18,29,112) and enr_typ2scd.eff_end_date > cast(kps.transactiontimestamp as date) then enr_grp_des.ridership
              when kps.transactiontypeid in (15,18,29,112) and cast(kps.transactiontimestamp as date) between enr_typ2scd.eff_start_date and enr_typ2scd.eff_end_date then enr_typ2scd.is_ridership end as is_ridership
        ,enr_wallet_org_media.organizationname as organizationname
        ,enr_wallet_org_media.mediatype as mediatype
        ,enr_wallet_org_media.customertype as customertype
        ,'bus' as vehicletype
        ,kps.groupnumber as grp
        ,kps.designationnumber as des
        ,current_timestamp() as eff_start_date
        ,cast('2999-12-31' as timestamp) as eff_end_date
        ,current_timestamp() as inserttimestamp
        ,cast(1 as boolean) as is_current
        from kps
        left join enr_ttp on kps.ttp  = enr_ttp.legacyttp
        left join enr_grp_des on enr_grp_des.legacygroup = kps.groupnumber and enr_grp_des.legacydesignator = kps.designationnumber
        left join enr_typ2scd on enr_typ2scd.grp = kps.groupnumber and enr_typ2scd.des = kps.designationnumber and enr_typ2scd.eff_end_date < '2999-12-31'
        left join enr_typ2scdttp on enr_typ2scdttp.ttp = kps.ttp and enr_typ2scdttp.eff_end_date < '2999-12-31'
        left join enr_wallet_org_media on enr_wallet_org_media.identifier = kps.cardeid
        where (kps.transactiontypeid in (14,92,16,93,15,94,18,95,19,43,29,106,112) or (kps.transactiontypeid = 106 and paygotype in (1,5)))
    """

    dy_probe_enriched = sparkSqlQuery(
        glueContext,
        query=enrichment_sql,
        mapping={ 
            "kps": dy_incremental,
            "enr_ttp": ttpridership,
            "enr_grp_des": ttpridership,
            "enr_wallet_org_media": walletorgmedia,
            "enr_typ2scd": dy_dim_product,
            "enr_typ2scdttp": dy_dim_product,
        },
        transformation_ctx="dy_probe_enriched",
    )

    return dy_probe_enriched

def get_secret(secret_name):
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    print(f'Retriving Secret: {secret_name}')
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except Exception as e:
        logging.info(print('Error Occured while getting Secrets from Secrets Manager',e))
        raise e
    
    print('Got Secrets',get_secret_value_response)
    # Decrypts secret using the associated KMS key.
    secrets = json.loads(get_secret_value_response['SecretString'])
    username = secrets['username']
    password = secrets['password']
    host = secrets['host']
    return (username, password, host)

def readRDS(tenantschema, tableName):
    # username, password, host =  get_secret(db_secret)
    host = db_host
    password = db_password
    username = db_username
    print(f"Connectiong to db:'{host}'', with user:'{username}'")
    conn_options = {
        "url": f"jdbc:mysql://{host}:3306/{tenantschema}",
        "dbtable": tableName,
        "user": username,
        "password": password,
        "redshiftTmpDir": ''
    }

    return glueContext.create_dynamic_frame_from_options("redshift", conn_options)

def find_tanents(df_flat_tbl):
    col = 'tenantname'
    return df_flat_tbl.select(col).na.drop().dropDuplicates().rdd.flatMap(lambda x: x).collect()

def find_schema(tanents):
    df = readRDS('reporting', 'tenant_schema').toDF()
    schema = {row['tenant_name'].upper():row['tenant_schema'] for row in df.collect()}
    
    return {
        tanent: schema[tanent.upper()]
        for tanent in tanents
        if tanent.upper() in schema
    }

def encrich_tanents(tanents_schema, dy_incremental, dy_dim_product):
    dys = []
    for tanent, schema in tanents_schema.items():
        print(tanent, schema)
        dy = Filter.apply(frame=dy_incremental, f=lambda x:x['tenantname'] == tanent)
        walletorgmedia = readRDS(schema, 'vwwalletorgmedia')
        ttpridership = validate_dtype(readRDS(schema, 'vwttpridership'), ttpridership_mapping)

        dys.append(enrichment(walletorgmedia, ttpridership, dy_dim_product, dy))
    
    dy_probe_enriched = dys[0]
    for dy in dys[1:]:
        dy_probe_enriched = dy_probe_enriched.union(dy)
    
    return dy_probe_enriched

def validate_dtype(dy, mapping):
    return ApplyMapping.apply(frame=dy,mappings=mapping)

def replace_null(df, cols):
    for col in cols:
        df = df.withColumn(col, f.when(df[col].isNull(), "NULL").otherwise(df[col]))
    return df

def write_last_sync(df):
    last_sync_schema = StructType([
        StructField("jobname",StringType(),True),
        StructField("hoodie_commit_time",StringType(),True),
        StructField("inserttimestamp",TimestampType(),True)]
    )
    
    last_sync_df = spark.createDataFrame(sc.emptyRDD(), last_sync_schema)
    
    hoodie_commit_time = df.agg({"_hoodie_commit_time": "max"}).collect()[0][0]
    jobname = glue_job_name
    rows = [[jobname, hoodie_commit_time]]
    columns = ['jobname','hoodie_commit_time']
    last_sync_latest_df = spark.createDataFrame(rows, columns)
    last_sync_latest_df = last_sync_latest_df.withColumn("inserttimestamp", f.current_timestamp())
    last_sync_latest_dy = DynamicFrame.fromDF(last_sync_latest_df, glueContext, "dyframe")
    update_hudi_table(last_sync_latest_dy, "last_sync", "config", "jobname", "inserttimestamp")
    logging.info(print(f'last_sync_refreshed for {glue_job_name}'))

def read_last_sync(landing_df):
    df_last_sync =  read_hudi_table("last_sync", hudi_database)

    get_max_committime_fact = df_last_sync.filter(f.col('jobname') == glue_job_name).select(f.max(f.col('hoodie_commit_time')).alias('max_hoodiecommit_time')).first().max_hoodiecommit_time
    if get_max_committime_fact is None:
        get_max_committime_fact = '0'
    return landing_df.filter(f.col('_hoodie_commit_time') > get_max_committime_fact)

def dimProduct(df_enriched):
    df_dim_product_hudi_tbl = read_hudi_table("dim_product", hudi_database)
    df_enriched = df_enriched.na.drop(subset=["productname"]) 
    df_enriched = df_enriched.select(
        df_enriched.tenantid
        ,df_enriched.ttp
        ,df_enriched.productname
        ,df_enriched.productcategory
        ,df_enriched.grp
        ,df_enriched.des
        ,df_enriched.eff_start_date
        ,df_enriched.eff_end_date
        ,df_enriched.inserttimestamp
        ,df_enriched.is_current
        ,df_enriched.is_ridership
    ).withColumn("product_dim_key", f.md5(f.concat_ws('-', f.col('tenantid'), f.col('productname'), f.col('des'), f.col('is_ridership'))))
                                                    
    join_cond_ttp = [
        df_dim_product_hudi_tbl.ttp == df_enriched.ttp
        ,df_dim_product_hudi_tbl.tenantid == df_enriched.tenantid
        ,(df_dim_product_hudi_tbl.productname != df_enriched.productname) | (df_dim_product_hudi_tbl.des != df_enriched.des) | (df_dim_product_hudi_tbl.is_ridership != df_enriched.is_ridership)
        ,df_enriched.ttp != 0
        ,df_dim_product_hudi_tbl.is_current == True
        ]
                    
    join_cond_grp_des = [
        df_dim_product_hudi_tbl.grp == df_enriched.grp
        ,df_dim_product_hudi_tbl.des == df_enriched.des
        ,df_enriched.grp != 0
        ,df_enriched.des != 0
        ,df_dim_product_hudi_tbl.tenantid == df_enriched.tenantid
        ,(df_dim_product_hudi_tbl.productname != df_enriched.productname) | (df_dim_product_hudi_tbl.is_ridership != df_enriched.is_ridership)
        ,df_dim_product_hudi_tbl.is_current == True
    ] 

    df_expired_dim_product_ttp = (
        df_dim_product_hudi_tbl
        .join(df_enriched, join_cond_ttp)
        .select(df_dim_product_hudi_tbl.tenantid,
            df_dim_product_hudi_tbl.ttp,
            df_dim_product_hudi_tbl.productname,
            df_dim_product_hudi_tbl.productcategory,
            df_dim_product_hudi_tbl.grp,
            df_dim_product_hudi_tbl.des,
            df_dim_product_hudi_tbl.eff_start_date,
            df_enriched.eff_start_date.alias('eff_end_date'),
            df_dim_product_hudi_tbl.product_dim_key,
            df_dim_product_hudi_tbl.inserttimestamp,
            df_dim_product_hudi_tbl.is_ridership
        )
        .withColumn('is_current', f.lit(False))
    )

    df_expired_dim_product_grp_des = (
        df_dim_product_hudi_tbl
        .join(df_enriched, join_cond_grp_des)
        .select(df_dim_product_hudi_tbl.tenantid,
            df_dim_product_hudi_tbl.ttp,
            df_dim_product_hudi_tbl.productname,
            df_dim_product_hudi_tbl.productcategory,
            df_dim_product_hudi_tbl.grp,
            df_dim_product_hudi_tbl.des,
            df_dim_product_hudi_tbl.eff_start_date,
            df_enriched.eff_start_date.alias('eff_end_date'),
            df_dim_product_hudi_tbl.product_dim_key,
            df_dim_product_hudi_tbl.inserttimestamp,
            df_dim_product_hudi_tbl.is_ridership)
        .withColumn('is_current', f.lit(False))
        )

    df_expired_product = df_expired_dim_product_ttp.unionByName(df_expired_dim_product_grp_des)

    join_toget_new_products = [
        df_dim_product_hudi_tbl.productname == df_enriched.productname
        ,df_dim_product_hudi_tbl.tenantid == df_enriched.tenantid
        ,df_dim_product_hudi_tbl.des == df_enriched.des
        ,df_dim_product_hudi_tbl.is_ridership == df_enriched.is_ridership
    ]

    df_new_products = (df_enriched.join(
        df_dim_product_hudi_tbl,join_toget_new_products, "leftouter")
        .filter(df_dim_product_hudi_tbl.productname.isNull())
        .select(df_enriched.tenantid,
            df_enriched.ttp,
            df_enriched.productname,
            df_enriched.productcategory,
            df_enriched.grp,
            df_enriched.des,
            df_enriched.eff_start_date,
            df_enriched.eff_end_date,
            df_enriched.product_dim_key,
            df_enriched.inserttimestamp,
            df_enriched.is_current,
            df_enriched.is_ridership
        ))

    df_dim_product = df_expired_product.unionByName(df_new_products)
    df_dim_product = df_dim_product.na.drop(subset=["productname"]).dropDuplicates(['tenantid', 'productname'])
    dy_dim_product = DynamicFrame.fromDF(df_dim_product, glueContext, "dy_dim_product")
    update_hudi_table(dy_dim_product, "dim_product", "dimension", "tenantid,productname,des,is_ridership", "inserttimestamp")

def dimTenant(df_enriched):
    df_dim_tenant_hudi_tbl = read_hudi_table("dim_tenant", hudi_database)
    df_enriched = df_enriched.na.drop(subset=["tenantid"]).dropDuplicates(["tenantid"]) 
    df_enriched = df_enriched.select(
        df_enriched.tenantname
        ,df_enriched.tenantid
        ,df_enriched.inserttimestamp
    )

    join_cond_tenant = [f.lower(df_enriched.tenantid) == f.lower(df_dim_tenant_hudi_tbl.tenantid)]

    df_dim_tenant = (
        df_enriched
        .join(df_dim_tenant_hudi_tbl, join_cond_tenant, "leftouter")
        .filter(df_dim_tenant_hudi_tbl.tenantid.isNull())
        .select(
            df_enriched.tenantname
            ,df_enriched.tenantid
            ,df_enriched.inserttimestamp)
        ).withColumn("tenant_dim_key", f.md5(f.concat_ws('-', f.col('tenantid'))))

    df_dim_tenant = df_dim_tenant.na.drop(subset=["tenantid"]).dropDuplicates(["tenantid"])
    dy_dim_tenant = DynamicFrame.fromDF(df_dim_tenant, glueContext, "dy_dim_tenant")
    update_hudi_table(dy_dim_tenant, "dim_tenant", "dimension", "tenantid", "inserttimestamp")

def dimMediatype(df_enriched):
    df_dim_mediatype_hudi_tbl = read_hudi_table("dim_mediatype", hudi_database)
    df_enriched = df_enriched.na.drop(subset=["mediatype"]).dropDuplicates(["mediatype"]) 
    df_enriched = df_enriched.select(
        df_enriched.mediatype
        ,df_enriched.inserttimestamp
    )

    join_cond_mediatype = [f.lower(df_enriched.mediatype) == f.lower(df_dim_mediatype_hudi_tbl.mediatype)]

    df_dim_mediatype = (
        df_enriched
        .join(df_dim_mediatype_hudi_tbl, join_cond_mediatype, "leftouter")
        .filter(df_dim_mediatype_hudi_tbl.mediatype.isNull())
        .select(df_enriched.mediatype
                ,df_enriched.inserttimestamp)
        ).withColumn("mediatype_dim_key", f.md5(f.concat_ws('-', f.col('mediatype'))))

    df_dim_mediatype = df_dim_mediatype.na.drop(subset=["mediatype"]).dropDuplicates(["mediatype"])
    dy_dim_mediatype = DynamicFrame.fromDF(df_dim_mediatype, glueContext, "dy_dim_mediatype")
    update_hudi_table(dy_dim_mediatype, "dim_mediatype", "dimension", "mediatype", "inserttimestamp")

def dimProductcategory(df_enriched):
    df_dim_productcategory_hudi_tbl = read_hudi_table("dim_productcategory", hudi_database)
    df_enriched = df_enriched.na.drop(subset=["productcategory"]).dropDuplicates(["productcategory"]) 
    df_enriched = df_enriched.select(
        df_enriched.productcategory
        ,df_enriched.inserttimestamp
    )

    join_cond_productcategory = [f.lower(df_enriched.productcategory) == f.lower(df_dim_productcategory_hudi_tbl.productcategory)]

    df_dim_productcategory = (
        df_enriched
        .join(df_dim_productcategory_hudi_tbl, join_cond_productcategory, "leftouter")
        .filter(df_dim_productcategory_hudi_tbl.productcategory.isNull())
        .select(df_enriched.productcategory
                ,df_enriched.inserttimestamp)
        ).withColumn("productcategory_dim_key", f.md5(f.concat_ws('-', f.col('productcategory'))))

    df_dim_productcategory = df_dim_productcategory.na.drop(subset=["productcategory"]).dropDuplicates(["productcategory"])
    dy_dim_productcategory = DynamicFrame.fromDF(df_dim_productcategory, glueContext, "dy_dim_productcategory")
    update_hudi_table(dy_dim_productcategory, "dim_productcategory", "dimension", "productcategory", "inserttimestamp")

def dimVehicletype(df_enriched):
    df_dim_vehicletype_hudi_tbl = read_hudi_table("dim_vehicletype", hudi_database)
    df_enriched = df_enriched.na.drop(subset=["vehicletype"]).dropDuplicates(["vehicletype"]) 
    df_enriched = df_enriched.select(df_enriched.vehicletype
                                                        ,df_enriched.inserttimestamp)

    join_cond_vehicletype = [f.lower(df_enriched.vehicletype) == f.lower(df_dim_vehicletype_hudi_tbl.vehicletype)]

    df_dim_vehicletype = (
        df_enriched
        .join(df_dim_vehicletype_hudi_tbl, join_cond_vehicletype, "leftouter")
        .filter(df_dim_vehicletype_hudi_tbl.vehicletype.isNull())
        .select(df_enriched.vehicletype
            ,df_enriched.inserttimestamp)
        ).withColumn("vehicletype_dim_key", f.md5(f.concat_ws('-', f.col('vehicletype'))))

    df_dim_vehicletype = df_dim_vehicletype.na.drop(subset=["vehicletype"]).dropDuplicates(["vehicletype"])
    dy_dim_vehicletype = DynamicFrame.fromDF(df_dim_vehicletype, glueContext, "dy_dim_vehicletype")
    update_hudi_table(dy_dim_vehicletype, "dim_vehicletype", "dimension", "vehicletype", "inserttimestamp")

def dimOrganization(df_enriched):
    df_dim_organization_hudi_tbl = read_hudi_table("dim_organization", hudi_database)
    df_enriched = df_enriched.na.drop(subset=["organizationname", "tenantid"]).dropDuplicates(["organizationname", "tenantid"]) 
    df_enriched = df_enriched.select(
        df_enriched.organizationname
        ,df_enriched.tenantid
        ,df_enriched.inserttimestamp
    )

    join_cond_organization = [
        f.lower(df_enriched.organizationname) == f.lower(df_dim_organization_hudi_tbl.organizationname)
        ,f.lower(df_enriched.tenantid) == f.lower(df_dim_organization_hudi_tbl.tenantid)
    ]

    df_dim_organization = (
        df_enriched
        .join(df_dim_organization_hudi_tbl, join_cond_organization, "leftouter")
        .filter(df_dim_organization_hudi_tbl.organizationname.isNull())
        .select(
            df_enriched.organizationname
            ,df_enriched.tenantid
            ,df_enriched.inserttimestamp)
        ).withColumn("organization_dim_key", f.md5(f.concat_ws('-', f.col('organizationname'), f.col('tenantid'))))

    df_dim_organization = df_dim_organization.na.drop(subset=["organizationname"]).dropDuplicates(["organizationname"])
    dy_dim_organization = DynamicFrame.fromDF(df_dim_organization, glueContext, "dy_dim_organization")
    update_hudi_table(dy_dim_organization, "dim_organization", "dimension", "organizationname, tenantid", "inserttimestamp")

def dimCustomertype(df_enriched):
    df_dim_customertype_hudi_tbl = read_hudi_table("dim_customertype", hudi_database)
    df_enriched = df_enriched.na.drop(subset=["customertype", "tenantid"]).dropDuplicates(["customertype", "tenantid"]) 
    df_enriched = df_enriched.select(
        df_enriched.customertype
        ,df_enriched.tenantid
        ,df_enriched.inserttimestamp
    )

    join_cond_customertype = [
        f.lower(df_enriched.customertype) == f.lower(df_dim_customertype_hudi_tbl.customertype)
        ,f.lower(df_enriched.tenantid) == f.lower(df_dim_customertype_hudi_tbl.tenantid)]

    df_dim_customertype = (
        df_enriched
        .join(df_dim_customertype_hudi_tbl, join_cond_customertype, "leftouter")
        .filter(df_dim_customertype_hudi_tbl.customertype.isNull())
        .select(df_enriched.customertype
            ,df_enriched.tenantid
            ,df_enriched.inserttimestamp)
        ).withColumn("customertype_dim_key", f.md5(f.concat_ws('-', f.col('customertype'), f.col('tenantid'))))

    df_dim_customertype = df_dim_customertype.na.drop(subset=["customertype"]).dropDuplicates(["customertype"])
    dy_dim_customertype = DynamicFrame.fromDF(df_dim_customertype, glueContext, "dy_dim_customertype")
    update_hudi_table(dy_dim_customertype, "dim_customertype", "dimension", "customertype,tenantid", "inserttimestamp")

def dimRunnumber(df_enriched):
    df_dim_runnumber_hudi_tbl = read_hudi_table("dim_runnumber", hudi_database)
    df_enriched = df_enriched.na.drop(subset=["runnumber", "tenantid"]).dropDuplicates(["runnumber", "tenantid"]) 
    df_enriched = df_enriched.select(
        df_enriched.runnumber
        ,df_enriched.tenantid
        ,df_enriched.inserttimestamp
    )

    join_cond_runnumber = [f.lower(df_enriched.runnumber) == f.lower(df_dim_runnumber_hudi_tbl.runnumber)
                            ,f.lower(df_enriched.tenantid) == f.lower(df_dim_runnumber_hudi_tbl.tenantid)]

    df_dim_runnumber = (
        df_enriched
        .join(df_dim_runnumber_hudi_tbl, join_cond_runnumber, "leftouter")
        .filter(df_dim_runnumber_hudi_tbl.runnumber.isNull())
        .select(
            df_enriched.runnumber
            ,df_enriched.tenantid
            ,df_enriched.inserttimestamp)
        ).withColumn("runnumber_dim_key", f.md5(f.concat_ws('-', f.col('runnumber'), f.col('tenantid'))))

    df_dim_runnumber = df_dim_runnumber.na.drop(subset=["runnumber", "tenantid"]).dropDuplicates(["runnumber", "tenantid"])
    dy_dim_runnumber = DynamicFrame.fromDF(df_dim_runnumber, glueContext, "dy_dim_runnumber")
    update_hudi_table(dy_dim_runnumber, "dim_runnumber", "dimension", "runnumber, tenantid", "inserttimestamp")

def dumRoutenumber(df_enriched):
    df_dim_routenumber_hudi_tbl = read_hudi_table("dim_routenumber", hudi_database)
    df_enriched = df_enriched.na.drop(subset=["routenumber", "tenantid"]).dropDuplicates(["routenumber", "tenantid"]) 
    df_enriched = df_enriched.select(
        df_enriched.routenumber
        ,df_enriched.tenantid
        ,df_enriched.inserttimestamp
    )

    join_cond_routenumber = [
        f.lower(df_enriched.routenumber) == f.lower(df_dim_routenumber_hudi_tbl.routenumber)
        ,f.lower(df_enriched.tenantid) == f.lower(df_dim_routenumber_hudi_tbl.tenantid)
    ]

    df_dim_routenumber = (
        df_enriched
        .join(df_dim_routenumber_hudi_tbl, join_cond_routenumber, "leftouter")
        .filter(df_dim_routenumber_hudi_tbl.routenumber.isNull())
        .select(df_enriched.routenumber
            ,df_enriched.tenantid
            ,df_enriched.inserttimestamp)
        ).withColumn("routenumber_dim_key", f.md5(f.concat_ws('-', f.col('routenumber'), f.col('tenantid'))))

    df_dim_routenumber = df_dim_routenumber.na.drop(subset=["routenumber", "tenantid"]).dropDuplicates(["routenumber", "tenantid"])
    dy_dim_routenumber = DynamicFrame.fromDF(df_dim_routenumber, glueContext, "dy_dim_routenumber")
    update_hudi_table(dy_dim_routenumber, "dim_routenumber", "dimension", "routenumber,tenantid", "inserttimestamp")

def dimTripnumber(df_enriched):
    df_dim_tripnumber_hudi_tbl = read_hudi_table("dim_tripnumber", hudi_database)
    df_enriched = df_enriched.na.drop(subset=["trip","tenantid"]).dropDuplicates(["trip", "tenantid"]) 
    df_enriched = df_enriched.select(
        df_enriched.trip
        ,df_enriched.tenantid
        ,df_enriched.inserttimestamp
    )

    join_cond_tripnumber = [
        f.lower(df_enriched.trip) == f.lower(df_dim_tripnumber_hudi_tbl.trip)
        ,f.lower(df_enriched.tenantid) == f.lower(df_dim_tripnumber_hudi_tbl.tenantid)
    ]

    df_dim_tripnumber = (
        df_enriched
        .join(df_dim_tripnumber_hudi_tbl, join_cond_tripnumber, "leftouter")
        .filter(df_dim_tripnumber_hudi_tbl.trip.isNull())
        .select(
            df_enriched.trip
            ,df_enriched.tenantid
            ,df_enriched.inserttimestamp)
        ).withColumn("tripnumber_dim_key", f.md5(f.concat_ws('-', f.col('trip'), f.col('tenantid'))))

    df_dim_tripnumber = df_dim_tripnumber.na.drop(subset=["trip", "tenantid"]).dropDuplicates(["trip", "tenantid"])
    dy_dim_tripnumber = DynamicFrame.fromDF(df_dim_tripnumber, glueContext, "dy_dim_tripnumber")
    update_hudi_table(dy_dim_tripnumber, "dim_tripnumber", "dimension", "trip, tenantid", "inserttimestamp")

def dimStopnumber(df_enriched):
    dy_dim_stops = glueContext.create_dynamic_frame.from_options(
        format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
        connection_type="s3",
        format="csv",
        connection_options={
            "paths": [stop_number_source],
            "recurse": True,
        },
        transformation_ctx="dy_dim_stops",
    )

    dy_dim_stops_flatten = ApplyMapping.apply(frame=dy_dim_stops,
    mappings=[("tenantid", "string", "tenantid", "string"),
            ("stopnumber", "string", "stopnumber", "string"),
            ("stopname", "string", "stopname", "string"),
            ("latitude", "string", "latitude", "string"),
            ("longitude", "string", "longitude", "string")]) 

    df_dim_stop_hudi_tbl = read_hudi_table("dim_stopnumber", hudi_database)
    df_stop_enriched = dy_dim_stops_flatten.toDF()
    df_stopnumber_nulls = df_enriched.select(df_enriched.tenantid).distinct()
    df_stopnumber_nulls = (df_stopnumber_nulls.withColumn('stopnumber', f.lit('NULL'))
                        .withColumn('stopname', f.lit('NULL'))
                        .withColumn('latitude', f.lit('NULL'))
                        .withColumn('longitude', f.lit('NULL'))
                        )
    df_stop_enriched = df_stop_enriched.union(df_stopnumber_nulls)
    df_stop_enriched = df_stop_enriched.na.drop(subset=["stopnumber","tenantid"]).dropDuplicates(["stopnumber", "tenantid"]) 
    df_stop_enriched = df_stop_enriched.withColumn("inserttimestamp", f.current_timestamp())

    join_cond_stopnumber = [
        f.lower(df_stop_enriched.stopnumber) == f.lower(df_dim_stop_hudi_tbl.stopnumber)
        ,f.lower(df_stop_enriched.tenantid) == f.lower(df_dim_stop_hudi_tbl.tenantid)
    ]

    df_dim_stopnumber = (
        df_stop_enriched
        .join(df_dim_stop_hudi_tbl, join_cond_stopnumber, "leftouter")
        .filter(df_dim_stop_hudi_tbl.stopnumber.isNull())
        .select(
            df_stop_enriched.tenantid
            ,df_stop_enriched.stopnumber
            ,df_stop_enriched.stopname
            ,df_stop_enriched.latitude
            ,df_stop_enriched.longitude
            ,df_stop_enriched.inserttimestamp)
        ).withColumn("stopnumber_dim_key", f.md5(f.concat_ws('-', f.col('stopnumber'), f.col('tenantid'))))

    df_dim_stopnumber = df_dim_stopnumber.na.drop(subset=["stopnumber", "tenantid"]).dropDuplicates(["stopnumber", "tenantid"])
    dy_dim_stopnumber = DynamicFrame.fromDF(df_dim_stopnumber, glueContext, "dy_dim_stopnumber")
    update_hudi_table(dy_dim_stopnumber, "dim_stopnumber", "dimension", "stopnumber,tenantid", "inserttimestamp")

def factRidership(df_enriched):
    dy_dim_mediatype = read_hudi_table("dim_mediatype")
    dy_dim_organization = read_hudi_table("dim_organization")
    dy_dim_customertype = read_hudi_table("dim_customertype")
    dy_dim_productcategory = read_hudi_table("dim_productcategory")
    dy_dim_product = read_hudi_table("dim_product")
    dy_dim_tenant = read_hudi_table("dim_tenant")
    dy_dim_routenumber = read_hudi_table("dim_routenumber")
    dy_dim_tripnumber= read_hudi_table("dim_tripnumber")
    dy_dim_runnumber = read_hudi_table("dim_runnumber")
    dy_dim_stopnumber = read_hudi_table("dim_stopnumber")
    dy_dim_vehicletype = read_hudi_table("dim_vehicletype")
    dy_probe_enriched = df_enriched

    fact_ridership_sql="""
        select kps.busnumber
            ,kps.agencyid as cardagencyid
            ,kps.cardeid
            ,kps.cardtype
            ,kps.manufacturerid as cardmanufacturerid
            ,kps.seqnumber as cardsequencenumber
            ,kps.track2data
            ,kps.cashboxid
            ,kps.direction
            ,kps.drivernumber
            ,kps.fareboxid
            ,kps.fareboxtimestamp
            ,kps.fareboxtype
            ,kps.fareboxversion
            ,kps.faresetid
            ,kps.latitude
            ,kps.longitude
            ,kps.locationnumber
            ,kps.mediaattributes
            ,kps.barcodeid
            ,kps.passengercount
            ,kps.scantime
            ,kps.validity
            ,kps.originaldirection
            ,kps.originalroute
            ,kps.probeid                
            ,kps.earnedpoints
            ,kps.expirationtimestamp
            ,kps.farededuction
            ,kps.firstuseflag
            ,kps.groupnumber
            ,kps.designationnumber
            ,kps.ttp
            ,kps.impliedvalue
            ,kps.key
            ,kps.keydescription
            ,kps.ktfare
            ,kps.paygotype
            ,kps.pendingrecharge
            ,kps.productid
            ,kps.remainingvalue
            ,kps.restoredflag
            ,kps.starttimestamp                
            ,kps.routenumber
            ,kps.runnumber
            ,kps.statusflag
            ,kps.stopnumber                                
            ,kps.thirdpartybillingcode
            ,kps.transactionseq
            ,kps.transactiontimestamp
            ,kps.transactiontypeid
            ,kps.transactiontypename
            ,kps.trip
            ,kps.tenantid
            ,kps.inserttimestamp
            ,'bus' as vehicletype
            ,'farebox' as sourcetype
            ,pc.productcategory_dim_key
            ,o.organization_dim_key
            ,m.mediatype_dim_key
            ,c.customertype_dim_key
            ,p.product_dim_key
            ,ron.routenumber_dim_key
            ,tn.tripnumber_dim_key
            ,run.runnumber_dim_key
            ,coalesce(sn.stopnumber_dim_key, md5(concat('NULL', '-', kps.tenantid))) as stopnumber_dim_key
            ,vt.vehicletype_dim_key
        from kps
        left join pc on kps.productcategory = pc.productcategory
        left join t on t.tenantid = kps.tenantid
        left join o on kps.organizationname = o.organizationname and kps.tenantid = o.tenantid
        left join m on kps.mediatype = m.mediatype
        left join c on kps.customertype = c.customertype and kps.tenantid = c.tenantid
        left join p on kps.productname = p.productname and kps.tenantid = p.tenantid and p.is_current = True
        left join ron on kps.routenumber = ron.routenumber and kps.tenantid = ron.tenantid
        left join tn on kps.trip = tn.trip and kps.tenantid = tn.tenantid
        left join run on kps.runnumber = run.runnumber and kps.tenantid = run.tenantid
        left join sn on kps.stopnumber = sn.stopnumber and kps.tenantid = sn.tenantid
        left join vt on kps.vehicletype = vt.vehicletype
        where p.is_ridership = True 
    """

    df_fact_ridership = sparkSqlQuery1(
        query=fact_ridership_sql,
        mapping={ 
            "kps": dy_probe_enriched,
            "m": dy_dim_mediatype,
            "o": dy_dim_organization,
            "c": dy_dim_customertype,
            "pc": dy_dim_productcategory,
            "p": dy_dim_product,
            "t": dy_dim_tenant,
            "ron": dy_dim_routenumber,
            "tn": dy_dim_tripnumber,
            "run": dy_dim_runnumber,
            "sn": dy_dim_stopnumber,
            "vt": dy_dim_vehicletype
        }
    )

    dy_fact_ridership = DynamicFrame.fromDF(df_fact_ridership, glueContext, 'dy_fact_ridership')

    update_hudi_table(dy_fact_ridership, "fact_ridership", "fact", "tenantid,locationnumber,transactionseq,transactiontimestamp,busnumber,fareboxid,transactiontypeid", "inserttimestamp")
  
def process_facts_dim(df_probe_enriched):
    dimProduct(df_probe_enriched)
    dimTenant(df_probe_enriched)
    dimMediatype(df_probe_enriched)
    dimProductcategory(df_probe_enriched)
    dimVehicletype(df_probe_enriched)
    dimOrganization(df_probe_enriched)
    dimCustomertype(df_probe_enriched)
    dimRunnumber(df_probe_enriched)
    dumRoutenumber(df_probe_enriched)
    dimTripnumber(df_probe_enriched)
    dimStopnumber(df_probe_enriched)
    factRidership(df_probe_enriched)
    # write_last_sync(df_probe_enriched)

def process_facts_dim(df_probe_enriched):
    custmoer_type(df_probe_enriched)
    product(df_probe_enriched)
    mediatype(df_probe_enriched)
    product_cat(df_probe_enriched)
    organization(df_probe_enriched)
    run_num(df_probe_enriched)
    route_num(df_probe_enriched)
    trip_num(df_probe_enriched)
    tanant(df_probe_enriched)
    fact_ridership(df_probe_enriched)
    # write_last_sync(df_probe_enriched)

def process_batch(dy_incremental, tanents_schema, dy_dim_product):
    # tanents_schema = {x:x.lower() for x in tanents}
    dy_probe_enriched = encrich_tanents(tanents_schema, dy_incremental, dy_dim_product)
    dy_probe_enriched = validate_dtype(dy_probe_enriched, mapping)
    # END enrichment

    # Filter-out null products
    df_probe_enriched = dy_probe_enriched.toDF()
    df_probe_enriched = df_probe_enriched.filter(df_probe_enriched.productname.isNotNull())

    # Treat Null Values
    df_probe_enriched = replace_null(df_probe_enriched, dim_replace_null_cols)
    process_facts_dim(df_probe_enriched)


df_stg_probe_landing = read_hudi_table("stg_probe_landing", 'dev-hudi-db')
# df_incremental = read_last_sync(df_stg_probe_landing)
dy_incremental = DynamicFrame.fromDF(df_stg_probe_landing, glueContext, "dy_incremental")
# dy_incremental = DynamicFrame.fromDF(df_incremental, glueContext, "dy_incremental")
print('Total New records to be Processed', dy_incremental.count())
df_dim_product = read_hudi_table("dim_product", hudi_database)
dy_dim_product = DynamicFrame.fromDF(df_dim_product, glueContext, "dy_dim_product")

tanents = find_tanents(dy_incremental.toDF())
print("Tenants in this Batch are", tanents)
tanents_schema = find_schema(tanents)
print('RDS Schema ', tanents_schema)
tanents_schema = {'CDTA': 'cdta'}

data_sorted = dy_incremental.toDF().sort("transactiontimestamp")
window = Window.orderBy("transactiontimestamp")
data_with_index = data_sorted.withColumn("index", f.row_number().over(window))
total_records = data_with_index.count()

total_records = total_records//10
batch_size = total_records//4


for start_index in range(1, total_records, batch_size):
    end_index = start_index + batch_size - 1
    print(f'Processing from {start_index} to {end_index}, Toatal Size:- {batch_size}')
    
    df_batch = data_with_index.filter((data_with_index['index'] >= start_index) & (data_with_index['index'] <= end_index))
    dy_batch = DynamicFrame.fromDF(df_batch, glueContext, "df_batch")
    process_batch(dy_batch, tanents_schema, dy_dim_product)

job.commit()