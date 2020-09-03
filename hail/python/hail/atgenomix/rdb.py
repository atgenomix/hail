import os
import secrets
import hail as hl
import pymysql.cursors
from datetime import datetime


class Query(object):
    # SELECT
    VCF_QUERY = "SELECT * FROM core_vcfdataset WHERE owner_id = \"{}\""
    BAM_QUERY = "SELECT * FROM core_bamdataset WHERE owner_id = \"{}\""
    FASTQ_QUERY = "SELECT * FROM core_fastqdataset WHERE owner_id = \"{}\""
    VCF_QUERY_NAME = "SELECT * FROM core_vcfdataset WHERE owner_id = \"{}\" AND name LIKE \"{}.%\""
    BAM_QUERY_NAME = "SELECT * FROM core_bamdataset WHERE owner_id = \"{}\" AND name LIKE \"{}.%\""
    FASTQ_QUERY_NAME = "SELECT * FROM core_fastqdataset WHERE owner_id = \"{}\" AND name LIKE \"{}.%\""

    # INSERT
    IN_VCFDS = "INSERT INTO `core_vcfdataset` (`id`, `name`, `uri`, `sid`, `size`, `last_accessed`, `status`, " \
        "`irb_id`, `reference`, `relation`, `owner_id`) VALUES ('{}', '{}', '{}', '', {}, '{}', 0, '{}', {}, 0, '{}');"
    IN_BASEDSMEM = "INSERT INTO `core_basedatasetmembers` (`id`, created_at) VALUES ('{}', '{}');"
    IN_VCFDSMEM = "INSERT INTO `core_vcfdatasetmembers` (basedatasetmembers_ptr_id, user_id, vcfdataset_id) VALUES ('{}', '{}', '{}');"
    IN_OUTPUTVCFDS = "INSERT INTO `core_outputvcfdataset` (job_id, vcf_id) VALUES ('{}', '{}');"


def get_connection():
    server = "{}.mysql.database.azure.com".format(os.environ["RDB_SERVER"])
    user = "{}@{}".format(os.environ["RDB_USER"], os.environ["RDB_SERVER"])
    passwd = os.environ["RDB_PASSWORD"]
    database = os.environ["RDB_DATABASE"]
    port = os.environ["RDB_PORT"]
    connection = pymysql.connect(
        host=server,
        user=user,
        password=passwd,
        db=database,
        port=int(port),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor
    )
    return connection


def execute_sql(sql):
    connection = get_connection()
    with connection.cursor() as cursor:
        cursor.execute(sql)
        output = cursor.fetchall()

    connection.close()
    return output


def get_size(path):
    size = 0
    paths = hl.utils.hadoop_ls(path)
    for path in paths:
        if path['is_dir'] is True:
            size += get_size(path['path'])
        else:
            size += int(path['size_bytes'])

    return size


def write_dataset_to_rdb(now, name, uri, irb_id="", reference="38", size=None):
    """
        :param name: output name e.g. "example.mt"
        :param uri: path to save targeted files e.g. uri/example.mt
    """
    vcf_uid = secrets.token_hex(16)
    basedatasetmember_uid = secrets.token_hex(16)

    # Generate Record to core_vcfdataset Table
    size = get_size(uri)
    last_accessed = now.strftime("%Y-%m-%d %H:%M:%S.%f")
    owner_id = os.environ["SEQSLAB_USER"]
    input_uri = "{}{}".format(os.environ["FILESYSTEM"], uri)

    connection = get_connection()
    with connection.cursor() as cursor:
        cursor.execute(Query.IN_VCFDS.format(vcf_uid, name, input_uri, size, last_accessed, irb_id, reference, owner_id))
    connection.commit()

    # Generate Record to core_basedatasetmembers
    connection = get_connection()
    with connection.cursor() as cursor:
        cursor.execute(Query.IN_BASEDSMEM.format(basedatasetmember_uid, last_accessed))
    connection.commit()

    # Generate Record to core_vcfdatasetmembers
    with connection.cursor() as cursor:
        cursor.execute(Query.IN_VCFDSMEM.format(basedatasetmember_uid, owner_id, vcf_uid))
    connection.commit()

    # Generate Record to core_outputvcfdataset
    with connection.cursor() as cursor:
        cursor.execute(Query.IN_OUTPUTVCFDS.format(os.environ["JOBID"], vcf_uid))
    connection.commit()
    connection.close()
