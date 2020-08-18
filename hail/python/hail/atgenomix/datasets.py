import os
import pandas as pd
import pymysql.cursors
from typing import Optional, Dict, Tuple, Any, List
from hail.utils.java import Env, FatalError, jindexed_seq_args, warning
from hail.expr.types import hail_type, tarray, tfloat64, tstr, tint32, tstruct, \
    tcall, tbool, tint64, tfloat32
from hail.matrixtable import MatrixTable
from hail.expr.expressions import Expression, StructExpression, \
    expr_struct, expr_any, expr_bool, analyze, Indices, \
    construct_reference, construct_expr, extract_refs_by_indices, \
    ExpressionException, TupleExpression, unify_all
from hail import ir
from hail.typecheck import typecheck, typecheck_method, dictof, anytype, \
    anyfunc, nullable, sequenceof, oneof, numeric, lazy, enumeration


class Query(object):
    VCF_QUERY = "SELECT * FROM core_vcfdataset WHERE owner_id = \"{}\""
    BAM_QUERY = "SELECT * FROM core_bamdataset WHERE owner_id = \"{}\""
    FASTQ_QUERY = "SELECT * FROM core_fastqdataset WHERE owner_id = \"{}\""
    VCF_QUERY_NAME = "SELECT * FROM core_vcfdataset WHERE owner_id = \"{}\" AND name = \"{}\""
    BAM_QUERY_NAME = "SELECT * FROM core_bamdataset WHERE owner_id = \"{}\" AND name = \"{}\""
    FASTQ_QUERY_NAME = "SELECT * FROM core_fastqdataset WHERE owner_id = \"{}\" AND name = \"{}\""


def execute_sql(sql):
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
    with connection.cursor() as cursor:
        cursor.execute(sql)
        output = cursor.fetchall()

    connection.close()
    return output


def append_data(data, list_res, type_input):
    for i in list_res:
        if ".mt" in i["name"]:
            data.append(("mt", i["name"], i["last_accessed"].isoformat()))
        else:
            data.append((type_input, i["name"], i["last_accessed"].isoformat()))

    return data


def list_datasets(spark, sample_name=None, type=None):
    """
    list all datasets in Atgenomix Platforms
    """

    owner_id = os.environ["SEQSLAB_USER"]
    data = []
    if sample_name is None:
        if type is None:
            append_data(data, execute_sql(Query.VCF_QUERY.format(owner_id)), "vcf")
            append_data(data, execute_sql(Query.BAM_QUERY.format(owner_id)), "bam")
            append_data(data, execute_sql(Query.FASTQ_QUERY.format(owner_id)), "fastq")
        else:
            if type == "vcf":
                append_data(data, execute_sql(Query.VCF_QUERY.format(owner_id)), "vcf")
            elif type == "bam":
                append_data(data, execute_sql(Query.BAM_QUERY.format(owner_id)), "bam")
            elif type == "fastq":
                append_data(data, execute_sql(Query.FASTQ_QUERY.format(owner_id)), "fastq")
            else:
                raise NameError("type does not exist")
    else:
        if type is None:
            append_data(data, execute_sql(Query.VCF_QUERY_NAME.format(owner_id, sample_name)), "vcf")
            append_data(data, execute_sql(Query.BAM_QUERY_NAME.format(owner_id, sample_name)), "bam")
            append_data(data, execute_sql(Query.FASTQ_QUERY_NAME.format(owner_id, sample_name)), "fastq")
        else:
            if type == "vcf":
                append_data(data, execute_sql(Query.VCF_QUERY_NAME.format(owner_id, sample_name)), "vcf")
            elif type == "bam":
                append_data(data, execute_sql(Query.BAM_QUERY_NAME.format(owner_id, sample_name)), "bam")
            elif type == "fastq":
                append_data(data, execute_sql(Query.FASTQ_QUERY_NAME.format(owner_id, sample_name)), "fastq")
            else:
                raise NameError("type does not exist")

    pd.set_option("max_colwidth", 1000)
    output_df = spark.createDataFrame(data, ['Type', 'Name', 'Last_Accessed'])
    return output_df.toPandas()


def import_vcf(sample_name,
               force=False,
               force_bgz=False,
               header_file=None,
               min_partitions=None,
               drop_samples=False,
               call_fields=['PGT'],
               reference_genome='default',
               contig_recoding=None,
               array_elements_required=True,
               skip_invalid_loci=False,
               entry_float_type=tfloat64,
               filter=None,
               find_replace=None,
               n_partitions=None,
               block_size=None,
               _partitions=None) -> MatrixTable:
    """
    Customized import_vcf function for Atgenomix Users to load directly vcf files from Atgenomix platform.
    """

    owner_id = os.environ["SEQSLAB_USER"]
    res = execute_sql(Query.VCF_QUERY_NAME.format(owner_id, sample_name))
    path = res[0]['uri']
    path = path[path.index("/"):] + "*.vcf.gz"

    if res[0]['reference'] == 38:
        reference_genome = 'GRCh38'

    reader = ir.MatrixVCFReader(path, call_fields, entry_float_type, header_file,
                                n_partitions, block_size, min_partitions,
                                reference_genome, contig_recoding, array_elements_required,
                                skip_invalid_loci, force_bgz, force, filter, find_replace,
                                _partitions)
    return MatrixTable(ir.MatrixRead(reader, drop_cols=drop_samples))


def read_matrix_table(sample_name, *, _intervals=None, _filter_intervals=False, _drop_cols=False,
                      _drop_rows=False) -> MatrixTable:
    """
    Customized read_matrix_table function for Atgenomix Users to load directly mt files from Atgenomix platform.
    """

    owner_id = os.environ["SEQSLAB_USER"]
    res = execute_sql(Query.VCF_QUERY_NAME.format(owner_id, sample_name))
    path = res[0]['uri']
    path = path[path.index("/"):] + "all.mt"

    for rg_config in Env.backend().load_references_from_dataset(path):
        hl.ReferenceGenome._from_config(rg_config)

    return MatrixTable(ir.MatrixRead(ir.MatrixNativeReader(path, _intervals, _filter_intervals),
                       _drop_cols, _drop_rows))


@typecheck_method(output=str,
                  overwrite=bool,
                  stage_locally=bool,
                  _codec_spec=nullable(str),
                  _partitions=nullable(expr_any))
def mt_write(mt,
             output: str,
             overwrite: bool = False,
             stage_locally: bool = False,
             _codec_spec: Optional[str] = None,
             _partitions=None):

    path = "/seqslab/usr/{}/notebook/{}".format(os.environ["USER_ID"], output.strip("/"))
    mt.write(path, overwrite, stage_locally, _codec_spec, _partitions)


@typecheck_method(output=str,
                  overwrite=bool,
                  stage_locally=bool,
                  _codec_spec=nullable(str))
def ht_write(ht,
             output: str,
             overwrite=False,
             stage_locally: bool = False,
              _codec_spec: Optional[str] = None):

    path = "/seqslab/usr/{}/notebook/{}".format(os.environ["USER_ID"], output.strip("/"))
    ht.write(path, overwrite, stage_locally, _codec_spec)