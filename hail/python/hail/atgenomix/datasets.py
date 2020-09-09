import os
import sys
import uuid
import secrets
import subprocess
import hail as hl
import pandas as pd
from .rdb import *
from datetime import datetime
from pyspark.sql import SQLContext
from typing import Optional, Dict, Tuple, Any, List
from hail.utils.java import Env, FatalError, jindexed_seq_args, warning, error
from hail.expr.types import hail_type, tarray, tfloat64, tstr, tint32, tstruct, \
    tcall, tbool, tint64, tfloat32
from hail.matrixtable import MatrixTable
from hail.table import Table
from hail.expr.expressions import Expression, StructExpression, \
    expr_struct, expr_any, expr_bool, analyze, Indices, \
    construct_reference, construct_expr, extract_refs_by_indices, \
    ExpressionException, TupleExpression, unify_all
from hail import ir
from hail.typecheck import typecheck, nullable, oneof, dictof, anytype, \
    sequenceof, enumeration, sized_tupleof, numeric, table_key_type, char
from hail.genetics.reference_genome import reference_genome_type
from hail.methods.misc import require_biallelic, require_row_key_variant, require_row_key_variant_w_struct_locus, require_col_key_str
from hail.utils.java import warning


def name_path_generator():
    now = datetime.now()
    uid = uuid.uuid4()
    full_path = "/seqslab/usr/{}/variant/{}/0/".format(os.environ["SEQSLAB_USER"], uid)
    return now, full_path


def append_data(data, list_res, type_input):
    for i in list_res:
        if ".mt" in i["name"]:
            data.append(("mt", i["name"], i["last_accessed"].isoformat()))
        else:
            data.append((type_input, i["name"], i["last_accessed"].isoformat()))

    return data


def list_datasets(type=None, sample_name=None):
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
    pd.set_option('display.max_columns', None)
    output_df = SQLContext(sc).createDataFrame(data, ['Type', 'Name', 'Last_Accessed'])
    return output_df.toPandas()


@typecheck(sample_name=oneof(str, sequenceof(str)),
           force=bool,
           force_bgz=bool,
           header_file=nullable(str),
           min_partitions=nullable(int),
           drop_samples=bool,
           call_fields=oneof(str, sequenceof(str)),
           reference_genome=nullable(reference_genome_type),
           contig_recoding=nullable(dictof(str, str)),
           array_elements_required=bool,
           skip_invalid_loci=bool,
           entry_float_type=enumeration(tfloat32, tfloat64),
           filter=nullable(str),
           find_replace=nullable(sized_tupleof(str, str)),
           n_partitions=nullable(int),
           block_size=nullable(int),
           # json
           _partitions=nullable(str))
def import_vcf(sample_name,
               force=False,
               force_bgz=True,
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
        Customized import_vcf function for Atgenomix Users to load directly .vcf.gz files from Atgenomix platform.
    """
    owner_id = os.environ["SEQSLAB_USER"]
    res = execute_sql(Query.VCF_QUERY_NAME.format(owner_id, sample_name))
    if len(res) > 1:
        error("Found more than 1 dataset. Please specify clearly your name of dataset.")
        return None
    elif len(res) == 0:
        error("Not Found this file in dataset. Please use exact name in dataset.")
        return None
    else:
        path = res[0]['uri']
        path = path[path.index("/"):] + "*.vcf.gz"
        if res[0]['reference'] == 38:
            reference_genome = 'GRCh38'

        # Update last accessed
        update_last_accessed(res[0]['id'])

        return hl.import_vcf(path, force, force_bgz, header_file, min_partitions, drop_samples, call_fields,
                             reference_genome, contig_recoding, array_elements_required, skip_invalid_loci, entry_float_type,
                             filter, find_replace, n_partitions, block_size, _partitions)


@typecheck(dataset=oneof(MatrixTable, Table),
           filename=str,
           partition_num=int,
           append_to_header=nullable(str),
           parallel=nullable(ir.ExportType.checker),
           metadata=nullable(dictof(str, dictof(str, dictof(str, str)))))
def export_vcf(dataset, filename, partition_num=23, append_to_header=None, parallel='header_per_shard', metadata=None):
    """
        Customized export_vcf function for Atgenomix Users to save directly vcf files to Atgenomix platform.
    """
    # Generate name and path and make sure file extension as .vcf.bgz
    if filename.endswith(".bgz") and not filename.endswith(".vcf.bgz"):
        filename = filename.replace(".bgz", ".vcf.bgz")
    elif filename.endswith(".gz"):
        if filename.endswith(".vcf.gz"):
            filename = filename.replace(".gz", ".bgz")
        else:
            filename = filename.replace(".gz", ".vcf.bgz")
    elif filename.endswith(".vcf"):
        filename = filename + ".bgz"
    else:
        filename = filename + ".vcf.bgz"

    now, full_path = name_path_generator()
    full_path_bgz = full_path.replace("0/", filename)

    # Repartition and save as .bgz
    dataset_repartition = dataset.repartition(partition_num)
    hl.export_vcf(dataset_repartition, full_path_bgz, append_to_header, parallel, metadata)

    # Rename .vcf.bgz to .vcf.gz
    rename_folder = ["hadoop", "fs", "-mv", full_path_bgz, full_path.rstrip("/")]
    pid = subprocess.Popen(
        rename_folder,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd="/usr/local/hadoop/bin",
    )
    pid.communicate()

    # Rename all .bgz files under full_path
    files = hl.utils.hadoop_ls(full_path.rstrip("/"))
    for file in files:
        del file['is_dir']

    backend = Env.backend()
    files_df = backend._spark_session.read.json(backend.sc.parallelize(files))

    def rename(row):
        path = row.path
        if not ("_SUCCESS" in path):
            hl.utils.hadoop_copy(path, path.replace(".bgz", ".vcf.gz"))
            hl.utils.hadoop_delete(path)
        return "rename succeed!"

    files_df.rdd.map(rename).collect()

    # Decide reference_genome
    struct_info = str(dataset_repartition.row)
    if "GRCh38" in struct_info:
        reference_genome = 38
    elif "GRCh37" in struct_info:
        reference_genome = 19
    else:
        reference_genome = 99

    # Update to rdb
    write_dataset_to_rdb(now, filename.replace(".bgz", ".gz"), full_path, "", reference_genome)


def write_matrix_table(mt: MatrixTable,
                       filename: str,
                       overwrite: bool = False,
                       stage_locally: bool = False,
                       _codec_spec: Optional[str] = None,
                       _partitions=None):
    """
        Customized write_matrix_table function for Atgenomix Users to save directly mt files to Atgenomix platform.
    """
    # Check file extension is .mt or not?
    if filename.endswith(".mt"):
        # Process users desired output files
        now, full_path = name_path_generator()
        mt.write(full_path.rstrip("/"), overwrite, stage_locally, _codec_spec, _partitions)

        # Update to rdb
        write_dataset_to_rdb(now, filename, full_path)
    else:
        error("Please use a filename with .mt at the end.")


@typecheck(sample_name=str,
           _intervals=nullable(sequenceof(anytype)),
           _filter_intervals=bool,
           _drop_cols=bool,
           _drop_rows=bool)
def read_matrix_table(sample_name, *, _intervals=None, _filter_intervals=False, _drop_cols=False,
                      _drop_rows=False) -> MatrixTable:
    """
    Customized read_matrix_table function for Atgenomix Users to load directly mt files from Atgenomix platform.
    """
    owner_id = os.environ["SEQSLAB_USER"]
    res = execute_sql(Query.VCF_QUERY_NAME.format(owner_id, sample_name))
    if len(res) > 1:
        error("Found more than 1 dataset. Please specify clearly your name of dataset.")
        return None
    elif len(res) == 0:
        error("Not Found this file in dataset. Please use exact name in dataset.")
        return None
    else:
        path = res[0]['uri'].rstrip("/")
        path = path[path.index("/"):]
        for rg_config in Env.backend().load_references_from_dataset(path):
            hl.ReferenceGenome._from_config(rg_config)

        # Update last accessed
        update_last_accessed(res[0]['id'])

        return MatrixTable(ir.MatrixRead(ir.MatrixNativeReader(path, _intervals, _filter_intervals),
            _drop_cols, _drop_rows))


def write_hail_table(ht: Table,
                     filename: str,
                     overwrite=False,
                     stage_locally: bool = False,
                     _codec_spec: Optional[str] = None):
    """
        Customized write_hail_table function for Atgenomix Users to save directly ht files to Atgenomix platform.
    """
    # Check file extension is .ht or not?
    if filename.endswith(".ht"):
        # Process users desired output files
        now, full_path = name_path_generator()
        ht.write(full_path.rstrip("/"), overwrite, stage_locally, _codec_spec)

        # Update to rdb
        write_dataset_to_rdb(now, filename, full_path)
    else:
        error("Please use a filename with .ht at the end.")


@typecheck(path=str,
           _intervals=nullable(sequenceof(anytype)),
           _filter_intervals=bool)
def read_table(sample_name, *, _intervals=None, _filter_intervals=False) -> Table:
    """
    Customized read_table function for Atgenomix Users to load directly ht files from Atgenomix platform.
    """
    owner_id = os.environ["SEQSLAB_USER"]
    res = execute_sql(Query.VCF_QUERY_NAME.format(owner_id, sample_name))
    if len(res) > 1:
        error("Found more than 1 dataset. Please specify clearly your name of dataset.")
        return None
    elif len(res) == 0:
        error("Not Found this file in dataset. Please use exact name in dataset.")
        return None
    else:
        path = res[0]['uri'].rstrip("/")
        path = path[path.index("/"):]
        for rg_config in Env.backend().load_references_from_dataset(path):
            hl.ReferenceGenome._from_config(rg_config)

        # Update last accessed
        update_last_accessed(res[0]['id'])

        tr = ir.TableNativeReader(path, _intervals, _filter_intervals)
        return Table(ir.TableRead(tr, False))
