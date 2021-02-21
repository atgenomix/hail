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
        path = path[path.index("/"):] + ("*.vcf.gz" if path[path.index("/"):].endswith("/") else + "/*.vcf.gz")
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

    # Decide reference_genome
    struct_info = str(dataset_repartition.row)
    if "GRCh38" in struct_info:
        reference_genome = 38
    elif "GRCh37" in struct_info:
        reference_genome = 19
    else:
        reference_genome = 99

    # Create to rdb
    vcf_uid = write_dataset_to_rdb(now, filename.replace(".bgz", ".gz"), full_path, "", reference_genome)

    # Export to dataset
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
        path = eval(row._corrupt_record)['path']
        if not ("_SUCCESS" in path):
            hl.utils.hadoop_copy(path, path.replace(".bgz", ".vcf.gz"))
            hl.utils.hadoop_delete(path)
        return "rename succeed!"

    files_df.rdd.map(rename).collect()
    
    # Update Size to RDB
    update_size(vcf_uid, full_path)


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

        # Create to rdb
        vcf_uid = write_dataset_to_rdb(now, filename, full_path)

        # Write Matrix Table
        mt.write(full_path.rstrip("/"), overwrite, stage_locally, _codec_spec, _partitions)

        # Update Size to RDB
        update_size(vcf_uid, full_path)
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


def write_table(ht: Table,
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

        # Create to rdb
        vcf_uid = write_dataset_to_rdb(now, filename, full_path)

        # Write Hail Table
        ht.write(full_path.rstrip("/"), overwrite, stage_locally, _codec_spec)

        # Update Size to RDB
        update_size(vcf_uid, full_path)
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


def publish_track(name, matrix_table):
    """
    Publish a matrix_table (vcf) to /seqslab/custom_db/reference_genome
    """
    # Decide reference_genome
    struct_info = str(matrix_table.row)
    if "GRCh38" in struct_info:
        reference_genome = str(38)
    elif "GRCh37" in struct_info:
        reference_genome = str(19)
    else:
        reference_genome = str(99)

    # Export matrix_table locally
    filename_bgz = "/tmp/{}.vcf.bgz".format(name)
    filename_gz = "/tmp/{}.vcf.gz".format(name)
    filename_tbi = "/tmp/{}.vcf.gz.tbi".format(name)
    path = "file://{}".format(filename_bgz)
    hl.export_vcf(matrix_table, path)

    # Rename bgz to gz
    cmd_rename_vcf_bgz2gz = [
        "mv",
        filename_bgz,
        filename_gz
    ]
    pid0 = subprocess.Popen(
        cmd_rename_vcf_bgz2gz,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd="/tmp",
    )
    pid0_log = pid0.communicate()

    # Create .tbi for the specific vcf file
    cmd_create_tbi = [
        "tabix",
        "-p",
        "vcf",
        filename_gz
    ]
    pid1 = subprocess.Popen(
        cmd_create_tbi,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd="/usr/local/bin",
    )
    pid1_log = pid1.communicate()

    default_fs = Env.backend().sc._jsc.hadoopConfiguration().get("fs.defaultFS")
    directory_path = "{}/seqslab/custom_db/{}".format(default_fs, reference_genome)

    # mkdir recursively for directory_path
    cmd_hadoop_mkdir = [
        "hadoop",
        "fs",
        "-mkdir",
        "-p",
        directory_path
    ]
    pid2_1 = subprocess.Popen(
        cmd_hadoop_mkdir,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd="/usr/local/hadoop/bin/",
    )
    pid2_1_log = pid2_1.communicate()

    # hadoop put .vcf.gz and .vcf.gz.tbi under /seqslab/custom_db
    cmd_put_file2hadoop = [
        "hadoop",
        "fs",
        "-put",
        filename_gz,
        filename_tbi,
        directory_path
    ]
    pid2_2 = subprocess.Popen(
        cmd_put_file2hadoop,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd="/usr/local/hadoop/bin/",
    )
    pid2_2_log = pid2_2.communicate()

    # Remove files locally
    cmd_remove_files = [
        "rm",
        filename_gz,
        filename_tbi
    ]
    pid3 = subprocess.Popen(
        cmd_remove_files,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd="/tmp",
    )
