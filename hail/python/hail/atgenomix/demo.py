import hail as hl


def load_sample_mt():
    return hl.read_matrix_table("wasbs://mnt-mount@staticbundle.blob.core.windows.net/system/sample/clinvar_gencode_dbNSFP_gnomAD.mt")


def load_sample_vcf():
    return hl.import_vcf("wasbs://mnt-mount@staticbundle.blob.core.windows.net/system/sample/vcf_sample/0/*.vcf.gz", force_bgz=True, reference_genome="GRCh38")