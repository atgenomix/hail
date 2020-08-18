import hail as hl


def load_sample_mt():
    return hl.read_matrix_table("wasbs://mnt-mount@staticbundle.blob.core.windows.net/system/sample/clinvar_gencode_dbNSFP_gnomAD.mt")