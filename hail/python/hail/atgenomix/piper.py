from hail.utils.java import Env, FatalError, warning


class PiperBackEnd:

    backend = Env.backend()
    jvm = backend.jvm()
    java_package = getattr(jvm, 'java').util
    piper_package = getattr(jvm, 'net').vartotal

    @staticMethod
    def vcf2vcf(self):
        cmdLine = ['--vcf-output-path', '/seqslab/usr/yuting/export/test.vcf.gz', '--partition-bed-path',
                   '/seqslab/system/bed/38/dry_run.bed', '--reference-version', '38', '--reference-system', 'GRCH',
                   '--workflow-type', '1', '--extra-params', '', '--input-suffix', 'gz', '--output-ext', 'vcf.gz',
                   '--conf-dir', '/home/spark-current/conf', '--path-prefix', '', '--job-id', 'std2std', '--filesystem',
                   'hdfs', '-i', "0=/seqslab/usr/yuting/export/new.vcf.gz"]

        gateway = backend._gateway

        string_class = gateway.jvm.java.lang.String
        java_array = gateway.new_array(string_class, len(cmdLine))
        for i in range(len(cmdLine)):
            java_array[i] = cmdLine[i]

        vcf2vcf_piper = piper_package.piper.cli.Vcf2VcfPiper.apply(java_array)
        vcf2vcf_piper.run(backend._jsc)
