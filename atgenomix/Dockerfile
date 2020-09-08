FROM atgenomix.azurecr.io/atgenomix/seqslab_runtime-1.2_ubuntu-16.04_hail-annotation:cache

# Install atgenomix hail
WORKDIR /usr/local/seqslab/hail
RUN  git config --global user.email "johnny5584767@gmail.com" \
     && git config --global user.name "Yuting LIN" \
     && git reset --hard HEAD \
     && git pull origin develop \
     && cd /usr/local/seqslab/hail/hail \
     && make clean \
     && make install HAIL_COMPILE_NATIVES=1 SCALA_VERSION=2.11.12 SPARK_VERSION=2.4.2
