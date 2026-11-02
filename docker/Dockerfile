# Cria Imagem PySpark

# Imagem do SO usada como base
FROM python:3.11.10-bookworm as spark-base

# Atualiza o SO e instala pacotes
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      nano \
      unzip \
      rsync \
      openjdk-17-jdk-headless \
      build-essential \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Variáveis de ambiente
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

# Cria as pastas
RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# Download do arquivo de binários do Spark
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz -o spark-3.5.0-bin-hadoop3.tgz \
 && tar xzf spark-3.5.0-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-3.5.0-bin-hadoop3.tgz

# Conector Kafka já na imagem: a demo não depende de internet ao iniciar
RUN M=https://repo1.maven.org/maven2 \
 && curl -fsSL --output-dir /opt/spark/jars -O $M/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar \
 && curl -fsSL --output-dir /opt/spark/jars -O $M/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
 && curl -fsSL --output-dir /opt/spark/jars -O $M/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
 && curl -fsSL --output-dir /opt/spark/jars -O $M/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Prepara o ambiente com PySpark
FROM spark-base as pyspark

# Instala as dependências Python
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Mais variáveis de ambiente
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_HOME="/opt/spark"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

# Copia o arquivo de configuração do Spark para a imagem
COPY conf/spark-defaults.conf "$SPARK_HOME/conf"
COPY conf/log4j2.properties "$SPARK_HOME/conf"

# Permissões
RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

# Variável PYTHONPATH
ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

# Copia o script de inicialização dos serviços para a imagem
COPY docker/entrypoint.sh .

# Ajusta o privilégio
RUN chmod +x entrypoint.sh

# Executa o script quando inicializar um container
ENTRYPOINT ["./entrypoint.sh"]

