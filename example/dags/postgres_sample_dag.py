import textwrap
from datetime import datetime, timedelta
import uuid

from elasticsearch import Elasticsearch
from airflow import DAG  # noqa
from airflow import macros  # noqa
from airflow.operators.python_operator import PythonOperator  # noqa
from pyhocon import ConfigFactory
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.extractor.postgres_metadata_extractor import PostgresMetadataExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer


dag_args = {
    'concurrency': 10,
    # One dagrun at a time
    'max_active_runs': 1,
    # 4AM, 4PM PST
    'schedule_interval': '0 11 * * *',
    'catchup': False
}

default_args = {
    'owner': 'amundsen',
    'start_date': datetime(2018, 6, 18),
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'priority_weight': 10,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=120)
}

# NEO4J cluster endpoints
NEO4J_ENDPOINT = 'bolt://neo4j:7687'

neo4j_endpoint = NEO4J_ENDPOINT

neo4j_user = 'neo4j'
neo4j_password = 'test'

es = Elasticsearch([
    {'host': 'elasticsearch'},
])

# TODO: user provides a list of schema for indexing
SUPPORTED_SCHEMAS = ['public']
# String format - ('schema1', schema2', .... 'schemaN')
SUPPORTED_SCHEMA_SQL_IN_CLAUSE = "('{schemas}')".format(schemas="', '".join(SUPPORTED_SCHEMAS))

OPTIONAL_TABLE_NAMES = ''


def connection_string():
    user = 'user'
    password = 'password'
    host = 'host.docker.internal'
    port = '5432'
    db = 'moviesdemo'
    return "postgresql://%s:%s@%s:%s/%s" % (user, password, host, port, db)


def create_table_extract_job(**kwargs):
    where_clause_suffix = textwrap.dedent("""
        where table_schema in {schemas}
    """)

    tmp_folder = '/var/tmp/amundsen/table_metadata'
    node_files_folder = '{tmp_folder}/nodes/'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships/'.format(tmp_folder=tmp_folder)

    job_config = ConfigFactory.from_dict({
        'extractor.postgres_metadata.{}'.format(PostgresMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY):
            where_clause_suffix,
        'extractor.postgres_metadata.{}'.format(PostgresMetadataExtractor.USE_CATALOG_AS_CLUSTER_NAME):
            True,
        'extractor.postgres_metadata.extractor.sqlalchemy.{}'.format(SQLAlchemyExtractor.CONN_STRING):
            connection_string(),
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.NODE_DIR_PATH):
            node_files_folder,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.RELATION_DIR_PATH):
            relationship_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NODE_FILES_DIR):
            node_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.RELATION_FILES_DIR):
            relationship_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_END_POINT_KEY):
            neo4j_endpoint,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_USER):
            neo4j_user,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_PASSWORD):
            neo4j_password,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.JOB_PUBLISH_TAG):
            'unique_tag',  # should use unique tag here like {ds}
    })
    job = DefaultJob(conf=job_config,
                     task=DefaultTask(extractor=PostgresMetadataExtractor(), loader=FsNeo4jCSVLoader()),
                     publisher=Neo4jCsvPublisher())
    job.launch()


def create_es_publisher_sample_job():
    # loader saves data to this location and publisher reads it from here
    extracted_search_data_path = '/var/tmp/amundsen/search_data.json'

    task = DefaultTask(loader=FSElasticsearchJSONLoader(),
                       extractor=Neo4jSearchDataExtractor(),
                       transformer=NoopTransformer())

    # elastic search client instance
    elasticsearch_client = es
    # unique name of new index in Elasticsearch
    elasticsearch_new_index_key = 'tables' + str(uuid.uuid4())
    # related to mapping type from /databuilder/publisher/elasticsearch_publisher.py#L38
    elasticsearch_new_index_key_type = 'table'
    # alias for Elasticsearch used in amundsensearchlibrary/search_service/config.py as an index
    elasticsearch_index_alias = 'table_search_index'

    job_config = ConfigFactory.from_dict({
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.GRAPH_URL_CONFIG_KEY): neo4j_endpoint,
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.MODEL_CLASS_CONFIG_KEY):
            'databuilder.models.table_elasticsearch_document.TableESDocument',
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.NEO4J_AUTH_USER): neo4j_user,
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.NEO4J_AUTH_PW): neo4j_password,
        'loader.filesystem.elasticsearch.{}'.format(FSElasticsearchJSONLoader.FILE_PATH_CONFIG_KEY):
            extracted_search_data_path,
        'loader.filesystem.elasticsearch.{}'.format(FSElasticsearchJSONLoader.FILE_MODE_CONFIG_KEY): 'w',
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.FILE_PATH_CONFIG_KEY):
            extracted_search_data_path,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.FILE_MODE_CONFIG_KEY): 'r',
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_CLIENT_CONFIG_KEY):
            elasticsearch_client,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_NEW_INDEX_CONFIG_KEY):
            elasticsearch_new_index_key,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_DOC_TYPE_CONFIG_KEY):
            elasticsearch_new_index_key_type,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_ALIAS_CONFIG_KEY):
            elasticsearch_index_alias
    })

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=ElasticsearchPublisher())
    job.launch()


class SQLParser:
    join_keywords = {
        "join",
        "full join",
        "cross join",
        "inner join",
        "left join",
        "right join",
        "full outer join",
        "right outer join",
        "left outer join",
    }

    def get_join_data_from_query(self, query):
        # TODO handle cases where there are multiple queries with union (only last query will be picked up)
        # TODO handle cases where comments mess up the queries (when they don't have newlines)
        # TODO handle cases where queries are not selects (not sure if needed ???)
        # TODO handle cases where queries are nested inside each other

        idx = 0

        joined_tables = []
        source_table = ""
        join_type = ""

        from_flag = False
        join_flag = False

        formatted = sqlparse.format(query, strip_comments=True)
        parsed = sqlparse.parse(formatted)[0]

        # iterate through tokens to find out source table name and joined tables
        while True:
            idx, token = parsed.token_next(idx, skip_ws=True, skip_cm=True)
            if token is None:
                break

            if from_flag:
                try:
                    source_table = {"schema": token.get_parent_name(), "table": token.get_real_name()}
                except Exception:
                    print("FAILED. QUERY: {}".format(query))
                from_flag = False

            if join_flag:
                join_table = {"schema": token.get_parent_name(), "table": token.get_real_name(), "join_type": join_type}
                joined_tables.append(join_table)
                join_flag = False

            token_val = token.value.lower()
            if token_val == "from":
                from_flag = True
            elif token_val in self.join_keywords:
                join_type = token_val
                join_flag = True

        return source_table, joined_tables


def populate_join_data(tx, source_table_name, dest_table_name):
    # Check if data already exists
    result = tx.run("""
        MATCH (t1: Table)-[r:JOINED_BY]->(t2: Table)
        WHERE t1.name=$source_table_name and t2.name=$dest_table_name
        RETURN t1
    """, source_table_name=source_table_name, dest_table_name=dest_table_name)

    # If exists just increase graph weight
    if result.peek():
        tx.run("""
        MATCH (t1: Table)-[r1:JOINED_BY]->(t2: Table), (t2: Table)-[r2:JOINS]->(t1: Table)
        WHERE t1.name=$source_table_name and t2.name=$dest_table_name
        SET r1.join_count = r1.join_count + 1
        SET r2.join_count = r2.join_count + 1
        """, source_table_name=source_table_name, dest_table_name=dest_table_name)
    # Else create new relation
    else:
        tx.run("""
            MATCH (t1:Table), (t2: Table)
            WHERE t1.name=$source_table_name and t2.name=$dest_table_name
            CREATE (t1)-[r:JOINED_BY {join_count: $join_count}]->(t2)
            CREATE (t2)-[r1:JOINS {join_count: $join_count}]->(t1)
            """, source_table_name=source_table_name, dest_table_name=dest_table_name, join_count=1)


def get_join_data_from_redshift_and_update():
    QUERY_LOG_STATEMENT = """
    select query, trim(querytxt) as sqlquery
    from stl_query;
    
    
    """

    parser = SQLParser()

    with cursor() as cur:
        cur.execute(QUERY_LOG_STATEMENT)
        result = cur.fetchall()
        for row in result:
            _, query_text = row
            if (query_text.startswith("select") or query_text.startswith("SELECT")) and ("join" in query_text or "JOIN" in query_text):
                source_table, joined_tables = parser.get_join_data_from_query(query_text)
                print("Source Table: {}".format(source_table))
                for join_table in joined_tables:
                    print("Joined Table: {}".format(join_table))
                    if source_table and joined_tables:
                        with driver.session() as session:
                            session.write_transaction(populate_join_data, source_table['table'], join_table['table'])



with DAG('amundsen_databuilder', default_args=default_args, **dag_args) as dag:

    create_table_extract_job = PythonOperator(
        task_id='create_table_extract_job',
        python_callable=create_table_extract_job
    )

    create_es_index_job = PythonOperator(
        task_id='create_es_publisher_sample_job',
        python_callable=create_es_publisher_sample_job
    )
