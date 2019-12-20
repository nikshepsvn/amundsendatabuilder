import copy
import csv
import ctypes
import logging
import time
from os import listdir
from os.path import isfile, join

import six
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

from pyhocon import ConfigFactory
from pyhocon import ConfigTree  # noqa: F401
from typing import Set, List  # noqa: F401

from databuilder.publisher.base_publisher import Publisher


# Setting field_size_limit to solve the error below
# _csv.Error: field larger than field limit (131072)
# https://stackoverflow.com/a/54517228/5972935
csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))

# Config keys
# A directory that contains CSV files for nodes
NODE_FILES_DIR = 'node_files_directory'
# A directory that contains CSV files for relationships
RELATION_FILES_DIR = 'relation_files_directory'
# A end point for Gremlin e.g: bolt://localhost:9999
GREMLIN_END_POINT_KEY = 'gremlin_endpoint'
# A boolean flag to make it fail if relationship is not created

# list of nodes that are create only, and not updated if match exists
GREMLIN_CREATE_ONLY_NODES = 'gremlin_create_only_nodes'

# This will be used to provide unique tag to the node and relationship
JOB_PUBLISH_TAG = 'job_publish_tag'

# Gremlin property name for published tag
PUBLISHED_TAG_PROPERTY_NAME = 'published_tag'

# Gremlin property name for last updated timestamp
LAST_UPDATED_EPOCH_MS = 'publisher_last_updated_epoch_ms'
# CSV HEADER
# A header with this suffix will be pass to Gremlin statement without quote
UNQUOTED_SUFFIX = ':UNQUOTED'
# A header for Node label
NODE_LABEL_KEY = 'LABEL'
# A header for Node key
NODE_KEY_KEY = 'KEY'
# Required columns for Node
NODE_REQUIRED_KEYS = {NODE_LABEL_KEY, NODE_KEY_KEY}

# Relationship relates two nodes together
# Start node label
RELATION_START_LABEL = 'START_LABEL'
# Start node key
RELATION_START_KEY = 'START_KEY'
# End node label
RELATION_END_LABEL = 'END_LABEL'
# Node node key
RELATION_END_KEY = 'END_KEY'
# Type for relationship (Start Node)->(End Node)
RELATION_TYPE = 'TYPE'
# Type for reverse relationship (End Node)->(Start Node)
RELATION_REVERSE_TYPE = 'REVERSE_TYPE'
# Required columns for Relationship
RELATION_REQUIRED_KEYS = {RELATION_START_LABEL, RELATION_START_KEY,
                          RELATION_END_LABEL, RELATION_END_KEY,
                          RELATION_TYPE, RELATION_REVERSE_TYPE}

LOGGER = logging.getLogger(__name__)
DEFAULT_CONFIG = ConfigFactory.from_dict({})

def node_merge(g, label, key, val, add_properties, update_properties):
    # Query for node
    node = g.V().has(label, key, val)
    # If node exists, update it's properties
    if node.hasNext():
        for prop in update_properties:
            k, v = prop
            node.property(k, v).next()
    # If node does not exist, create it and add properties
    else:
        node = g.addV(label).property(key, val)
        for prop in add_properties:
            k, v = prop
            node.property(k, v).next()

    return node


def relation_merge(g, start_label, start_key, start_val, end_label, end_key, end_val, relation_type, reverse_relation_type, add_properties, update_properties):
    v1 = g.V().has(start_label, start_key, start_val).next()
    v2 = g.V().has(end_label, end_key, end_val).next()

    def get_edge():
        return g.V().has(start_label, start_key, start_val).outE(relation_type).as_('e').inV().has(end_label, end_key, end_val).select('e')

    def get_reverse_edge():
        return g.V().has(end_label, end_key, end_val).outE(reverse_relation_type).as_('e').inV().has(start_label, start_key, start_val).select('e')

    does_rel1_exist = get_edge().hasNext()
    does_rel2_exist = get_reverse_edge().hasNext()

    if does_rel1_exist:
        for prop in update_properties:
            k, v = prop
            get_edge().property(k, v).next()
    if does_rel2_exist:
        for prop in update_properties:
            k, v = prop
            get_reverse_edge().property(k, v).next()

    if not does_rel1_exist and not does_rel2_exist:
        e1 = g.addE(relation_type).from_(v1).to(v2)
        e2 = g.addE(reverse_relation_type).from_(v2).to(v1)
        for prop in add_properties:
            k, v = prop
            e1.property(k, v).next()
            e2.property(k, v).next()
    return v1, v2


class GremlinCsvPublisher(Publisher):
    """
    A Publisher takes two folders for input and publishes to Gremlin.
    One folder will contain CSV file(s) for Node where the other folder will contain CSV file(s) for Relationship.

    Gremlin follows Label Node properties Graph

    #TODO User UNWIND batch operation for better performance
    """

    def __init__(self):
        # type: () -> None
        super(GremlinCsvPublisher, self).__init__()

    def init(self, conf):
        # type: (ConfigTree) -> None
        conf = conf.with_fallback(DEFAULT_CONFIG)

        self._count = 0  # type: int
        self._node_files = self._list_files(conf, NODE_FILES_DIR)
        self._node_files_iter = iter(self._node_files)

        self._relation_files = self._list_files(conf, RELATION_FILES_DIR)
        self._relation_files_iter = iter(self._relation_files)

        # config is list of node label.
        # When set, this list specifies a list of nodes that shouldn't be updated, if exists
        self.create_only_nodes = set(conf.get_list(GREMLIN_CREATE_ONLY_NODES, default=[]))
        self.labels = set()  # type: Set[str]
        self.publish_tag = conf.get_string(JOB_PUBLISH_TAG)  # type: str
        if not self.publish_tag:
            raise Exception('{} should not be empty'.format(JOB_PUBLISH_TAG))

        self.g = traversal().withRemote(DriverRemoteConnection(conf.get_string(GREMLIN_END_POINT_KEY), 'g'))

        LOGGER.info('Publishing Node csv files {}, and Relation CSV files {}'
                    .format(self._node_files, self._relation_files))

    def _list_files(self, conf, path_key):
        # type: (ConfigTree, str) -> List[str]
        """
        List files from directory
        :param conf:
        :param path_key:
        :return: List of file paths
        """
        if path_key not in conf:
            return []

        path = conf.get_string(path_key)
        return [join(path, f) for f in listdir(path) if isfile(join(path, f))]

    def publish_impl(self):  # noqa: C901
        # type: () -> None
        """
        Publishes Nodes first and then Relations
        :return:
        """

        start = time.time()


        LOGGER.info('Publishing Node files: {}'.format(self._node_files))
        try:
            while True:
                try:
                    node_file = next(self._node_files_iter)
                    self._publish_node(node_file)
                except StopIteration:
                    break

            LOGGER.info('Publishing Relationship files: {}'.format(self._relation_files))
            while True:
                try:
                    relation_file = next(self._relation_files_iter)
                    self._publish_relation(relation_file)
                except StopIteration:
                    break

            LOGGER.info('Committed total {} statements'.format(self._count))

            # TODO: Add statsd support
            LOGGER.info('Successfully published. Elapsed: {} seconds'.format(time.time() - start))
        except Exception as e:
            LOGGER.exception('Failed to publish. Rolling back.')
            raise e

    def get_scope(self):
        # type: () -> str
        return 'publisher.gremlin'

    def _publish_node(self, node_file):
        # type: (str, Transaction) -> Transaction
        """
        Iterate over the csv records of a file, each csv record transform to Merge statement and will be executed.
        All nodes should have a unique key, and this method will try to create unique index on the LABEL when it sees
        first time within a job scope.
        Example of Cypher query executed by this method:
        MERGE (col_test_id1:Column {key: 'presto://gold.test_schema1/test_table1/test_id1'})
        ON CREATE SET col_test_id1.name = 'test_id1',
                      col_test_id1.order_pos = 2,
                      col_test_id1.type = 'bigint'
        ON MATCH SET col_test_id1.name = 'test_id1',
                     col_test_id1.order_pos = 2,
                     col_test_id1.type = 'bigint'

        :param node_file:
        :return:
        """

        with open(node_file, 'r') as node_csv:
            for count, node_record in enumerate(csv.DictReader(node_csv)):
                self.create_node_merge_statement(node_record=node_record)

    def is_create_only_node(self, node_record):
        # type: (dict) -> bool
        """
        Check if node can be updated
        :param node_record:
        :return:
        """
        if self.create_only_nodes:
            return node_record[NODE_LABEL_KEY] in self.create_only_nodes
        else:
            return False

    def create_node_merge_statement(self, node_record):
        # type: (dict) -> str
        """
        Creates node merge statement
        :param node_record:
        :return:
        """
        create_prop_body = self._create_props_body(node_record, NODE_REQUIRED_KEYS)

        if not self.is_create_only_node(node_record):
            update_prop_body = self._create_props_body(node_record, NODE_REQUIRED_KEYS)

        try:
            node_merge(self.g, node_record["LABEL"], "key_test", node_record["KEY"], create_prop_body, update_prop_body)
        except Exception as e:
            print("error thrown when creating node, but still working")

    def _publish_relation(self, relation_file):
        # type: (str, Transaction) -> Transaction
        """
        Creates relation between two nodes.
        (In Amundsen, all relation is bi-directional)

        Example of Cypher query executed by this method:
        MATCH (n1:Table {key: 'presto://gold.test_schema1/test_table1'}),
              (n2:Column {key: 'presto://gold.test_schema1/test_table1/test_col1'})
        MERGE (n1)-[r1:COLUMN]->(n2)-[r2:BELONG_TO_TABLE]->(n1)
        RETURN n1.key, n2.key

        :param relation_file:
        :return:
        """
        with open(relation_file, 'r') as relation_csv:
            for count, rel_record in enumerate(csv.DictReader(relation_csv)):
                self.create_relationship_merge_statement(rel_record=rel_record)

    def create_relationship_merge_statement(self, rel_record):
        # type: (dict) -> str
        """
        Creates relationship merge statement
        :param rel_record:
        :return:
        """
        create_prop_body = self._create_props_body(rel_record, RELATION_REQUIRED_KEYS)
        start_label = rel_record[RELATION_START_LABEL]
        end_label = rel_record[RELATION_END_LABEL]
        start_key = rel_record[RELATION_START_KEY]
        end_key = rel_record[RELATION_END_KEY]
        relation = rel_record[RELATION_TYPE]
        reverse_relation = rel_record[RELATION_REVERSE_TYPE]

        if create_prop_body:
            # We need one more body for reverse relation
            create_prop_body = create_prop_body
            update_prop_body = self._create_props_body(rel_record, RELATION_REQUIRED_KEYS)

        try:
            relation_merge(self.g, start_label, "key_test", start_key, end_label, "key_test", end_key, relation, reverse_relation,
                           create_prop_body, update_prop_body)
        except Exception as e:
            print("error thrown when creating relation, but still working")

    def _create_props_body(self,
                           record_dict,
                           excludes):
        # type: (dict, Set, str) -> str
        """
        Creates properties body with params required for resolving template.

        e.g: Note that node.key3 is not quoted if header has UNQUOTED_SUFFIX.
        identifier.key1 = 'val1' , identifier.key2 = 'val2', identifier.key3 = val3

        :param record_dict: A dict represents CSV row
        :param excludes: set of excluded columns that does not need to be in properties (e.g: KEY, LABEL ...)
        :param identifier: identifier that will be used in CYPHER query as shown on above example
        :param is_update: Creates property body for update statement in MERGE
        :return: Properties body for Cypher statement
        """
        template_params = {}
        props = []
        for k, v in six.iteritems(record_dict):
            if k in excludes:
                template_params[k] = v
                continue

            # escape quote for Cypher query
            # if isinstance(str, v):
            v = v.replace('\'', "\\'")

            if k.endswith(UNQUOTED_SUFFIX):
                k = k[:-len(UNQUOTED_SUFFIX)]
                props.append((k, v))
            else:
                props.append((k, v))

            template_params[k] = v

        props.append((PUBLISHED_TAG_PROPERTY_NAME, self.publish_tag))
        props.append((LAST_UPDATED_EPOCH_MS, 'timestamp()'))

        return props
