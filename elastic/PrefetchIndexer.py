#!/usr/bin/python
import os
import yaml
import argparse
import uuid
import json
import logging
import hashlib
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk as es_bulk
from elasticsearch import helpers
import pyrpf

def GetArguments():
    usage = '''Index prefetch files to Elastic'''

    arguments = argparse.ArgumentParser(
        description=(usage)
    )
    arguments.add_argument(
        '-s', '--source',
        dest='source',
        action="store",
        required=True,
        type=unicode,
        help=u'The source. File or Directory.'
    )
    arguments.add_argument(
        '--esindex',
        dest='esindex',
        action="store",
        required=True,
        type=unicode,
        help='Elastic Index'
    )
    arguments.add_argument(
        '--esconfig',
        dest='esconfig',
        action="store",
        required=True,
        type=unicode,
        default=None,
        help='Elastic YAML Config File'
    )
    arguments.add_argument(
        '--mapping',
        dest='mapping',
        action="store",
        type=unicode,
        default=None,
        help='Index Mapping'
    )

    return arguments

def GetFilelist(source):
    '''Get a filelist based off of the source of files
    to be parsed.

    :param
        source: The source name (a file or a directory)
    :returns
        filelist: List of files to parse
    '''
    filelist = []
    if os.path.isfile(source):
        filelist.append(source)
    elif os.path.isdir(source):
        for root, dirs, files in os.walk(source):
            for file in files:
                if file.lower().endswith('.pf'):
                    filelist.append(
                        os.path.join(
                            root,
                            file
                        )
                    )
    return filelist

def Main():
    # get arguments to parse
    arguments = GetArguments()
    options = arguments.parse_args()

    COMMIT_COUNT = 25

    es_handler = None
    if options.esconfig:
        es_handler = EsHandler(
            options.esindex,
            options.esconfig
        )

        # Create index
        if options.mapping:
            with open(options.mapping,"rb") as fh:
                mapping = json.load(fh)
                es_handler.CreateIndex(
                    options.esindex,
                    'prefetch',
                    mapping
                )

    filelist = GetFilelist(options.source)
    pfcnt = 0
    for filename in filelist:
        with open(filename,"rb") as fh:
            try:
                json_doc = pyrpf.as_json(
                    filename,
                    fh
                )
            except Exception as error:
                logging.error("Error Parsing {}: {}".format(filename,unicode(error)))
                continue

            if es_handler:
                # Insert json doc here #
                es_handler.InsertBulkRecord(
                    json.loads(json_doc)
                )

            pfcnt += 1

            if pfcnt == COMMIT_COUNT:
                es_handler.CommitBulkRecords()
                pfcnt = 0

    if es_handler:
        es_handler.CommitBulkRecords()

class EsHandler():
    def __init__(self,es_index,config_file):
        self.es_index = es_index

        with open(config_file,"rb") as fh:
            self.config = yaml.load(fh)

        self.esh = Elasticsearch(
            **self.config
        )

        # Store records for bulk insert
        self.records = []

    def InsertBulkRecord(self,record):
        md5 = hashlib.md5()
        md5.update(unicode(record))

        action = {
            "_index": self.es_index,
            "_type": 'prefetch',
            "_id": md5.hexdigest(),
            "_source": record
        }

        self.records.append(action)

    def CommitBulkRecords(self):
        logging.debug('Starting Indexing Bulk Records')
        success_count, failed_items = es_bulk(
            self.esh,
            self.records,
            timeout='60s',
            raise_on_error=False
        )

        if len(failed_items) > 0:
            logging.error('{} index errors.'.format(
                len(failed_items)
            ))
            for failed_item in failed_items:
                logging.error(unicode(failed_item))

        self.records = []

        logging.debug('Finished Indexing Bulk Records')

    def CreateIndex(self, index, doc_type, mapping):
        '''
        Create mapping for a document type
        IN
            self: EsHandler
            index: the name of the index
            doc_type: the document type
            mapping: The dictionary mapping (not a json string)
        '''
        if self.esh.indices.exists(index=index):
            result = self.esh.indices.delete(
                index=index
            )

        result = self.esh.indices.create(
            index=index
        )
        print result
        self.esh.indices.put_mapping(
            doc_type=doc_type,
            index=index,
            body=mapping['mappings']
        )


if __name__ == "__main__":
    Main()
