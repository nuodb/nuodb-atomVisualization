# (C) Copyright NuoDB, Inc. 2012-2017  All Rights Reserved.

import os
import time
import urllib2

import json
import bottle
from bottle import *
import pynuodb

from datetime import datetime

app = application = bottle.default_app()

class Table:
    def __init__(self):
        self.table_name = None
        self.last_time = None
        self.data = None
        self.indexes = None

    def __str__(self):
        return "Name: %s, Data: %s, Indexes: %s" % (self.table_name, self.data, self.indexes)

old_data = None
connection = None

@get('/names')
def listing_handler():
    global old_data, connection
    tick = datetime.now()
    cursor = connection.cursor()

    stmt_select = "SELECT schema, tableName, tableCatalogId from system.localtableatoms where schema <> 'SYSTEM'"
    cursor.execute(stmt_select)

    tables = {}
    for row in cursor.fetchall():
        t = Table()
        t.table_name = ".".join([row[0], row[1]])
        t.data = {}
        t.indexes = {}
        tables[row[2]] = t

    tock = datetime.now()
    diff = tock - tick    # the result is a datetime.timedelta object
    print(diff.total_seconds())

    stmt_select = "SELECT objectId, catalogId, objectType, fetchCount from system.localatoms"
    cursor.execute(stmt_select)

    for row in cursor.fetchall():
        column_type = row[2]
        t = tables.get(row[1], None)
        if t:
            if column_type == "Data" or column_type == "Records":
                t.data[row[0]] = [row[3], row[3], True]
            elif column_type == "Index":
                t.indexes[row[0]] = [row[3], row[3], True]

    cursor.close()

    tock = datetime.now()
    diff = tock - tick    # the result is a datetime.timedelta object
    print(diff.total_seconds())

    response.headers['Content-Type'] = 'application/json'
    response.headers['Access-Control-Allow-Origin'] = '*'

    if old_data:
        for table_key, table in old_data.iteritems():
            new_table = tables.get(table_key, None)
            if not new_table:
                new_table = Table()
                new_table.table_name = table.table_name
                new_table.data = {}
                new_table.indexes = {}
                tables[table_key] = new_table

            for item, data in table.data.iteritems():
                if item in new_table.data:
                    new_table.data[item][1] = new_table.data[item][0] - data[0]
                else:
                    new_table.data[item] = [data[0], 0, False]

            for item, index in table.indexes.iteritems():
                if item in new_table.indexes:
                    new_table.indexes[item][1] = new_table.indexes[item][0] - index[0]
                else:
                    new_table.indexes[item] = [index[0], 0, False]

    tock = datetime.now()
    diff = tock - tick    # the result is a datetime.timedelta object
    print(diff.total_seconds())

    json_map = {}
    for key, element in tables.iteritems():
        d = [{"x": i%100, "y": i/100, "value": element.data[x][1]}for i,x in enumerate(sorted(element.data.keys())) if element.data[x][2]]
        i = [{"x": i%100, "y": i/100, "value": element.indexes[x][1]}for i,x in enumerate(sorted(element.indexes.keys())) if element.indexes[x][2]]
        json_element = {'data': d, 'indexes': i}
        json_map[element.table_name] = json_element

    tock = datetime.now()
    diff = tock - tick    # the result is a datetime.timedelta object
    print(diff.total_seconds())

    old_data = tables

    return json.dumps(json_map)

@get('/average')
def get_average():
    response.headers['Content-Type'] = 'application/json'
    response.headers['Access-Control-Allow-Origin'] = '*'
    try:
        content = urllib2.urlopen("http://0.0.0.0:17170").read()
        return json.dumps(content)
    except:
        return json.dumps(0)

if __name__ == '__main__':
    options = {"schema": "test"}
    connect_kw_args = {'database': "test", 'host': "localhost", 'user': "dba", 'password': "dba", 'options': options}

    connection = pynuodb.connect(**connect_kw_args)

    bottle.run(host = '0.0.0.0', port = 8000)

    connection.close()
