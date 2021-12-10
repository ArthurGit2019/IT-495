#!/usr/bin/env python
# coding: utf-8

# In[15]:


import sys
import xml.etree.ElementTree as ET
from tableaudocumentapi import workbook, datasource, connection, field, xfile
from tableaudocumentapi.workbook import Workbook
from tableaudocumentapi.datasource import Datasource, FieldDictionary
from tableaudocumentapi.connection import Connection
from tableaudocumentapi.field import Field
import os
import pandas as pd


# In[ ]:


try:
    sr = sys.argv[1]
except IndexError:
    raise SystemExit(f"Usage: {sys.argv[0]} <string_to_reverse>")
print(sr + " tree created.")


# In[16]:


#sr = 'model_roi.twbx'
#myTree = xfile.xml_open(sr)
#ET.dump(myTree)


# In[17]:


sourceWB = Workbook(sr)


# In[18]:


# help(sourceWB)


# In[19]:


datasources = sourceWB.datasources


# In[20]:


# print("Datasource 2: "datasources[1].name)


# In[21]:


import pprint
from typing import Any, Dict, Iterable, List
def get_data(item: Any) -> Dict[str, Any]:
    result = dict()
    dtype = type(item)
    
    if isinstance(item, str):
        return item
    
    for attr in [p for p in dir(dtype) if isinstance(getattr(dtype, p), property)]:
        value = getattr(item, attr, None)
        if isinstance(value, list):
            result[attr] = [get_data(x) for x in value]
            
        elif isinstance(value, dict):
            result[attr] = [get_data(x) for x in value.values()]
            
        elif value is not None:
            result[attr] = value
            
    return result


# In[22]:


# sourceWB = Workbook("Downloads/model_roi.twbx")
# pprint.pprint(get_data(sourceWB))
result = get_data(sourceWB)


# In[23]:


# for source in result['datasources']:
#     for field in source['fields']:
#         if 'calculation' not in field.keys():
#             pprint.pprint(field['id'])


# In[24]:


newpath = r'model_roi'
if not os.path.exists(newpath):
    os.mkdir(newpath)


# In[25]:


datasource_path = r'model_roi/Data Sources'
if not os.path.exists(datasource_path):
    os.mkdir(datasource_path)


# In[26]:


length = len(datasources)
for i in range(length):
    # print(i)
    ds_path = r'model_roi/Data Sources/datasource_' + str((i+1))
    if not os.path.exists(ds_path):
        os.mkdir(ds_path)
    ds_info = os.path.join(ds_path, 'datasource_info.md')
    conn_info = os.path.join(ds_path, 'connection_info.md')
    
    field_path = os.path.join(ds_path, 'field')
    if not os.path.exists(field_path):
        os.mkdir(field_path)
        
    exist_field = os.path.join(field_path, 'exist_fields')
    if not os.path.exists(exist_field):
        os.mkdir(exist_field)
    for source in result['datasources']:
        for field in source['fields']:
            if 'calculation' not in field.keys():
                fname = field['id']
                j = 0
                for j in range(len(fname)):
                    if fname[j].isalpha():
                        break
                fname = fname[j:-1] + ".md"        
                exist_field_file = os.path.join(exist_field, fname)
                if not os.path.exists(exist_field_file):
                    f = open(exist_field_file, "a")
                    pprint.pprint(field, stream=f)
                    f.close()
        
    calculated_field = os.path.join(field_path, 'calculated_fields')
    if not os.path.exists(calculated_field):
        os.mkdir(calculated_field)
    for source in result['datasources']:
        for field in source['fields']:
            if 'calculation' in field.keys():
                fname = field['id']
                j = 0
                for j in range(len(fname)):
                    if fname[j].isalpha():
                        break
                fname = fname[j:-1] + ".md"
                calculated_field_file = os.path.join(calculated_field, fname)
                if not os.path.exists(calculated_field_file):
                    f = open(calculated_field_file, "a")
                    pprint.pprint(field, stream=f)
                    f.close()
        
    if not os.path.exists(ds_info):
        f = open(ds_info, "a")
        print(f"Filename: {datasources[i]._filename}", file=f)
        # print(datasources[i]._datasourceXML, file=f)
        # print(datasources[i]._datasourceTree, file=f)
        print(f"Name: {datasources[i].name}", file=f)
        print(f"Version: {datasources[i]._version}", file=f)
        print(f"Caption: {datasources[i]._caption}", file=f)
        print(f"Connection: {datasources[i]._connections}", file=f)
        f.close()
        
    if not os.path.exists(conn_info):
        f = open(conn_info, "a")
        for connection in datasources[i].connections:
            print(f"Database: {connection.dbname}", file=f)
            print(f"Server: {connection.server}", file=f)
            print(f"Username: {connection.username}", file=f)
            print(f"Authentication: {connection.authentication}", file=f)
            print(f"Class: {connection._class}", file=f)
            print(f"Port: {connection.port}", file=f)
            print(f"Query Band: {connection.query_band}", file=f)
            print(f"Initial SQL: {connection.initial_sql}", file=f)
        f.close()


# In[10]:


# print(datasources[0]._datasourceXML)


# In[19]:


# for source in datasources:
#     print(len(source.connections))
#     # help(source.connections)
#     for connection in source.connections:
#         print(connection._connectionXML)
#         print(connection.dbname)
#         print(connection.server)
#         print(connection.username)
#         print(connection.authentication)
#         print(connection._class)
#         print(connection.port)
#         print(connection.query_band)
#         print(connection.initial_sql)


# In[9]:


# for source in datasources:
#     print(f"fields: {source.fields}")
#     print("")


# In[10]:


sourceWB.filename


# In[11]:


sourceWB.worksheets


# In[12]:


# for source in datasources:
#     print(source.name)
#     print(source.caption)


# In[13]:


fields = FieldDictionary(sourceWB.datasources[0].fields)


# In[14]:


# source.fields.values()


# In[15]:


# for name, field in source.fields.items():
#     print(name)
#     print(field.id)
#     print(field.datatype)
#     print(field.role)


# In[16]:


# for field in source.fields.values():
#     print(field.datatype)


# In[26]:


# f = open("extraction.txt", "w")
# property_names=[p for p in dir(Field) if isinstance(getattr(Field, p), property)]
# for field in source.fields.values():
#     print({p: getattr(field, p) for p in property_names}, file=f)
    # f.write(str({p: getattr(field, p) for p in property_names}))


# In[28]:


# extract_file = open("extract.txt", "w")
# import pprint
# property_names=[p for p in dir(Field) if isinstance(getattr(Field, p), property)]
# for field in source.fields.values():
#     pprint.pprint({p: getattr(field, p) for p in property_names}, stream=extract_file)


# In[70]:


fields._indexes


# In[71]:


fields._populate_indexes


# In[72]:


# help(fields)


# In[73]:


# for item in source.fields.values():
#     if item.calculation is None:
#         continue
#     else:
#         print(item)            


# In[74]:


# for item in sourceWB._workbookRoot.findall('.//column[@caption]'):
#     if item.find(".//calculation") is None:
#         continue
#     else:
#         if item.find(".//calculation[@formula]") is None:
#             continue
#         else:
#             print(f"Name: {item.attrib['caption']}", f"Calculated Fields: {Field._read_id(item)}", f"\t\t\t\t\t\tFormula: {Field._read_calculation(item)}" , 
#               "\t\t\t\t\t\tDescription: {}".format(Field._read_description(item)))
#             calculation_row = (Field._read_id(item), Field._read_calculation(item), Field._read_description(item))

