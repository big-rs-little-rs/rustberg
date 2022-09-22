# rustberg
Iceberg + datafusion + rust

Overall goal is to support iceberg through datafusion in rust, and eventually a python wrapper:

Currently this project is focussed on supporting only read queries with table metadata backed by Hive Metastore. Initial assumption is that the table files are stored in NFS
