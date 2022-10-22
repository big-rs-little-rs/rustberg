<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# rustberg
Iceberg + datafusion + rust

Overall goal is to support iceberg through datafusion in rust, and eventually a python wrapper:

Currently, this project is focussed on supporting only read queries with table metadata backed by Hive Metastore. Initial assumption is that the table files are stored in NFS

## License

This project is licensed under either of

- [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) ([`LICENSE-APACHE`](LICENSE-APACHE))
- [MIT license](https://opensource.org/licenses/MIT) ([`LICENSE-MIT`](LICENSE-MIT))

at your option.