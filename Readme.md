# Readme

This is based on christian werners SQLiteODBC driver and Ivo Herweijers Presto C Client

## License

GNU General Public License

## Dependencies

- cmake build tooling
- curl for http calls
- unixodbc for sql.h header definitions and isql test utility
- check c testing framework

## Experimentation

This is an exporiment how fast one can lean C with something productive:
- Try to implement non leaking modern C style components
- Implement most essiential bits of an ODBC Server
- Implement a REST interface client in C
- Implement various Parsers in C

## Todos

- [ ] Datatypes
    - [X] Parse all available Data Type Information from Presto
        - Enable complex types ROW, ARRAY, JSON
    - [ ] Mapping to translate to proper ODBC Data Types

- [ ] Properly add null in records
- [X] Check if there is a weird bug in the tablebuff data, not it was in very long an complex sqlite char casting code
- [ ] Prepare should capture input and output columns early to enable bind to those for clients before pulling data
    - [X] Output columns 
    - [ ] Input columns

- [ ] Functionality
    - [ ] Pull in data in chunks
    - [ ] Driver vs PrestoClient Implementation 

- [ ] Use header information properly
    - [X] set catalog
    - [X] set schema
    - [ ] transaction handling

- [ ] No resource leaks in API client usage
    - [X] Valgrind has no error in the test queries

- [ ] No resource leaks in ODBC calls
    - [X] Statement
    - [ ] Client
    - [ ] Environment

- [] Tests
    - [ ] "Show queries" are core dumping the whole ODBC driver thing

- [ ] Windows Build driver dll with embedded libc and friends

- [ ] Implement database metadata
    - Catalogs, Schemas, Tables, Columns, Views you name it