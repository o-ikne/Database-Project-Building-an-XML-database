![GitHub Contributors Image](https://contrib.rocks/image?repo=o-ikne/Database-Project-Building-an-XML-database)
![GitHub Contributors Image](https://contrib.rocks/image?repo=boulkhir/refresher-cs-2020)
[![Generic badge](https://img.shields.io/badge/Made_With-Python-<COLOR>.svg)](https://shields.io/)
[![Generic badge](https://img.shields.io/badge/Library-sqlite3_xml-red.svg)](https://shields.io/)
[![Linux](https://svgshare.com/i/Zhy.svg)](https://svgshare.com/i/Zhy.svg)
![visitor badge](https://visitor-badge.glitch.me/badge?page_id=o-ikne.Database-Project-Building-an-XML-database)

# __Database-Project__
This project is realized by Zakaria Boulkhir and me as a database course project in the Master 1 of data science at the university of Lille.

### __Overview__

The goal of this project is to implement an algorithm to import an __XML__ document into an __SQL__ relational database, and translating the largest possible fragment of __XPath__ to equivalent __SQL__ queries using Python.

The main steps of this project are:

- Proposing a relational encoding scheme for a given __XML__ document.

- Implementation of the functionality of importing an __XML__ document `imdb.xml` into a relational database using __SAX API__.
  
- Developing of a scheme of translating largest possible fragment of __XPath__ to equivalent __SQL__ queries evaluated over the encoded instance.
  
- Proposing an automated testing approach to verify that the system is working correctly.
  
- Developing a protocol for experimental comparison of the querying (time) performance of the system and an existing __XML__ query system.

### __Data__
The data for this project is composed of two databases, the first being __employe__ XML database (`xml-sax/rh.xml`), and the second one is __movie__ XML database (`DBS20/imdb-small.xml`).

### __Installation__

To try our implementation in your own machine you need to install the following requirements:

```python
pip install -r requirements.txt
```
