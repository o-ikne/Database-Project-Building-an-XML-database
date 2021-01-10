
import re
from enconding_scheme.encoding_xml_imdb import TravahoHandler


conn = sqlite3.connect('emp_imdb.db')
conn.row_factory = sqlite3.Row
c = conn.cursor()

parser = xml.sax.make_parser()
handler = TravahoHandler("imdb.dtd", "imdb-small.xml")
parser.setContentHandler(handler)

## the tags of xml and their attributes
tables = dict()

## the scheme of xml
scheme = handler.tag_tables
for table in handler.tables:
    tables[table.replace('_', '-')] = list(map(lambda x:x.split()[0].replace('_', '-'), handler.tables[table]))
del tables['imdb']


def look_up(scheme, tag, parents=[]):
    """
    INPUT : scheme : xml scheme
            tag    : tag from xml document
    RETURN : the parents of the tag
    """
    for key in scheme.keys():
        if tag in scheme[key]:
            parents.append(key)
            look_up(scheme, key, parents)
    return parents
  
  
def xpath_to_sql(query):
    """
    converts an XPath query to an SQL equivalent
    """
    
    ## the function in the query
    func = query.split('//')[0]
    
    ## if the query hasa function
    if func:
        ## query without function
        query = query[len(func):-1]
        
        ## transalte the new query to SQL
        sql_query = xpath_to_sql(query).split()
        
        ## add the function to the translated query
        sql_query.insert(1, func)
        sql_query.insert(3, ')')
        return ' '.join(sql_query)
    
    ## otherwise
    else:
        ## eliminate //
        query = query[2:]
        
        ## initiate WHERE AND JOINT Expressions
        where_cndts = []
        join_cndts = ''

        ## if / in query
        if '/' in query:
            ## get the attribute
            attribute = query.split('/')[-1].upper()
            
        ## otherwise
        else:
            ## get all attributes
            attribute = '*'
            
        ## split query into element and WHERE AND JOINT PARTS
        a = query.split('/')[0].split('[')
        table = a[0].upper()
        cndts = a[-1].replace(']', '').replace('@', '').upper() if len(a) > 1 else ''
        joint_tables = a[1:-1]

        ## initiate sql query
        sql_cmd = f"""SELECT {attribute} FROM {table}"""

        ## for each join expression in query
        for t in joint_tables:
            if t in tables: 
                ## add join expression to sql query
                sql_cmd += f""" JOIN {t.upper()} ON ({t}.{table}_ID = {table}.ID)"""

        ## if query contains conditions
        if cndts:
            ## for each condition
            for cndt in cndts.split(' AND '):
                ## split condtion's tag and value
                tag_value = re.split('=|<|>|!=|<=|>=|<>', cndt)
                tag = tag_value[0]
                value = tag_value[1]
                
                ## if tag in xml scheme
                if tag.lower() in scheme:
                    ## look for tag's parent
                    tags = look_up(scheme, tag.lower(), [])
                    
                    ## if tag is not a column of table
                    if tag.lower() not in tables[tags[0]]:
                        ## add joint and where conditions
                        join_cndts += f""" JOIN {tags[0].upper()} ON ({tags[0].upper()}.{table}_ID = {table}.ID)"""
                        where_cndts += [f""" {tag}={value}"""]
                    ## otherwise
                    else:
                        ## add where condition
                        where_cndts += [f""" {cndt}"""]
                else:
                    where_cndts += [f""" {cndt}"""]

        ## add joint expressions to sql query
        sql_cmd += join_cndts
        
        ## add where conditions to query
        if where_cndts:
            sql_cmd += ' WHERE ' + ' AND '.join(where_cndts)
    
    return sql_cmd.replace('@', '')
