
from enconding_scheme import TravahoHandler


conn = sqlite3.connect('emp_3.db')
conn.row_factory = sqlite3.Row
c = conn.cursor()

parser = xml.sax.make_parser()
handler = TravahoHandler("DBS20/imdb.dtd", "DBS20/imdb-small.xml")
parser.setContentHandler(handler)

tables = dict()
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
    func = query.split('//')[0]
    if func:
        query = query[len(func):-1]
        sql_query = xpath_to_sql(query).split()
        sql_query.insert(1, func)
        sql_query.insert(3, ')')
        return ' '.join(sql_query)
    else:
        query = query[2:]
        where_cndts = []
        join_cndts = ''

        if '/' in query:
            attribute = query.split('/')[-1].upper()
        else:
            attribute = '*'
        a = query.split('/')[0].split('[')
        table = a[0].upper()
        cndts = a[-1].replace(']', '').replace('@', '').upper() if len(a) > 1 else ''
        joint_tables = a[1:-1]

        if func:
            sql_cmd = f"""SELECT {func}({attribute}) FROM {table}"""
        else:
            sql_cmd = f"""SELECT {attribute} FROM {table}"""

        for t in joint_tables:
            if t in tables: 
                sql_cmd += f""" JOIN {t.upper()} ON ({t}.{table}_ID = {table}.ID)"""

        if cndts:
            for cndt in cndts.split(' AND '):
                tag_value = re.split('=|<|>|!=|<=|>=|<>', cndt)
                tag = tag_value[0]
                value = tag_value[1]
                if tag.lower() in scheme:
                    tags = look_up(scheme, tag.lower(), [])
                    if tag.lower() not in tables[tags[0]]:
                        print(tag)
                        join_cndts += f""" JOIN {tags[0].upper()} ON ({tags[0]}.{table}_ID = {table}.ID)"""
                        where_cndts += [f""" {tag}={value}"""]
                    else:
                        where_cndts += [f""" {cndt}"""]
                else:
                    where_cndts += [f""" {cndt}"""]


        sql_cmd += join_cndts
        if where_cndts:
            sql_cmd += ' WHERE ' + ' AND '.join(where_cndts)
    
    return sql_cmd.replace('@', '')
