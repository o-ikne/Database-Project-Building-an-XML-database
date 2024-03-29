
import pandas as pd

import sqlite3
import xml.sax 
import xml.etree.ElementTree as ET




class TravahoHandler(xml.sax.ContentHandler):
    def __init__(self, dtd_file, xml_file):
        self.dtd_file = dtd_file
        self.xml_file = xml_file
        self.tree = ET.parse(xml_file)
        self.get_tags()
        self.execute_commands()
        self.id_employe = None
        
    def get_commands(self):
        assert self.dtd_file.endswith('.dtd')
        self.tables = self.get_tables()
        sql_commands = []
        for table in self.tables:
            header = f"CREATE TABLE IF NOT EXISTS {table.upper()}"
            body = f"ID INTEGER PRIMARY KEY AUTOINCREMENT"
            for e, column in enumerate(self.tables[table]):
                body += f", {column.split()[0].upper()}  TEXT"
                if column.split()[-1].strip('>') == "#REQUIRED":
                    body += " NOT NULL"
            upper_e = table.replace('_', '-')
            if upper_e in self.tag_tables and (tag:=self.tag_tables[upper_e]):#(tag:=self.fetch_id(upper_e)):
                tag = next(iter(tag))
                body += f", ID_{tag.upper()} INT REFERENCES {tag.upper()}(ID)"
            command = f"{header} ({body})"
            sql_commands.append(command)
        return sql_commands
            
    def execute_commands(self):
        self.commands = self.get_commands()
        print('DROP EXISTING TABLES :')
        print('======================')
        for table in self.tables:
            print(f'''DROP TABLE IF EXISTS {table.upper()}''')
            c.execute(f'''DROP TABLE IF EXISTS {table.upper()}''')
        print('\n')
        print('CREATING NEW TABLES :')
        print('=====================')
        for command in self.commands:
            print(command)
            c.execute(command)

    def get_tables(self):
        tables = dict()
        self.switcher = dict()
        with open(self.dtd_file) as f:
            text = f.read()
            elements = list(filter(lambda x:x !='', text.split('\n\n')))
            for element in elements:
                pcdata = None
                element = element.replace('-', '_').strip('>')
                attributes = element.split('<!ATTLIST')
                element_name = attributes[0].split()[1]
                if attributes[0].count('!ELEMENT') == 2:
                    col_to_add = attributes[0].split()[-2]
                    #if attributes[0].split()[-1].strip('>') != "EMPTY":
                    self.switcher[col_to_add] = element_name
                term = attributes[0].split()[-1].strip('>') 
                if term == "(#PCDATA)":
                        pcdata = attributes[0].split()[-2]
                        exec("%s = %s" % ('self.inside_' + pcdata.replace('-', '_'), False))
                        exec("%s = %s" % ('self.id_' + pcdata.replace('-', '_'), False))
                element_attributes = list(map(str.strip, element.split('\n')[2:]))
                element_attributes = list(filter(lambda x:x != '', element_attributes))
                for element_attribute in element_attributes:
                    cols = element_attribute.split()
                    if  cols[0] == "<!ATTLIST":
                        element_attributes = element_attributes[1:]
                    
                tables[element_name] =  element_attributes
                upper_e = element_name.replace('_', '-')
                if pcdata:
                    element_attributes.insert(0, pcdata)
        self.reverse_switcher = dict([(value, key) for key, value in self.switcher.items()]) 
        return tables

    def formats_switcher(self): 
        return {'TEXT':str, 'INTEGER':int, "FLOAT":float, "BOOL":bool}
        
    def startDocument(self):
        print(self.tag_tables)
        print('\n')
        print('INSERT DATA :')
        print("=============")
    

    def startElement(self, xml_name, xml_attrs):
        xml_name = xml_name.replace('-', '_')
        self.current_name = xml_name
        if 'inside_'+xml_name in self.__dict__:
            exec("%s = %s" % ('self.inside_'+xml_name, True))
            exec("%s = %s" % ('self.nom_'+xml_name, "str()")) 
        if xml_name in self.switcher:
            xml_name = self.switcher[xml_name]
        
        for col in self.tables[xml_name]:
            attribute = col.split()[0]
            if attribute in xml_attrs:
                exec(attribute.upper() +  "= xml_attrs[attribute].strip()")
            else:
                exec(attribute.upper() + " = False")
                        
        attributes = list(map(lambda x: x.split()[0].upper(), self.tables[xml_name]))
        variables  = list(map(eval, attributes))
        exec("%s = %s" % ('self.variables', variables))
        exec("%s = %s" % ('self.attributes', attributes))
        #print(f"""INSERT INTO {xml_name_upper.replace('-', '_').upper()}{tuple(attributes)} VALUES {tuple(variables)}""")
        if any(variables) and not 'nom_'+xml_name in self.__dict__:
            try:
                c.execute(f"""INSERT INTO {xml_name.upper()}{tuple(attributes)} VALUES {tuple(variables)}""")
            except Exception as e:
                print("ERROR WHILE INSERTING DATA")
                print('__________________________________________________________________')
                print(e)
                print(variables)
                print('__________________________________________________________________')
        
        if 'id_' + xml_name in self.__dict__:
            exec("%s = %s" % ('self.id_' + xml_name, c.lastrowid))
            


    def characters(self, text):
        table = self.current_name
        if 'inside_'+table in self.__dict__ and eval('self.inside_'+table):
            if text.strip():
                a = eval('self.nom_'+table)
                if len(a) > 0:
                    a += ', '
                a += text.strip() 
                exec("%s = %s" % ('self.nom_'+table, "a"))
            
    def endElement(self, xml_name):
        xml_name_ = xml_name.replace('-', '_')
        c_table = xml_name
        if xml_name in self.switcher:
            c_table = self.switcher[xml_name]
        if 'nom_'+xml_name_ in self.__dict__:
            attributes = self.attributes
            variables  = self.variables
            attributes = ', '.join(attributes)
            if (e:=eval('self.nom_'+xml_name)):
                variables[0] = e
            if len(variables) == 1:
                command = f"""SELECT ID FROM {c_table.upper()} WHERE ({attributes})=('{variables[0]}')""" 
            if len(variables) >1:
                command = f"""SELECT ID FROM {c_table.upper()} WHERE ({attributes})={tuple(variables)}""" 
            c.execute(command)
            d = c.fetchone()
            if d:
                exec("%s = %s" % ('self.id_' + xml_name_, d['ID']))
            else:
                #if any(variables):
                    if len(variables) == 1:
                        cmd = f"""INSERT INTO {c_table.upper()}({attributes}) VALUES ('{variables[0]}')"""
                    else:
                        cmd = f"""INSERT INTO {c_table.upper()}({attributes}) VALUES {tuple(variables)}"""
                    c.execute(cmd)
                    exec("%s = %s" % ('self.id_' + xml_name_, c.lastrowid))
            exec("%s = %s" % ('self.inside_' + xml_name_, False))
                          
        else:
            if c_table in self.tag_tables and (tag:=self.tag_tables[c_table]):
                tag = next(iter(tag))
                try:
                    #UPDATE EMPLOYE SET ID_DEPARTEMENT = ? WHERE ID = ? (8, 25)
                    cmd = f"""UPDATE {c_table.upper()} SET ID_{tag.upper()} = ? WHERE ID = ? """
                    #cmd = f"""UPDATE {c_table.upper()} SET (ID, ID_{tag.upper()})={eval('self.id_'+xml_name_), eval('self.id_'+tag)}"""
                    #print(cmd, (eval('self.id_'+xml_name_), eval('self.id_'+tag)))
                    c.execute(cmd, (eval('self.id_'+xml_name_), eval('self.id_'+tag)))
                except Exception as e:
                    print("=================>", e)
                
    def endDocument(self):
        print('\n')
        print('DONE')
        print('===================')
        print('DATA HAS BEEN INSTERTED SUCCESSFULLY')
        conn.commit()
        
    def get_tags(self):
        root = self.tree.getroot()
        self.tag_tables = self.tags(root[0])
    
    def tags(self, e, d={}):
        d[e.tag] = {i.tag for i in e}
        for i in e:
            self.tags(i, d)
        return d
                        
    def fetch_id(self, table):
        for tag in self.tag_tables:
            if table in self.tag_tables[tag]:
                return tag



conn = sqlite3.connect('xml-sax/emp-rh.db')
conn.row_factory = sqlite3.Row
c = conn.cursor()

if __name__ == '__main__':
    parser = xml.sax.make_parser()
    parser.setContentHandler(TravahoHandler("xml-sax/rh.dtd", 'xml-sax/rh.xml'))
    parser.parse(open('xml-sax/rh.xml'))
    conn.close()
