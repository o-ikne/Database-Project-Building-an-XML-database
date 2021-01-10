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
        self.id_movie = 1
        
    def get_tables(self):
        """return the tables and their attributes from the DTD file"""
        ## initiate tables
        tables = dict()
        ## switcher of linked attributes (ex:title and alternative-titles)
        self.switcher = dict()
        
        ## open the dtd file
        with open(self.dtd_file) as f:
            text = f.read()
            ## get element
            elements = list(filter(lambda x:x !='', text.split('\n\n')))
            
            ## for each element
            for element in elements:
                pcdata = None
                element = element.replace('-', '_').strip('>')
                
                ## get attributes
                attributes = element.split('<!ATTLIST')
                
                ## get element name
                element_name = attributes[0].split()[1]
                
                ## if it contains two elements
                if attributes[0].count('!ELEMENT') == 2:
                    col_to_add = attributes[0].split()[-2]
                    self.switcher[col_to_add] = element_name
                    term = attributes[0].split()[-1].strip('>') 
                    
                    ## if second element is not empty
                    if term == "(#PCDATA)":
                        pcdata = attributes[0].split()[-2]
                        ## create an inside_element attribute for the class
                        exec("%s = %s" % ('self.inside_' + element_name.replace('-', '_'), False))
                
                element_attributes = list(map(str.strip, element.split('\n')[2:]))
                element_attributes = list(filter(lambda x:x != '', element_attributes))
                ## for eachattribute
                for element_attribute in element_attributes:
                    if  element_attribute.split()[0] == "<!ATTLIST":
                        element_attributes = element_attributes[1:]

                tables[element_name] = element_attributes
                
                ## create an id attribute for the class
                exec("%s = %s" % ('self.id_' + element_name.replace('-', '_'), None))
                if pcdata:
                    element_attributes.insert(0, pcdata)
        return tables
        
    def get_commands(self):
        """return SQL commands to create SQL table"""
        
        ## checks that the file is dtd
        assert self.dtd_file.endswith('.dtd')
        
        ## getting tables and their attributes
        self.tables = self.get_tables()
        
        ## initiate commands
        sql_commands = []
        
        ## for each table
        for table in self.tables:
            ## initiate the command by creating the table
            header = f"CREATE TABLE IF NOT EXISTS {table.upper()}"
            
            ## add primary key
            body = f"ID INTEGER PRIMARY KEY AUTOINCREMENT"
            
            ## for each column of the table
            for e, column in enumerate(self.tables[table]):
                ## add attributes to commmand
                body += f", {column.split()[0].upper()}  TEXT"
                
                ## if the attribute is required
                if column.split()[-1].strip('>') == "#REQUIRED":
                    body += " NOT NULL"
                    
            upper_e = table.replace('_', '-')
            ## create the refernce ID
            if upper_e in self.tag_tables and (tag:=self.fetch_id(upper_e)):
                body += f", {tag.upper()}_ID INT REFERENCES {tag.upper()}(ID)"
            
            ## join header and body
            command = f"{header} ({body})"
            print(command)
            sql_commands.append(command)
        return sql_commands
            
    def execute_commands(self):
        """executes the SQL commands"""
        ## get SQL commands
        self.commands = self.get_commands()
        print('\n')
        print('DROP EXISTING TABLES :')
        print('======================')
        
        ## drop existing tables
        for table in self.tables:
            print(f'''DROP TABLE IF EXISTS {table.upper()}''')
            c.execute(f'''DROP TABLE IF EXISTS {table.upper()}''')
        ## execute commands
        for command in self.commands:
            c.execute(command)

    def get_tags(self):
        """return the tags of the each table"""
        root = self.tree.getroot()
        self.tag_tables = self.tags(root[0])
    
    def tags(self, tag, d={}):
        """return the tags for a given tag"""
        d[tag.tag] = {i.tag for i in tag}
        for i in tag:
            self.tags(i, d)
        return d
                        
    def fetch_id(self, table):
        """return the parent of a tag"""
        for tag in self.tag_tables:
            if table in self.tag_tables[tag]:
                return tag.replace('-', '_')

        
    def startDocument(self):       
        print('\n')
        print('INSERT DATA :')
        print("====================")
    

    def startElement(self, xml_name, xml_attrs):
        xml_name = xml_name.replace('-', '_')
        ## set self.inside_xml_name = True and initiate self.nom_xmml_name
        if 'inside_'+xml_name in self.__dict__:
            exec("%s = %s" % ('self.inside_'+xml_name, True))
            exec("%s = %s" % ('self.nom_'+xml_name, "str()"))
        
        ## switch linked attributes
        if xml_name in self.switcher:
            xml_name = self.switcher[xml_name]
          
        ## for column in table 'xml_name'
        for col in self.tables[xml_name]:
            attribute = col.split()[0]
            ## if attribute exists in xml attributes
            if attribute in xml_attrs:
                exec(attribute.upper() +  "= xml_attrs[attribute].strip()")
            else:
                exec(attribute.upper() + " = False")
                
        attributes = list(map(lambda x: x.split()[0].upper(), self.tables[xml_name]))
        variables  = list(map(eval, attributes))
        exec("%s = %s" % ('self.variables', variables))
        exec("%s = %s" % ('self.attributes', attributes))
        ## in case the number of attributes is 1 (just because we have a problem with 'tuple' function)
        ## tuple(1) = (1,)
        
        if len(self.tables[xml_name]) == 1:            
            ## execute the command
            
            if 'inside_'+xml_name in self.__dict__ and eval('self.inside_'+xml_name):
                if (e:=eval('self.nom_'+xml_name)):
                    variables[0] = e
            if any(variables):
                cmd = f"""INSERT INTO {xml_name.upper()}({attributes[0]}) VALUES ('{variables[0]}')"""
                c.execute(cmd)

        elif len(self.tables[xml_name]) > 1:
            try:        
                if 'inside_'+xml_name in self.__dict__ and eval('self.inside_'+xml_name):
                    if (e:=eval('self.nom_'+xml_name)):
                        variables[0] = e
                if any(variables):
                    cmd = f"""INSERT INTO {xml_name.upper()}{tuple(attributes)} VALUES {tuple(variables)}"""
                    c.execute(cmd)
                            
            except Exception as e:
                print("ERROR WHILE INSERTING DATA")
                print('__________________________________________________________________')
                print(e)
                print(variables)
                print('__________________________________________________________________')
                


    def characters(self, text):
        for table in self.tables:
            table = table.replace('-', '_')
            ## if we are inside the table
            if 'inside_'+table in self.__dict__ and eval('self.inside_'+table):
                if text.strip():
                    a = text.strip()
                    exec("%s = %s" % ('self.nom_'+table, "a"))
            
 
    def endElement(self, xml_name):
        xml_name = xml_name.replace('-', '_')
        if 'inside_' + xml_name in self.__dict__:
            ## get attributes and values
            attributes = self.attributes
            variables  = self.variables
            attributes = ', '.join(attributes)
            if (e:=eval('self.nom_'+xml_name)):
                variables[0] = e
                
            ## execute the command
            if len(variables) == 1:
                command = f"""SELECT ID FROM {xml_name.upper()} WHERE {attributes}='{variables[0]}'""" 
            if len(variables) >1:
                command = f"""SELECT ID FROM {xml_name.upper()} WHERE ({attributes})={tuple(variables)}""" 
            c.execute(command)
            
            ## fetch one element if matching
            d = c.fetchone()

            if any(variables):
                ## execute command
                if len(variables) == 1:
                    cmd = f"""INSERT INTO {xml_name.upper()}({attributes}) VALUES ('{variables[0]}')"""
                else:
                    cmd = f"""INSERT INTO {xml_name.upper()}({attributes}) VALUES {tuple(variables)}"""
                c.execute(cmd)
            
                
            exec("%s = %s" % ('self.inside_' + xml_name, False))

        c_table = xml_name
        if xml_name in self.switcher:
            xml_name = self.switcher[xml_name]

        if xml_name.replace('_','-') in self.tag_tables and (tag:=self.fetch_id(xml_name.replace('_','-') )):
            try:
                exec("%s = %s" % ('self.id_' + xml_name, c.lastrowid))
                cmd = f"""UPDATE {xml_name.upper()} SET {tag.upper()}_ID = ? WHERE ID = ? """
                c.execute(cmd, (eval('self.id_'+tag), eval('self.id_'+xml_name)))
            except Exception as e:
                print("=================>", e)
                
                
                
        if xml_name =='movie':
            #self.id_movie = c.lastrowid
            self.id_movie += 1

    def endDocument(self):
        print('\n')
        print('DONE')
        print('===================')
        print('DATA HAS BEEN INSTERTED SUCCESSFULLY')
        conn.commit()
        
        
conn = sqlite3.connect('emp_imdb.db')
conn.row_factory = sqlite3.Row
c = conn.cursor()

if __name__ == '__main__':
    parser = xml.sax.make_parser()
    handler = TravahoHandler("imdb.dtd", "imdb-small.xml")
    parser.setContentHandler(handler)
    parser.parse(open("imdb.dtd"))
    conn.close()
