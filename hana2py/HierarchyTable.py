'''This module creates the flattened hierarchy table from SAP BW'''

import json
import pkg_resources
import pandas as pd
from hana2py import utilities as u

class HierarchyTable():
    """
    Class to generate a flattened hierarchy table from a hierarchy in SAP BW.
    """

    def __init__(self, hierarchy, generated_table_schema, engine,
                 hierarchy_version='data/hierarchy_version.json'):

        self.hierarchy = hierarchy
        self.engine = engine
        self.generated_table_schema = generated_table_schema
        self.hierarchy_version = hierarchy_version

        with open(self.hierarchy_version, 'r') as hierarchy_file:
            hierarchy_info = json.load(hierarchy_file)

        self._query_file = pkg_resources.resource_filename(
            'hana2py', 'create_table_base_query.sql')

        self._hieid = hierarchy_info.get(self.hierarchy).get('hieid')
        self.schema_name = hierarchy_info.get(self.hierarchy).get('schema_name')
        self.table_name = hierarchy_info.get(self.hierarchy).get('table_name')
        self.generated_table_name = (self.hierarchy + '_HIER').upper()

        self.highest_level = self._find_highest_level()

        self.create_hierarchy_table()

    def _find_highest_level(self):
        """Identify the highest level that exists in this hierarchy

        :return:
            highest_level (int): highest level of the hierarchy table,
            will be the maximum column the flattened table expands to.
        """

        level = 1
        n_row = 1

        while n_row > 0:

            query = f'select count(*) from {self.table_name}'\
                    f'where HIEID = \'{self._hieid}\' ' \
                    f'and TLEVEL = {level}'

            n_df = pd.read_sql(query, con=self.engine)
            n_row = int(n_df.loc[0][0])
            level += 1

        highest_level = level-2

        return highest_level



    def _drop_table(self):
        """ Drop the generated table if exist.
        """

        if self.engine.dialect.has_table(self.engine, self.generated_table_name):
            query = u.get_sql_queries(self._query_file)[0]
            query = query.replace('GENERATED_SCHEMA_HERE',
                                  self.generated_table_schema)
            query = query.replace('GENERATED_TABLE_NAME_HERE',
                                  self.generated_table_name)

            message = f'Old table {self.generated_table_name} has been dropped.'

            u.execute_query(self.engine, query, message)
        else:
            print(f'Table {self.generated_table_name} does not exist, continue to create...')


    def _get_left_joins(self):

        left_join = ''
        highest_level = self.highest_level -1

        for level in range(highest_level, 0, -1):
            if level == highest_level:
                join_table = 'F'
            else:
                join_table = 'H' + str(level+1)

            left_join += f'\tLEFT OUTER JOIN {self.table_name} H{level} ' \
                f'ON H{level}.NODEID = {join_table}.PARENTID ' \
                f'AND H{level}.HIEID = F.HIEID\n'

        return left_join


    def _get_generated_loop(self):
        generated_loop = ''
        k = 0
        for j in range(self.highest_level, 1, -1):
            generated_loop += '(CASE F.TLEVEL\n'

            for i in range(1, j):
                level = self.highest_level -i + 1
                node_level = i+k
                col_level = level - 1
                generated_loop += f'\t\tWHEN {level} THEN H{node_level}.NODENAME\n'

            generated_loop += f'\tELSE \' \' END) AS L{col_level},\n\t'

            k = k+1

        generated_loop = generated_loop[:-3]
        return generated_loop

    def _get_main_table_query(self):
        query = u.get_sql_queries(self._query_file)[1]
        generated_loop = self._get_generated_loop()
        left_join = self._get_left_joins()

        query = query.replace('GENERATED_SCHEMA_HERE', self.generated_table_schema)
        query = query.replace('GENERATED_LOOP_HERE', generated_loop)
        query = query.replace('LEFT_JOIN_HERE', left_join)
        query = query.replace('HIEID_HERE', f'\'{self._hieid}\'')
        query = query.replace('MAIN_TABLE_NAME_HERE', self.table_name)
        query = query.replace('SCHEMA_NAME_HERE',
                              self.schema_name)
        query = query.replace('GENERATED_TABLE_NAME_HERE',
                              self.generated_table_name)

        return query

    def _get_node_text(self):
        NODETEXT = f'select *, case when node_text is null then case\n'
        for level in range(2, self.highest_level + 1):
            tlevel = [str(level) if level > 9 else '0' + str(level)][0]
            NODETEXT += f'\t\twhen tlevel=\'{tlevel}\' then t{level - 1}\n'

        NODETEXT += '\t\telse null end \n\t else node_text end as NODETEXT'

        return NODETEXT

    def _create_hierarchy_table_query(self):
        select_text = ''
        left_join_text = ''

        main_table_query = self._get_main_table_query()
        query = u.get_sql_queries(self._query_file)[2]

        for level in range(1, self.highest_level):
            select_text += f'\nt{level}.t as t{level},'
            left_join_text += '\nLEFT JOIN (SELECT NODENAME, TXTLG as T ' \
                              f'from "{self.schema_name}"."RSTHIERNODE" ' \
                              f'where HIEID = \'{self._hieid}\' ' \
                              f'and LANGU = \'E\') T{level} ' \
                              f'ON T{level}.NODENAME = h.L{level}'

        select_text = select_text[:-1]
        node_text = self._get_node_text()

        query = query.replace('SELECT_TEXT_HERE', select_text)
        query = query.replace('HIEID_HERE', '\'' + self._hieid + '\'')
        query = query.replace('GENERATED_SCHEMA_HERE',
                              self.generated_table_schema)
        query = query.replace('GENERATED_TABLE_NAME_HERE',
                              self.generated_table_name)
        query = query.replace('LEFT_JOIN_TEXT_HERE', left_join_text)
        query = query.replace('MAIN_TABLE_QUERY_HERE', main_table_query)
        query = query.replace('SCHEMA_NAME_HERE', self.schema_name)

        query = query.replace('NODE_TEXT_LOOP_HERE', node_text)

        query_file = 'create_{}_query.sql'.format(
            self.generated_table_name.lower())

        with open(query_file, 'w+') as file:
            file.write(query)

        query = query.replace('\n', ' ').replace('\t', ' ')

        return query

    def create_hierarchy_table(self):
        """Create hierarchy table in Hana """

        self._drop_table()

        query = self._create_hierarchy_table_query()
        message = f'Table {self.generated_table_name} has been created.'

        u.execute_query(self.engine, query, message)


    def get_hierarchy_table(self, top_n=None):
        """Get hierarchy table from Hana"""

        query = f'select * ' \
            f'from {self.generated_table_schema}.{self.generated_table_name}'

        if top_n is not None:
            query = query.replace('*', f'top {top_n} *')

        hierarchy_df = pd.read_sql(query, con=self.engine)

        return hierarchy_df
