import openpyxl
import re
import datetime
import glob
import psycopg2


def comp_str(s1, s2):
    return s1.lower() == s2.lower()


class XlsReader:
    def __init__(self):
        self.sheet = None
        self.common_data = dict()

        self.table = []
        self.table_quantities = []
        self.table_dimensions = []
        self.table_types = []
        self.table_roles = []

        self.functions = []
        self.arguments = []
        self.constants = []
        self.substance_constants = []

    def extend_argument(self, argument):
        if argument.lower() + ' =' in self.common_data:
            argument_str = self.common_data[argument.lower() + ' =']
            value = re.findall('([0-9]+(\.[0-9]+)*)', argument_str)[0][0]
            argument_str = re.sub(value, '', argument_str)
            dimension = argument_str.strip()

            self.table_quantities.append(argument)
            self.table_dimensions.append(dimension)
            for i in range(len(self.table)):
                self.table[i].append(value)

            return True
        else:
            return False

    def extend_data(self):
        if 'precision' in self.common_data:
            if 'class' in self.common_data['precision']:
                self.common_data['precision'] = re.sub("class\s", "", self.common_data['precision'])
                self.common_data['uncertainty_name'] = 'Precision class'
        if 'description' not in self.common_data:
            self.common_data['description'] = 'there are no information'
        if 'functions' in self.common_data:
            self.functions = [x.strip() for x in self.common_data['functions'].split(',')]
        if 'arguments' in self.common_data:
            self.arguments = [x.strip() for x in self.common_data['arguments'].split(',')]
        if 'constants' in self.common_data:
            self.constants = [x.strip() for x in self.common_data['constants'].split(',')]

        for function in self.functions:
            if function not in self.table_quantities:
                raise Exception("Function {0} not found in table".format(function))
        for argument in self.arguments:
            if argument not in self.table_quantities:
                if not self.extend_argument(argument):
                    raise Exception("Argument {0} not found in table or data".format(argument))

        for quantity in self.table_quantities:
            if quantity in self.functions:
                self.table_roles.append('func')
            elif quantity in self.arguments:
                self.table_roles.append('arg')
            elif quantity in self.constants:
                self.table_roles.append('cnst')
            else:
                raise Exception("Quantity {0} not found in functions/arguments/constants".format(quantity))

    def read_table(self, file_name):
        table_row = None
        readable_rows = []

        wb = openpyxl.load_workbook(file_name)
        self.sheet = wb.get_sheet_by_name(wb.get_sheet_names()[0])
        max_row = self.sheet.max_row
        rows = self.sheet.rows

        for i in range(len(self.sheet.rows)):
            row = rows[i]
            if row[0].value is not None and comp_str(row[0].value, 'table'):
                table_row = i + 1
                break
            for j in range(len(row) - 1):
                if row[j].value is not None and row[j + 1].value is not None:
                    self.common_data[row[j].value.lower()] = row[j + 1].value
                    j += 1

        for i in range(len(rows[table_row])):
            if rows[table_row][i].value is not None:
                readable_rows.append(i)
                self.table_quantities.append(rows[table_row][i].value)
                if rows[table_row + 1][i].value is not None:
                    self.table_dimensions.append(rows[table_row + 1][i].value)
                else:
                    self.table_dimensions.append(None)

        for i in range(table_row + 2, max_row):
            if rows[i][0].value is None and rows[i - 1][0].value is None:
                break
            if rows[i][0].value is None:
                continue
            row = rows[i]
            read_row = []
            for j in readable_rows:
                    read_row.append(row[j].value)
            self.table.append(read_row)

        self.extend_data()
        return self.common_data, self.table, self.table_quantities, self.table_dimensions, self.table_roles


class SqlTransformer:
    def __init__(self):
        self.common_data = None
        self.table = []
        self.table_quantities = []
        self.table_dimensions = []
        self.table_roles = []

    def check_data(self):
        for i in ['name', 'formula', 'state', 'description', 'uncertainty_name']:
            if i not in self.common_data:
                raise Exception("No {0} found in the document".format(i))

    @staticmethod
    def get_or_create(table, variable, condition, values):
        return "select id into {1} from ont.{0} where {2};\n" \
               "if {1} is NULL then\n" \
               "\tinsert into ont.{0} values ({3}) returning id into {1};\n" \
               "end if;\n\n".format(table, variable, condition, values)

    def measure_values(self):
        sql = ""
        for i in range(len(self.table)):
            row = self.table[i]
            for j in range(len(row)):
                sql += "\n\t(nextval('points_of_measure_id_seq'), " \
                       "{0}, " \
                       "\t{1}, " \
                       "(select id from data_set), " \
                       "data_source_id, " \
                       "(select id from ont.dimensions where dimension_name = '{2}'), " \
                       "(select id from ont.physical_quantities where quantity_designation = '{3}')),".format(
                        row[j],
                        i,
                        self.table_dimensions[j],
                        self.table_quantities[j]
                        )
        return sql[:-1]

    def uncertainty_values(self):
        sql = ""
        for i in range(len(self.table)):
            row = self.table[i]
            for j in range(len(row)):
                sql += "\n\t(nextval('measurement_uncertainties_id_seq'), " \
                       "'{0}', " \
                       "nextval('points_of_measure_id_seq_copy'), " \
                       "(select id from uncertainty_type)),".format(
                        self.common_data['precision'])
        return sql[:-1]

    def insert_rows(self, file_name):
        return "with\n" \
               "state as (select id from ont.states where lower(state_name) = '{0}'),\n" \
               "data_set as (insert into ont.data_sets " \
               "values(nextval('data_sets_id_seq'), '{1}', '{2}', '{3}', substance_in_state_id) returning id)\n" \
               "insert into ont.points_of_measure values {4};\n\n" \
               "with uncertainty_type as (select id from ont.uncertainty_types where uncertainty_name = '{5}')\n" \
               "insert into ont.measurement_uncertainties values {6};\n\n".format(
                self.common_data['state'],
                file_name,
                self.common_data['description'],
                re.sub('-', '', str(datetime.date.today())),
                self.measure_values(),
                self.common_data['uncertainty_name'],
                self.uncertainty_values())

    def generate_sql1(self, file_name, cursor):
        xls_reader = XlsReader()
        self.common_data, self.table, \
        self.table_quantities, self.table_dimensions, self.table_roles = xls_reader.read_table(file_name)
        print(self.common_data)
        print(self.table_quantities)
        print(self.table_dimensions)
        print(self.table[0])

        self.check_data()

        sql = "DO $$\n\ndeclare\n" \
              "\tchemical_substance_id bigint;\n" \
              "\tsubstance_in_state_id bigint;\n" \
              "\tdata_source_id        bigint;\n\n" \
              "begin\n\n"

        sql += self.get_or_create(
            "chemical_substances",
            "chemical_substance_id",
            "chemical_formula = '{1}' or substance_name = '{0}'".format(self.common_data['name'],
                                                                        self.common_data['formula']),
            "nextval('chemical_substances_id_seq'), '{1}', '{0}'".format(self.common_data['name'],
                                                                         self.common_data['formula']))
        sql += self.get_or_create(
            "substances_in_states",
            "substance_in_state_id",
            "substance_id = chemical_substance_id",
            "nextval('substances_in_states_id_seq'),"
            "'id' || chemical_substance_id || '_' || (select id from ont.states where lower(state_name)='{0}') || '_c',"
            "'id' || chemical_substance_id || '_' || (select id from ont.states where lower(state_name)='{0}') || '_f',"
            "FALSE,"
            "chemical_substance_id,"
            "(select id from ont.states where lower(state_name) = '{0}')".format(self.common_data['state']))

        sql += self.get_or_create(
            "data_sources",
            "data_source_id",
            "data_source_name = '{0}'".format(self.common_data['source']),
            "nextval('data_sources_id_seq'), '{0}'".format(self.common_data['source']))

        sql += "drop sequence if exists points_of_measure_id_seq_copy;\n" \
               "create temp sequence points_of_measure_id_seq_copy;\n" \
               "perform setval('points_of_measure_id_seq_copy', currval('points_of_measure_id_seq'));\n\n"

        sql += self.insert_rows(file_name)

        sql += "\nEND $$\nLANGUAGE plpgsql;"

        script_file = open(re.sub('\.xls.*', '.sql', file_name), mode='w')
        script_file.write(sql)
        script_file.close()

    def generate_sql(self, file_name, cursor):
        xls_reader = XlsReader()
        self.common_data, self.table, \
        self.table_quantities, self.table_dimensions, self.table_roles = xls_reader.read_table(file_name)
        print(self.common_data)
        print(self.table_quantities)
        print(self.table_dimensions)
        print(self.table[0])

        self.check_data()

        sql = "begin;\n\n"

        # Getting state
        cursor.execute("select id from ont.states where lower(state_name) = '{0}'".format(self.common_data['state']))
        state_id = cursor.fetchone()
        if state_id is None:
            raise Exception("State {0} not found in database".format(self.common_data['state']))
        else:
            state_id = state_id[0]

        # Getting chemical substance
        cursor.execute("select id from ont.chemical_substances "
                       "where chemical_formula = '{0}' or substance_name = '{1}'".format(
            self.common_data['formula'],
            self.common_data['name']))
        substance_id = cursor.fetchone()
        if substance_id is None:
            sql += "insert into ont.chemical_substances values " \
                   "(nextval('chemical_substances_id_seq'), '{0}', '{1}');\n".format(
                self.common_data['formula'],
                self.common_data['name'])
            substance_id = "currval('chemical_substances_id_seq')"

        # Getting data source
        cursor.execute("select id from ont.data_sources where data_source_name = '{0}'".format(
            self.common_data['source']))
        source_id = cursor.fetchone()
        if source_id is None:
            sql += "insert into ont.data_sources values " \
                   "(nextval('data_sources_id_seq'), '{0}');\n".format(
                self.common_data['source'])
            source_id = "currval('data_sources_id_seq')"
        else:
            source_id = source_id[0]

        # Getting substances_in_state
        substance_in_state_id = None
        if substance_id != "currval('chemical_substances_id_seq')":
            cursor.execute("select id from ont.substances_in_states where substance_id = {0} and state_id = {1}".format(
                substance_id,
                state_id))
            substance_in_state_id = cursor.fetchone()
        if substance_in_state_id is None:
            sql += "insert into ont.substances_in_states values (nextval('substances_in_states_id_seq'), " \
                   "'id' || {0} || '_' || {1} || '_c','id' || {0} || '_' || {1} || '_f', FALSE, {0} , {1});\n".format(
                substance_id,
                state_id)
            substance_in_state_id = "currval('substances_in_states_id_seq')"
        else:
            substance_in_state_id = substance_in_state_id[0]

        # Getting data set
        sql += "insert into ont.data_sets values (nextval('data_sets_id_seq'), '{0}', '{1}', '{2}', {3});\n".format(
            file_name,
            self.common_data['description'],
            re.sub('-', '', str(datetime.date.today())),
            substance_in_state_id
        )
        dataset_id = "currval('data_sets_id_seq')"

        # Inserting points of measure
        for i in range(len(self.table_quantities)):
            quantity = self.table_quantities[i]
            dimension = self.table_dimensions[i]
            role = self.table_roles[i]

            sql += "\n-- {0} column\n".format(quantity)

            cursor.execute("select id from ont.physical_quantity_roles where role_type = '{0}'".format(role))
            role_id = cursor.fetchone()[0]

            dimension = re.sub('\*', '/', dimension)
            cursor.execute("select id from ont.dimensions where dimension_name = '{0}'".format(dimension))
            dimension_id = cursor.fetchone()[0]

            cursor.execute("select id from ont.physical_quantities where quantity_designation = '{0}'".format(
                quantity))
            quantity_id = cursor.fetchone()
            if quantity_id is None:
                sql += "insert into ont.physical_quantities values (nextval('physical_quantities_id_seq'), " \
                       "'{0}', '{0}', {1});\n".format(quantity, role_id)
                quantity_id = "currval('physical_quantities_id_seq')"
                sql += "insert into ont.physical_quantities_states values " \
                       "({0}, currval('physical_quantities_id_seq'));\n".format(state_id)
                sql += "insert into ont.physical_quantities_dimensions values " \
                       "(currval('physical_quantities_id_seq'), {0});\n".format(dimension_id)
            else:
                quantity_id = quantity_id[0]

            sql += "insert into ont.points_of_measure values"

            for j in range(len(self.table)):
                measure = self.table[j][i]
                sql += "\n\t(nextval('points_of_measure_id_seq'), {0}, {1}, {2}, {3}, {4}, {5}),".format(
                    measure,
                    j,
                    dataset_id,
                    source_id,
                    dimension_id,
                    quantity_id
                )
            sql = sql[:-1] + ';\n'

        sql += "\ncommit;"

        script_file = open(re.sub('\.xls.*', '.sql', file_name), mode='w')
        script_file.write(sql)
        script_file.close()

connection_file = open('ConnectionString.txt', mode='r')
connection = psycopg2.connect(connection_file.read())
cursor = connection.cursor()

st = SqlTransformer()
for file in glob.glob('./*.xls*'):
    file = file[2:]
    if file[0] != '~':
        st.generate_sql(file, cursor)
