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
        self.table_roles = []

        self.sources = []
        self.uncertainties = []
        self.uncertainties_values = []

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

        self.uncertainties.append(self.common_data['uncertainty_name'])

        for i in range(len(self.table)):
            self.uncertainties_values.append([])
            if 'source' in self.common_data:
                self.sources.append(self.common_data['source'])
            for j in range(len(self.table_quantities)):
                self.uncertainties_values[i].append([])
                if 'uncertainty_name' in self.common_data:
                    self.uncertainties_values[i][j].append(self.common_data['precision'])
        print(self.uncertainties_values)

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
        return self.common_data, self.table, self.table_quantities, self.table_dimensions, self.table_roles, \
            self.sources, self.uncertainties, self.uncertainties_values


class SqlTransformer:
    def __init__(self):
        self.common_data = None
        self.table = []
        self.table_quantities = []
        self.table_dimensions = []
        self.table_roles = []
        self.sources = []
        self.uncertainties = []
        self.uncertainties_values = []
        self.cursor = None
        self.sql = ""

    def check_data(self):
        for i in ['name', 'formula', 'state', 'description', 'uncertainty_name']:
            if i not in self.common_data:
                raise Exception("No {0} found in the document".format(i))

    def get_id(self, table, condition):
        self.cursor.execute("select id from ont.{0} where {1}".format(table, condition))
        found_id = self.cursor.fetchone()
        if found_id is None:
            raise Exception("Id where '{0}' not found in table '{1}'".format(condition, table))
        else:
            return found_id[0]

    def get_or_create_id(self, table, condition, sequence, values):
        self.cursor.execute("select id from ont.{0} where {1}".format(table, condition))
        found_id = self.cursor.fetchone()
        if found_id is None:
            self.sql += "insert into ont.{0} values (nextval('{1}'), {2});\n".format(table, sequence, values)
            return "currval('{0}')".format(sequence)
        else:
            return found_id[0]

    def create_id(self, table, sequence, values):
        self.sql += "insert into ont.{0} values (nextval('{1}'), {2});\n".format(table, sequence, values)
        return "currval('{0}')".format(sequence)

    def insert_points_of_measure(self, state_id, source_id, dataset_id):
        for i in range(len(self.table_quantities)):
            quantity = self.table_quantities[i]
            dimension = self.table_dimensions[i]
            role = self.table_roles[i]

            self.sql += "\n-- {0} column\n".format(quantity)

            role_id = self.get_id("physical_quantity_roles", "role_type = '{0}'".format(role))

            dimension = re.sub('\*', '/', dimension)
            dimension_id = self.get_id("dimensions", "dimension_name = '{0}'".format(dimension))

            quantity_id = self.get_or_create_id(
                "physical_quantities", "lower(quantity_designation) = '{0}'".format(quantity),
                "physical_quantities_id_seq",
                "'{0}', '{0}', {1}".format(quantity, role_id))
            if quantity_id == "currval('physical_quantities_id_seq')":
                self.sql += "insert into ont.physical_quantities_states values " \
                            "({0}, currval('physical_quantities_id_seq'));\n".format(state_id)
                self.sql += "insert into ont.physical_quantities_dimensions values " \
                            "(currval('physical_quantities_id_seq'), {0});\n".format(dimension_id)

            self.sql += "insert into ont.points_of_measure values"

            for j in range(len(self.table)):
                measure = self.table[j][i]
                self.sql += "\n\t(nextval('points_of_measure_id_seq'), {0}, {1}, {2}, {3}, {4}, {5}),".format(
                    measure, j, dataset_id, source_id, dimension_id, quantity_id)
            self.sql = self.sql[:-1] + ';\n'

    def insert_uncertainties(self):
        self.sql += "\n-- Uncertainties\n"

        uncertainty_type_ids = []
        for uncertainty in self.uncertainties:
            uncertainty_type_ids.append(self.get_id("uncertainty_types", "uncertainty_name = '{0}'".format(
                uncertainty)))

        self.sql += "insert into ont.measurement_uncertainties values"
        for i in range(len(self.table_dimensions)):
            for j in range(len(self.table)):
                for k in range(len(uncertainty_type_ids)):
                    if k == 0:
                        self.sql += "\n\t(nextval('measurement_uncertainties_id_seq'), {0}, " \
                                    "nextval('points_of_measure_id_seq_copy'), {1}),".format(
                                        self.uncertainties_values[j][i][k], uncertainty_type_ids[k])
                    else:
                        self.sql += "\n\t(currval('measurement_uncertainties_id_seq'), {0}, " \
                                    "nextval('points_of_measure_id_seq_copy'), {1}),".format(
                                        self.uncertainties_values[j][i][k], uncertainty_type_ids[k])
        self.sql = self.sql[:-1] + ';\n'

    def generate_sql(self, file_name, cursor):
        xls_reader = XlsReader()
        self.common_data, self.table, self.table_quantities, self.table_dimensions, self.table_roles, \
            self.sources, self.uncertainties, self.uncertainties_values = xls_reader.read_table(file_name)
        self.cursor = cursor
        print(self.common_data)
        print(self.table_quantities)
        print(self.table_dimensions)
        print(self.table[0])

        self.check_data()

        self.sql = "begin;\n\n"

        # Getting state
        state_id = self.get_id("states", "lower(state_name) = '{0}'".format(self.common_data['state']))

        # Getting chemical substance
        substance_id = self.get_or_create_id("chemical_substances",
                                             "chemical_formula = '{0}' or substance_name = '{1}'".format(
                                                 self.common_data['formula'], self.common_data['name']),
                                             "chemical_substances_id_seq",
                                             "'{0}', '{1}'".format(
                                                 self.common_data['formula'], self.common_data['name']))

        # Getting data source
        source_id = self.get_or_create_id("data_sources",
                                          "data_source_name = '{0}'".format(self.common_data['source']),
                                          "data_sources_id_seq",
                                          "'{0}'".format(self.common_data['source']))

        # Getting substances_in_state
        if substance_id != "currval('chemical_substances_id_seq')":
            substance_in_state_id = self.get_or_create_id(
                "substances_in_states",
                "substance_id = {0} and state_id = {1}".format(substance_id, state_id),
                "substances_in_states_id_seq",
                "'id' || {0} || '_' || {1} || '_c','id' || {0} || '_' || {1} || '_f', FALSE, {0} , {1}".format(
                    substance_id, state_id))
        else:
            substance_in_state_id = self.create_id(
                "substances_in_states", "substances_in_states_id_seq",
                "'id' || {0} || '_' || {1} || '_c','id' || {0} || '_' || {1} || '_f', FALSE, {0} , {1}".format(
                    substance_id, state_id))

        # Getting data set
        dataset_id = self.create_id("data_sets", "data_sets_id_seq", "'{0}', '{1}', '{2}', {3}".format(
            file_name, self.common_data['description'], re.sub('-', '', str(datetime.date.today())),
            substance_in_state_id))

        # Creating temporary points of measure sequence copy
        self.sql += "\ndrop sequence if exists points_of_measure_id_seq_copy;\n" \
                    "create temp sequence points_of_measure_id_seq_copy;\n" \
                    "select setval('points_of_measure_id_seq_copy', currval('points_of_measure_id_seq'));\n"

        self.insert_points_of_measure(state_id, source_id, dataset_id)
        self.insert_uncertainties()

        self.sql += "\ncommit;"

        script_file = open(re.sub('\.xls.*', '.sql', file_name), mode='w')
        script_file.write(self.sql)
        script_file.close()

connection_file = open('ConnectionString.txt', mode='r')
connection = psycopg2.connect(connection_file.read())
curs = connection.cursor()

st = SqlTransformer()
for file in glob.glob('./*.xls*'):
    file = file[2:]
    if file[0] != '~':
        st.generate_sql(file, curs)
