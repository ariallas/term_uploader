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
        self.table_names = []

        self.sources = []
        self.sources_from_table = []
        self.uncertainties_types = []
        self.uncertainties_values = []

        self.functions = []
        self.arguments = []
        self.constants = []
        self.substance_constants = []
        self.uncertainties = []

        self.const_uncertaities = \
            ["Standart", "Standart,relative to %", "Extended with a significance level of 95%",
             "Deviation from the approximating expression", "Precision class"]

    def extend_data(self):
        if 'description' not in self.common_data:
            self.common_data['description'] = 'there are no information'

        for function in self.functions:
            if function[0] not in self.table_quantities:
                raise Exception("Function {0} not found in table".format(function))

        for argument in self.arguments:
            if argument[0] not in self.table_quantities and argument[3] is None:
                raise Exception("Argument {0} not found in table or data".format(argument[0]))
            elif argument[0] not in self.table_quantities:
                self.table_quantities.append(argument[0])
                for i in range(len(self.table)):
                    self.table[i].append(argument[3])
                    if len(self.uncertainties_values[i]) > 0:
                        self.uncertainties_values[i].append(list(self.uncertainties_values[i][0]))
                    else:
                        self.uncertainties_values[i].append([])

        for quantity in self.table_quantities:
            functions_designations = [i[0] for i in self.functions]
            arguments_designations = [i[0] for i in self.arguments]
            constants_designations = [i[0] for i in self.constants]

            if quantity in functions_designations:
                function = [f for f in self.functions if f[0] == quantity][0]
                self.table_roles.append('func')
                self.table_names.append(function[1])
                self.table_dimensions.append(function[2])
            elif quantity in arguments_designations:
                argument = [a for a in self.arguments if a[0] == quantity][0]
                self.table_roles.append('arg')
                self.table_names.append(argument[1])
                self.table_dimensions.append(argument[2])
            elif quantity in constants_designations:
                constant = [c for c in self.constants if c[0] == quantity][0]
                self.table_roles.append('cnst')
            else:
                raise Exception("Quantity {0} not found in functions/arguments/constants".format(quantity))

        for uncertainty in self.uncertainties:
            self.uncertainties_types.append(uncertainty[0])

        for i in range(len(self.table)):
            if 'source' in self.common_data and self.sources_from_table[i] is None:
                self.sources.append(self.common_data['source'])
            elif self.sources_from_table[i] is not None:
                self.sources.append(self.sources_from_table[i])
            else:
                raise Exception("Source for row {0} not found".format(i))
            for j in range(len(self.table_quantities)):
                for uncertainty in self.uncertainties:
                    if uncertainty[1] is not None:
                        self.uncertainties_values[i][j].append(uncertainty[1])

        print(self.uncertainties_values)
        print(self.uncertainties)

    @staticmethod
    def find_next_section(rows, max_row, index):
        for i in range(index, max_row):
            if rows[i][0].value is not None and rows[i][0].value.lower() in [
                    'functions', 'arguments', 'constants', 'table', 'uncertainties']:
                return i
        return max_row

    def parse_common_data(self, rows):
        for i in range(len(rows)):
            row = rows[i]
            for j in range(len(row) - 1):
                if row[j].value is not None and row[j + 1].value is not None:
                    self.common_data[row[j].value.lower()] = row[j + 1].value
                    j += 1

    def parse_table(self, rows):
        table_rows = []
        source_rows = []
        uncertainty_rows = []

        for i in range(len(rows[0])):
            if rows[0][i].value is None:
                continue
            elif rows[0][i].value == 'Source':
                source_rows.append(i)
            elif rows[0][i].value in self.const_uncertaities:
                uncertainty_rows.append(i)
                self.uncertainties.append((rows[0][i].value, None, None))
            else:
                table_rows.append(i)
                self.table_quantities.append(rows[0][i].value)

        for i in range(1, len(rows)):
            self.uncertainties_values.append([])
            if rows[i][0].value is None and rows[i - 1][0].value is None:
                break
            if rows[i][0].value is None:
                continue
            row = rows[i]
            read_row = []
            for j in table_rows:
                read_row.append(row[j].value)
                self.uncertainties_values[-1].append([])
            for j in source_rows:
                self.sources_from_table.append(row[j].value)
            for j in uncertainty_rows:
                for k in range(len(self.table_quantities)):
                    self.uncertainties_values[-1][k].append(row[j].value)
            self.table.append(read_row)
        print(self.uncertainties)

    def parse_functions(self, rows):
        for row in rows:
            if row[0].value is not None:
                self.functions.append((row[0].value, row[1].value, row[2].value))

    def parse_arguments(self, rows):
        for row in rows:
            if row[0].value is not None:
                self.arguments.append((row[0].value, row[1].value, row[2].value, row[3].value))

    def parse_uncertainties(self, rows):
        for row in rows:
            if row[0].value is not None:
                self.uncertainties.append((row[0].value, row[1].value, row[2].value))

    def parse_constants(self, rows):
        for row in rows:
            if row[0].value is not None:
                self.constants.append((row[0].value, row[1].value, row[2].value, row[3].value))

    def read_table(self, file_name):
        wb = openpyxl.load_workbook(file_name)
        self.sheet = wb.get_sheet_by_name(wb.get_sheet_names()[0])
        max_row = self.sheet.max_row
        rows = self.sheet.rows

        current_section = 0
        next_section = self.find_next_section(rows, max_row, current_section + 1)
        section_name = ""

        while current_section < max_row:
            if current_section != 0:
                section_name = rows[current_section - 1][0].value.lower()

            if current_section == 0:
                self.parse_common_data(rows[current_section:next_section])
            elif section_name == 'functions':
                self.parse_functions(rows[current_section:next_section])
            elif section_name == 'arguments':
                self.parse_arguments(rows[current_section:next_section])
            elif section_name == 'table':
                self.parse_table(rows[current_section:next_section])
            elif section_name == 'uncertainties':
                self.parse_uncertainties(rows[current_section:next_section])
            elif section_name == 'constants':
                self.parse_constants(rows[current_section:next_section])

            current_section = next_section + 1
            next_section = self.find_next_section(rows, max_row, current_section)

        print(self.constants)
        self.extend_data()
        return self.common_data, self.table, self.table_quantities, self.table_dimensions, self.table_roles, \
            self.sources, self.uncertainties_types, self.uncertainties_values, self.table_names


class SqlTransformer:
    def __init__(self):
        self.common_data = None
        self.table = []
        self.table_quantities = []
        self.table_dimensions = []
        self.table_roles = []
        self.table_names = []
        self.sources = []
        self.uncertainties = []
        self.uncertainties_values = []
        self.cursor = None
        self.sql = ""

    def check_data(self):
        for i in ['name', 'formula', 'state', 'description']:
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

    def insert_points_of_measure(self, state_id, source_ids, dataset_id):
        for i in range(len(self.table_quantities)):
            quantity = self.table_quantities[i]
            dimension = self.table_dimensions[i]
            role = self.table_roles[i]
            name = self.table_names[i]

            self.sql += "\n-- {0} column\n".format(quantity)

            role_id = self.get_id("physical_quantity_roles", "role_type = '{0}'".format(role))

            dimension_id = "NULL"
            if dimension is not None:
                dimension_id = self.get_or_create_id("dimensions", "dimension_name = '{0}'".format(dimension),
                                                     "dimensions_id_seq", "'{0}'".format(dimension))

            quantity_id = self.get_or_create_id(
                "physical_quantities", "lower(quantity_designation) = '{0}'".format(quantity.lower()),
                "physical_quantities_id_seq",
                "'{0}', '{1}', {2}".format(quantity, name, role_id))
            if quantity_id == "currval('physical_quantities_id_seq')":
                self.sql += "insert into ont.physical_quantities_states values " \
                            "({0}, currval('physical_quantities_id_seq'));\n".format(state_id)
                if dimension is not None:
                    self.sql += "insert into ont.physical_quantities_dimensions values " \
                                "(currval('physical_quantities_id_seq'), {0});\n".format(dimension_id)

            self.sql += "insert into ont.points_of_measure values"

            for j in range(len(self.table)):
                measure = self.table[j][i]
                self.sql += "\n\t(nextval('points_of_measure_id_seq'), {0}, {1}, {2}, {3}, {4}, {5}),".format(
                    measure, j, dataset_id, source_ids[j], dimension_id, quantity_id)
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
                was_added = False
                for k in range(len(uncertainty_type_ids)):
                    if self.uncertainties_values[j][i][k] is not None:
                        if not was_added:
                            self.sql += "\n\t(nextval('measurement_uncertainties_id_seq'), {0}, " \
                                        "nextval('points_of_measure_id_seq_copy'), {1}),".format(
                                            self.uncertainties_values[j][i][k], uncertainty_type_ids[k])
                            was_added = True
                        else:
                            self.sql += "\n\t(nextval('measurement_uncertainties_id_seq'), {0}, " \
                                        "currval('points_of_measure_id_seq_copy'), {1}),".format(
                                            self.uncertainties_values[j][i][k], uncertainty_type_ids[k])
        self.sql = self.sql[:-1] + ';\n'

    def generate_sql(self, file_name, cursor):
        xls_reader = XlsReader()
        self.common_data, self.table, self.table_quantities, self.table_dimensions, self.table_roles, \
            self.sources, self.uncertainties, self.uncertainties_values, self.table_names \
            = xls_reader.read_table(file_name)
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
        source_ids = []

        seen = set()
        unique_sources = [x for x in self.sources if not (x in seen or seen.add(x))]

        unique_source_ids = []
        inserted_sources = 0
        for source in unique_sources:
            source_id = self.get_or_create_id("data_sources",
                                              "data_source_name = '{0}'".format(self.common_data['source']),
                                              "data_sources_id_seq",
                                              "'{0}'".format(source))
            if source_id == "currval('data_sources_id_seq')":
                inserted_sources += 1
            unique_source_ids.append(source_id)
        for i in range(len(unique_source_ids)):
            if unique_source_ids[i] == "currval('data_sources_id_seq')":
                inserted_sources -= 1
                unique_source_ids[i] += " - " + str(inserted_sources)
        for source in self.sources:
            source_ids.append(unique_source_ids[unique_sources.index(source)])

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

        self.insert_points_of_measure(state_id, source_ids, dataset_id)
        self.insert_uncertainties()

        self.sql += "\nrollback;"

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
