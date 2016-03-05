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
        self.sources_table = []
        self.uncertainties_types = []
        self.uncertainties_values = []
        self.table_uncertainty_ids = []

        self.functions = []
        self.arguments = []
        self.constants = []
        self.substance_constants = []
        self.uncertainties = []

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
            substance_constants_designations = [i[0] for i in self.substance_constants]

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
                self.table_roles.append('cnst')
            elif quantity in substance_constants_designations:
                self.table_roles.append('scnst')
            else:
                raise Exception("Quantity {0} not found in functions/arguments/constants/sconstants/sources "
                                "and not an ID of uncertainty".format(quantity))

        uncertainties_columns = []
        for uncertainty in self.uncertainties:
            self.uncertainties_types.append(uncertainty[1])
            uncertainties_columns.append([])
            if len(uncertainty[3]) == 0:
                uncertainties_columns[-1] = None
            else:
                uncertainties_columns[-1] = uncertainty[3]

            for i in range(len(self.constants)):
                if uncertainties_columns[-1] is None or self.constants[i][0] in uncertainties_columns[-1]:
                    self.constants[i][4].append(uncertainty[1])
                    self.constants[i][5].append(uncertainty[2])
            for i in range(len(self.substance_constants)):
                if uncertainties_columns[-1] is None or self.substance_constants[i][0] in uncertainties_columns[-1]:
                    self.substance_constants[i][4].append(uncertainty[1])
                    self.substance_constants[i][5].append(uncertainty[2])

        for i in range(len(self.table)):
            if 'source' in self.common_data and self.sources_from_table[i] is None:
                self.sources.append(self.common_data['source'])
            elif self.sources_from_table[i] is not None:
                was_found = False
                for source in self.sources_table:
                    if self.sources_from_table[i] == source[0]:
                        self.sources.append(source[1])
                        was_found = True
                        break
                if not was_found:
                    raise Exception("Id for source {0} not found".format(self.sources_from_table[i]))
            else:
                raise Exception("Source for row {0} not found".format(i))
            for j in range(len(self.table_quantities)):
                for k in range(len(self.uncertainties)):
                    uncertainty = self.uncertainties[k]
                    if uncertainty[2] is None:
                        if uncertainties_columns[k] is not None and \
                                self.table_quantities[j] not in uncertainties_columns[k]:
                            self.uncertainties_values[i][j][k] = None
                        continue
                    # elif uncertainties_columns[k] is None or self.table_quantities[j] in uncertainties_columns[k]:
                    #     self.uncertainties_values[i][j].append(uncertainty[2])
                    # else:
                    #     self.uncertainties_values[i][j].append(None)
                    elif uncertainties_columns[k] is None or self.table_quantities[j] in uncertainties_columns[k]:
                        self.uncertainties_values[i][j].insert(k, uncertainty[2])
                    else:
                        self.uncertainties_values[i][j].insert(k, None)

    @staticmethod
    def find_next_section(rows, max_row, index):
        for i in range(index, max_row):
            if rows[i][0].value is not None and str(rows[i][0].value).lower() in [
                    'functions', 'arguments', 'constants', 'table', 'uncertainties', 'sources', 'sconstants']:
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
            elif type(rows[0][i].value) is int:
                uncertainty_rows.append(i)
                self.table_uncertainty_ids.append(rows[0][i].value)
            else:
                table_rows.append(i)
                self.table_quantities.append(rows[0][i].value)

        if len(source_rows) > 1:
            raise Exception("There can only be one source column")
        elif len(source_rows) == 0:
            for i in range(1, len(rows)):
                self.sources_from_table.append(None)

        for i in range(1, len(rows)):
            if rows[i][0].value is None and rows[i - 1][0].value is None:
                break
            if rows[i][0].value is None:
                continue
            self.uncertainties_values.append([])
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
                self.uncertainties.append((row[0].value, row[1].value, row[2].value, []))
                if row[3].value is not None:
                    for quantity in [x.strip() for x in row[3].value.split(',')]:
                        self.uncertainties[-1][3].append(quantity)

    def parse_constants(self, rows):
        for row in rows:
            if row[0].value is not None:
                self.constants.append([row[0].value, row[1].value, row[2].value, row[3].value])
                self.constants[-1].append([])
                self.constants[-1].append([])

    def parse_sconstants(self, rows):
        for row in rows:
            if row[0].value is not None:
                self.substance_constants.append([row[0].value, row[1].value, row[2].value, row[3].value])
                self.substance_constants[-1].append([])
                self.substance_constants[-1].append([])

    def parse_sources(self, rows):
        for row in rows:
            if row[0].value is not None:
                self.sources_table.append((row[0].value, row[1].value))

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
            elif section_name == 'sources':
                self.parse_sources(rows[current_section:next_section])
            elif section_name == 'sconstants':
                self.parse_sconstants(rows[current_section:next_section])

            current_section = next_section + 1
            next_section = self.find_next_section(rows, max_row, current_section)

        self.extend_data()
        return self.common_data, self.table, self.table_quantities, self.table_dimensions, self.table_roles, \
            self.sources, self.uncertainties_types, self.uncertainties_values, self.table_names, self.constants, \
            self.substance_constants


class SqlTransformer:
    def __init__(self):
        self.common_data = None
        self.table = []
        self.table_quantities = []
        self.table_dimensions = []
        self.substance_constants = []
        self.table_roles = []
        self.table_names = []
        self.sources = []
        self.uncertainties = []
        self.constants = []
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

    def insert_points_of_measure(self, table, table_quantities, table_dimensions, table_roles,
                                 table_names, state_id, source_ids, dataset_id, substance_id, start_row):
        for i in range(len(table_quantities)):
            quantity = table_quantities[i]
            dimension = table_dimensions[i]
            role = table_roles[i]
            name = table_names[i]

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

            if role == 'scnst':
                if quantity_id == "currval('physical_quantities_id_seq')":
                    self.sql += "insert into ont.chem_subst_quantities values ({0}, {1});\n".format(quantity_id,
                                                                                                    substance_id)
                else:
                    self.cursor.execute("select quantity_id from ont.chem_subst_quantities where "
                                        "quantity_id = {0} and substance_id = {1}".format(quantity_id, substance_id))
                    found_id = self.cursor.fetchone()
                    if found_id is None:
                        self.sql += "insert into ont.chem_subst_quantities values ({0}, {1});\n".format(quantity_id,
                                                                                                        substance_id)
            else:
                if quantity_id == "currval('physical_quantities_id_seq')" or state_id == "currval('states_id_seq')":
                    self.sql += "insert into ont.physical_quantities_states values ({0}, {1});\n".format(state_id,
                                                                                                          quantity_id)
                else:
                    self.cursor.execute("select state_id from ont.physical_quantities_states where "
                                        "physical_quantity_id = {0} and state_id = {1}".format(quantity_id, state_id))
                    found_id = self.cursor.fetchone()
                    if found_id is None:
                        self.sql += "insert into ont.physical_quantities_states values ({0}, {1});\n".format(state_id,
                                                                                                            quantity_id)

            if dimension is not None and (quantity_id == "currval('physical_quantities_id_seq')"
                                             or dimension_id == "currval('dimensions_id_seq')"):
                self.sql += "insert into ont.physical_quantities_dimensions values ({0}, {1});\n".format(quantity_id,
                                                                                                         dimension_id)
            elif dimension is not None:
                self.cursor.execute("select quantity_id from ont.physical_quantities_dimensions where "
                                    "quantity_id = {0} and dimension_id = {1}".format(quantity_id, dimension_id))
                found_id = self.cursor.fetchone()
                if found_id is None:
                    self.sql += "insert into ont.physical_quantities_dimensions values ({0}, {1});\n"\
                        .format(quantity_id,
                                dimension_id)

            self.sql += "insert into ont.points_of_measure values"

            for j in range(len(table)):
                measure = table[j][i]
                self.sql += "\n\t(nextval('points_of_measure_id_seq'), {0}, {1}, {2}, {3}, {4}, {5}),".format(
                    measure, j + start_row, dataset_id, source_ids[j], dimension_id, quantity_id)
            self.sql = self.sql[:-1] + ';\n'

    def insert_uncertainties(self, uncertainties, uncertainties_values, table_dimensions):
        if len(uncertainties) == 0:
            return

        self.sql += "\n-- Uncertainties\n"

        uncertainty_type_ids = []
        uncertainties_added = 0
        for uncertainty in uncertainties:
            uncertainty_id = self.get_or_create_id(
                "uncertainty_types",
                "uncertainty_name = '{0}'".format(uncertainty),
                "uncertainty_types_id_seq",
                "'{0}'".format(uncertainty))
            uncertainty_type_ids.append(uncertainty_id)
        for i in range(len(uncertainties) - 1, -1, -1):
            if uncertainty_type_ids[i] == "currval('uncertainty_types_id_seq')":
                uncertainty_type_ids[i] += " - {0}".format(uncertainties_added)
                uncertainties_added += 1

        self.sql += "insert into ont.measurement_uncertainties values"
        point_offset = 0
        for i in range(len(table_dimensions) - 1, -1, -1):
            for j in range(len(uncertainties_values) - 1, -1, -1):
                for k in range(len(uncertainty_type_ids)):
                    if uncertainties_values[j][i][k] is not None:
                        self.sql += "\n\t(nextval('measurement_uncertainties_id_seq'), '{0}', " \
                                    "currval('points_of_measure_id_seq') - {1}, {2}),".format(
                                        uncertainties_values[j][i][k],
                                        point_offset,
                                        uncertainty_type_ids[k])
                point_offset += 1
        self.sql = self.sql[:-1] + ';\n'

    def generate_sql(self, file_name, cursor):
        xls_reader = XlsReader()
        self.common_data, self.table, self.table_quantities, self.table_dimensions, self.table_roles, \
            self.sources, self.uncertainties, self.uncertainties_values, self.table_names, self.constants, \
            self.substance_constants = xls_reader.read_table(file_name)
        self.cursor = cursor
        self.check_data()

        self.sql = "begin;\n\n"

        # Getting state
        state_id = self.get_or_create_id("states", "lower(state_name) = '{0}'".format(self.common_data['state']),
                                         "states_id_seq", "'{0}', '{1}'".format(
                                                 self.common_data['formula'], self.common_data['name']))

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
                                              "data_source_name = '{0}'".format(source),
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

        self.insert_points_of_measure(self.table, self.table_quantities, self.table_dimensions, self.table_roles,
                                      self.table_names, state_id, source_ids, dataset_id, substance_id, 1)
        self.insert_uncertainties(self.uncertainties, self.uncertainties_values, self.table_dimensions)

        # Inserting constants
        ctable =           [[c[3]   for c in self.constants] + [c[3]    for c in self.substance_constants]]
        ctable_quantities = [c[0]   for c in self.constants] + [c[0]    for c in self.substance_constants]
        ctable_dimensions = [c[2]   for c in self.constants] + [c[2]    for c in self.substance_constants]
        ctable_roles =      ['cnst' for c in self.constants] + ['scnst' for c in self.substance_constants]
        ctable_names =      [c[1]   for c in self.constants] + [c[1]    for c in self.substance_constants]
        ctable_uncertainties = []
        ctable_uncertainties_values = [[]]
        for i in range(len(self.constants)):
            constant = self.constants[i]
            for uncertainy in constant[4]:
                ctable_uncertainties.append(uncertainy)
        for i in range(len(self.substance_constants)):
            constant = self.substance_constants[i]
            for uncertainy in constant[4]:
                ctable_uncertainties.append(uncertainy)
        cnt = 0
        for i in range(len(self.constants)):
            ctable_uncertainties_values[0].append([])
            for j in range(len(ctable_uncertainties)):
                ctable_uncertainties_values[0][i].append(None)
            for value in self.constants[i][5]:
                ctable_uncertainties_values[0][i][cnt] = value
                cnt += 1
        for i in range(len(self.substance_constants)):
            ctable_uncertainties_values[0].append([])
            for j in range(len(ctable_uncertainties)):
                ctable_uncertainties_values[0][i].append(None)
            for value in self.substance_constants[i][5]:
                ctable_uncertainties_values[0][i][cnt] = value
                cnt += 1

        print(ctable_uncertainties)
        self.insert_points_of_measure(ctable, ctable_quantities, ctable_dimensions, ctable_roles,
                                      ctable_names, state_id, source_ids, dataset_id, substance_id, 0)
        self.insert_uncertainties(ctable_uncertainties, ctable_uncertainties_values, ctable_dimensions)

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
