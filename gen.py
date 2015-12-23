import openpyxl
import re


def comp_str(s1, s2):
    return s1.lower() == s2.lower()


class XlsReader:
    def __init__(self):
        self.sheet = None
        self.common_data = dict()
        self.table = []
        self.table_quantities = []
        self.table_dimensions = []

    def correct_data(self):
        if 'precision' in self.common_data:
            self.common_data['precision'] = re.sub("class\s", "", self.common_data['precision'])
        if 'state' in self.common_data:
            self.common_data['state'] = self.common_data['state'].lower()

    def read_table(self):
        table_row = None

        wb = openpyxl.load_workbook('1_mod.xlsx')
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
                    self.common_data[re.findall("\w+", row[j].value.lower())[0]] = row[j + 1].value
                    j += 1

        for i in range(len(rows[table_row])):
            if rows[table_row][i] is not None:
                self.table_quantities.append(rows[table_row][i].value)
            if rows[table_row + 1][i] is not None:
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
            for val in row:
                if val.value is not None:
                    read_row.append(val.value)
            self.table.append(read_row)

        self.correct_data()
        return self.common_data, self.table, self.table_quantities, self.table_dimensions


class SqlTransformer:
    def __init__(self):
        self.common_data = None
        self.table = []
        self.table_quantities = []
        self.table_dimensions = []

    def check_data(self):
        if 'name' not in self.common_data and 'formula' not in self.common_data:
            raise Exception("No substance name or formula found")

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

    def insert_rows(self):
        return "with\n" \
               "state as (select id from ont.states where lower(state_name) = '{0}'),\n" \
               "data_set as (insert into ont.data_sets " \
               "values(nextval('data_sets_id_seq'), '{1}', '{2}', '{3}', substance_in_state_id) returning id),\n" \
               "insert into ont.points_of_measure values {4};\n\n" \
               "with uncertainty_type as (select id from ont.uncertainty_types where uncertainty_name = '{5}')\n" \
               "insert into ont.measurement_uncertainties values {6};\n\n".format(
                self.common_data['state'],
                'file_name',
                'file type (there are no information)',
                'string-date',
                self.measure_values(),
                'uncertainty_name',
                self.uncertainty_values())

    def generate_sql(self):
        xls_reader = XlsReader()
        self.common_data, self.table, self.table_quantities, self.table_dimensions = xls_reader.read_table()
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
            "nextval('substances_in_states_id_seq'), 'asdasd', 'asdasd', FALSE, chemical_substance_id,"
            "(select id from ont.states where lower(state_name) = '{0}')".format(self.common_data['state']))

        sql += self.get_or_create(
            "data_sources",
            "data_source_id",
            "data_source_name = '{0}'".format(self.common_data['source']),
            "nextval('data_sources_id_seq'), '{0}'".format(self.common_data['source']))

        sql += "drop sequence if exists points_of_measure_id_seq_copy;\n" \
               "create temp sequence points_of_measure_id_seq_copy;\n" \
               "select setval('points_of_measure_id_seq_copy', currval('points_of_measure_id_seq'));\n\n"

        sql += self.insert_rows()

        sql += "\nEND $$\nLANGUAGE plpgsql;"

        script_file = open('script.sql', mode='w')
        script_file.write(sql)

st = SqlTransformer()
st.generate_sql()
