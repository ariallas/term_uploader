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
            for i in range(len(row) - 1):
                if row[i].value is not None and row[i + 1].value is not None:
                    self.common_data[re.findall("\w+", row[i].value.lower())[0]] = row[i + 1].value
                    i += 1

        for quantity in rows[table_row]:
            if quantity.value is not None:
                self.table_quantities.append(quantity.value)
        for quantity_dimension in rows[table_row + 1]:
            if quantity_dimension.value is not None:
                self.table_dimensions.append(quantity_dimension.value)
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
        self.table = None
        self.table_quantities = None
        self.table_dimensions = None

    def check_data(self):
        if 'name' not in self.common_data and 'formula' not in self.common_data:
            raise Exception("No substance name or formula found")

    @staticmethod
    def get_or_create(table, variable, condition, values):
        return "select id into {1} from ont.{0} where {2};\n" \
               "if {1} is NULL then\n" \
               "\tinsert into ont.{0} values ({3}) returning id into {1};\n" \
               "end if;\n\n".format(table, variable, condition, values)

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

        sql += "\nEND $$\nLANGUAGE plpgsql;"
        script_file = open('script.sql', mode='w')
        script_file.write(sql)

st = SqlTransformer()
st.generate_sql()
