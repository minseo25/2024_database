from lark import Lark, Transformer, Tree, Token
from berkeleydb import db
import json, os
from datetime import datetime

# Create a database object
myDB = db.DB()

class Messages():
    """Class that contains the messages to be printed to the user."""
    prompt = "DB_MINSEO25> "

    @staticmethod
    def Prompt():
        print(Messages.prompt, end='')
    @staticmethod
    def SyntaxError():
        print(Messages.prompt + "Syntax error")
    @staticmethod
    def CreateTableSuccess(tablename):
        print(f"{Messages.prompt}'{tablename}' table is created")
    @staticmethod
    def DuplicateColumnDefError():
        print(Messages.prompt + "Create table has failed: column definition is duplicated")
    @staticmethod
    def DuplicatePrimaryKeyDefError():
        print(Messages.prompt + "Create table has failed: primary key definition is duplicated")
    @staticmethod
    def ReferenceTypeError():
        print(Messages.prompt + "Create table has failed: foreign key references wrong type")
    @staticmethod
    def ReferenceNonPrimaryKeyError():
        print(Messages.prompt + "Create table has failed: foreign key references non primary key column")
    @staticmethod
    def ReferenceExistenceError():
        print(Messages.prompt + "Create table has failed: foreign key references non existing table or column")
    @staticmethod
    def PrimaryKeyColumnDefError(colName):
        print(Messages.prompt + f"Create table has failed: cannot define non-existing column '{colName}' as primary key")
    @staticmethod
    def ForeignKeyColumnDefError(colName):
        print(Messages.prompt + f"Create table has failed: cannot define non-existing column '{colName}' as foreign key")
    @staticmethod
    def TableExistenceError():
        print(Messages.prompt + "Create table has failed: table with the same name already exists")
    @staticmethod
    def CharLengthError():
        print(Messages.prompt + "Char length should be over 0")
    @staticmethod
    def DropSuccess(tableName):
        print(f"{Messages.prompt}'{tableName}' table is dropped")
    @staticmethod
    def NoSuchTable(commandName):
        print(Messages.prompt + f"{commandName} has failed: no such table")
    @staticmethod
    def DropReferencedTableError(tableName):
        print(Messages.prompt + f"Drop table has failed: '{tableName}' is referenced by another table")
    @staticmethod
    def InsertResult():
        print(Messages.prompt + "1 row inserted")   
    @staticmethod
    def InsertTypeMismatchError():
        print(Messages.prompt + "Insert has failed: types are not matched")
    @staticmethod
    def InsertColumnExistenceError(colName):
        print(Messages.prompt + f"Insert has failed: '{colName}' does not exist")
    @staticmethod
    def InsertColumnNonNullableError(colName):
        print(Messages.prompt + f"Insert has failed: '{colName}' is not nullable")
    @staticmethod
    def DeleteResult(count):
        print(Messages.prompt + f"{count} row{'s' if count != 1 else ''} deleted")
    @staticmethod
    def SelectTableExistenceError(tableName):
        print(Messages.prompt + f"Select has failed: '{tableName}' does not exist")
    @staticmethod
    def SelectColumnResolveError(colName):
        print(Messages.prompt + f"Select has failed: fail to resolve '{colName}'")
    @staticmethod
    def SelectColumnNotGrouped(colName):
        print(Messages.prompt + f"Select has failed: column '{colName}' must either be included in the GROUP BY clause or be used in an aggregate function")
    @staticmethod
    def TableNotSpecified(clauseName):
        print(Messages.prompt + f"{clauseName.upper()} clause trying to reference tables which are not specified")
    @staticmethod
    def ColumnNotExist(clauseName):
        print(Messages.prompt + f"{clauseName.upper()} clause trying to reference non existing column")
    @staticmethod
    def AmbiguousReference(clauseName):
        print(Messages.prompt + f"{clauseName.upper()} clause contains ambiguous column reference")
    @staticmethod
    def IncomparableError():
        print(Messages.prompt + "Trying to compare incomparable columns or values")

class QueryError(Exception):
    """Custom exception class for query errors."""
    pass

class MyTransformer(Transformer):
    """Class that transforms the parsed SQL query into a database operation."""
    # functions that starts with an underscore are helper functions
    # they access the database and perform the operations
    def _get_table_names(self):
        """Get the names of the tables in the database."""
        table_names = []
        cursor = myDB.cursor()
        while True:
            x = cursor.next()
            if x is None:
                break
            key = x[0].decode()
            if key.startswith('schema:'):
                table_names.append(key.replace('schema:', ''))
        return table_names
    
    def _get_table_metadata(self, table_name):
        """Get the metadata of the table from the database."""
        value = myDB.get(f'schema:{table_name}'.encode())
        if value:
            return json.loads(value.decode())
        return None

    def _get_table_data(self, table_name):
        """Get the data of the table from the database."""
        value = myDB.get(f'data:{table_name}'.encode())
        if value:
            return json.loads(value.decode())
        return None

    def _create_table(self, table_name, column_definition, primary_key, foreign_key):
        """helper function of create_table_query"""
        # check if table with the same name already exists
        table_names = self._get_table_names()
        if table_name in table_names:
            Messages.TableExistenceError()
            raise QueryError
        
        column_list = {}
        for column in column_definition:
            column_list[column['column_name']] = {
                "data_type": column['data_type'],
                "not_null": column['not_null'],
            }
            # check if char length is over 0
            if column['data_type'].startswith('char'):
                if int(column['data_type'].split('(')[1].split(')')[0]) <= 0:
                    Messages.CharLengthError()
                    raise QueryError

        # check if column names are duplicated
        if len(column_list.keys()) != len(column_definition):
            Messages.DuplicateColumnDefError()
            raise QueryError

        # check if primary key is duplicated
        if len(primary_key) > 1:
            Messages.DuplicatePrimaryKeyDefError()
            raise QueryError
        
        # check if primary key column exists
        if len(primary_key) == 1:
            for column in primary_key[0]:
                if column_list.get(column) == None:
                    Messages.PrimaryKeyColumnDefError(column)
                    raise QueryError
        
        for fkey in foreign_key:
            # check if foreign key column exists
            for column in fkey['column_list']:
                if column_list.get(column) == None:
                    Messages.ForeignKeyColumnDefError(column)
                    raise QueryError        

            # check if reference table exists
            ref_table_name = fkey['ref_table_name']
            if ref_table_name not in table_names:
                Messages.ReferenceExistenceError()
                raise QueryError
            
            ref_table_metadata = self._get_table_metadata(ref_table_name)
            ref_table_metadata = ref_table_metadata["columns_metadata"]
            ref_column_list = list(ref_table_metadata.keys())
            ref_primary_key = [key for key, value in ref_table_metadata.items() if value['primary_key'] == True]

            # check if reference column exists
            for column in fkey['ref_column_list']:
                if column not in ref_column_list:
                    Messages.ReferenceExistenceError()
                    raise QueryError
            
            # check if foreign key references primary key
            if sorted(fkey['ref_column_list']) != sorted(ref_primary_key):
                Messages.ReferenceNonPrimaryKeyError()
                raise QueryError
            
            # check if foreign key references wrong type
            for col_curr, col_ref in zip(fkey['column_list'], fkey['ref_column_list']):
                if ref_table_metadata[col_ref]['type'] != column_list[col_curr]['data_type']:
                    Messages.ReferenceTypeError()
                    raise QueryError

        # no error, add table relation to the db
        for fkey in foreign_key:
            data = {}
            for col_curr, col_ref in zip(fkey['column_list'], fkey['ref_column_list']):
                data[col_curr] = col_ref
            myDB.put(f'reference:{table_name}:{fkey["ref_table_name"]}'.encode(), json.dumps(data).encode())
        
        # create table metadata
        # column의 순서도 저장할 필요가 있음 (insert 시 순서대로 저장되어야 함)
        schema_metadata = {"columns": []}
        metadata = {}
        for column_name, column_info in column_list.items():
            schema_metadata["columns"].append(column_name)
            metadata[column_name] = {
                "type": column_info['data_type'],
                "not_null": column_info['not_null'],
                "primary_key": False,
                "foreign_key": False,
            }
        if len(primary_key) == 1:
            for column in primary_key[0]:
                metadata[column]['primary_key'] = True
                metadata[column]['not_null'] = True
        for fkey in foreign_key:
            for column in fkey['column_list']:
                metadata[column]['foreign_key'] = True
        schema_metadata["columns_metadata"] = metadata
    
        # put the metadata into the database
        myDB.put(f'schema:{table_name}'.encode(), json.dumps(schema_metadata).encode())
        # put the empty data into the database
        myDB.put(f'data:{table_name}'.encode(), json.dumps([]).encode())

        Messages.CreateTableSuccess(table_name)

    def _referenced_by_another_table(self, table_name):
        """Check if the table is referenced by any other table."""
        cursor = myDB.cursor()
        while True:
            x = cursor.next()
            if x is None:
                break
            key = x[0].decode()
            if key.startswith('reference:'):
                if key.split(':')[-1] == table_name:
                    return True
        return False

    def _delete_table_relation(self, table_name):
        """Delete the table relation from the database."""
        cursor = myDB.cursor()
        while True:
            x = cursor.next()
            if x is None:
                break
            key = x[0].decode()
            if key.startswith(f'reference:{table_name}:'):
                myDB.delete(x[0])

    def _drop_table(self, table_name):
        """helper function of drop_table_query"""
        # check if table exists
        if self._get_table_metadata(table_name) == None:
            Messages.NoSuchTable("Drop table")
            raise QueryError

        # check if the table is referenced by another table
        if self._referenced_by_another_table(table_name):
            Messages.DropReferencedTableError(table_name)
            raise QueryError

        # delete the table relation from the db
        self._delete_table_relation(table_name)
        # delete the table metadata and data from the db
        myDB.delete(f'schema:{table_name}'.encode())
        myDB.delete(f'data:{table_name}'.encode())

        # print the success message
        Messages.DropSuccess(table_name)

    def _explain_table(self, query, table_name):
        """helper function of explain_query, describe_query, desc_query"""
        # check if table exists
        table_metadata = self._get_table_metadata(table_name)
        if table_metadata == None:
            Messages.NoSuchTable(query)
            raise QueryError

        # print the table metadata
        print('-'*64)
        formatted_string = "{:<20}| {:<11}| {:<11}| {:<17}".format("column_name", "type", "null", "key")
        print(formatted_string)

        table_metadata = table_metadata["columns_metadata"]
        for column_name, column_info in table_metadata.items():
            key = "PRI" if column_info['primary_key'] else ""
            if column_info['foreign_key']:
                key += '/' if key != "" else ""
                key += "FOR"

            formatted_string = "{:<20}| {:<11}| {:<11}| {:<17}".format(
                column_name,
                column_info['type'],
                "N" if column_info['not_null'] else "Y",
                key,
            )
            print(formatted_string)
        
        print('-'*64)
        # print the number of rows in the table (singular/plural distinction)
        print(f'{len(table_metadata)} row{"s" if len(table_metadata) != 1 else ""} in set')
        
    def _show_tables(self):
        """helper function of show_tables_query"""
        # get the table names and print them
        print('-'*24)
        table_names = self._get_table_names()
        for table_name in table_names:
            print(table_name)
        print('-'*24)
        # print the number of rows in the table (singular/plural distinction)
        print(f'{len(table_names)} row{"s" if len(table_names) != 1 else ""} in set')

    def _select_query(self, select_column_list, select_table_list, select_join_table_list, select_condition_list, select_order_by_list):
        """helper function of select_query"""
        # get all tables' data and metadata (in select_table_list and select_join_table_list)
        tables_info = {}
        for table in select_table_list:
            table_name = table['table_name']
            alias = table['table_alias'] or table_name # 없으면 
            metadata = self._get_table_metadata(table_name)
            # check if table exists
            if metadata is None:
                Messages.SelectTableExistenceError(table_name)
                raise QueryError
            
            tables_info[alias] = {
                'original_name': table_name,
                'data': self._get_table_data(table_name),
                'metadata': metadata,
            }
        for join in select_join_table_list:
            # join 테이블의 alias는 고려하지 않음
            table_name = join['join_table']
            metadata = self._get_table_metadata(table_name)
            # check if table exists
            if metadata is None:
                Messages.SelectTableExistenceError(table_name)
                raise QueryError

            tables_info[table_name] = {
                'original_name': table_name,
                'data': self._get_table_data(table_name),
                'metadata': metadata,
            }

        def resolve_column_reference(column_name, table_name, context):
            """Resolve the column reference (when table name is given or not given)"""
            if table_name:
                # when table name is given and not specified
                if table_name not in tables_info:
                    Messages.TableNotSpecified(context)
                    raise QueryError
                # when table name is given but column name is not in the table
                if column_name not in tables_info[table_name]['metadata']['columns']:
                    if context == "SELECT": # select 대상이 되는 column이 없을 때
                        Messages.SelectColumnResolveError(column_name)
                    else: # where 절, join 조건, order by 절에서 존재하지 않는 column을 참조할 때
                        Messages.ColumnNotExist(context)
                    raise QueryError
                
                return table_name

            # when column name is not given
            found_tables = []
            for alias, info in tables_info.items():
                if column_name in info['metadata']['columns']:
                    found_tables.append(alias)
            
            if len(found_tables) > 1:
                if context == "SELECT":
                    # select 대상이 되는 컬럼이 모호할 때
                    Messages.SelectColumnResolveError(column_name)
                else:
                    # where 조건, order by 정렬 기준에서 모호할 때
                    Messages.AmbiguousReference(context)
                raise QueryError
            
            if len(found_tables) == 0:
                if context == "SELECT":
                    # select 대상이 되는 컬럼이 없을 때
                    Messages.SelectColumnResolveError(column_name)
                else:
                    # order by 절, where 조건에서 존재하지 않는 컬럼을 참조할 때
                    Messages.ColumnNotExist(context)
                raise QueryError
            
            return found_tables[0]
        
        def check_join_conditions():
            """verify the join conditions"""
            parsed_conditions = []

            for join in select_join_table_list:
                table1 = join['table1']
                table2 = join['table2']
                column1 = join['table1_column']
                column2 = join['table2_column']

                # check if both tables exist
                if table1 not in tables_info or table2 not in tables_info:
                    Messages.TableNotSpecified("JOIN")
                    raise QueryError
                
                # check if joining columns exist
                table1_metadata = tables_info[table1]['metadata']
                table2_metadata = tables_info[table2]['metadata']
                if column1 not in table1_metadata['columns'] or column2 not in table2_metadata['columns']:
                    Messages.ColumnNotExist("JOIN")
                    raise QueryError
                
                # check if joining columns have the same type
                type1 = table1_metadata["columns_metadata"][column1]['type']
                type2 = table2_metadata["columns_metadata"][column2]['type']

                if type1 != type2 and not (type1.startswith('char') and type2.startswith('char')):
                    Messages.IncomparableError()
                    raise QueryError
                
                parsed_conditions.append({
                    'table1': table1,
                    'table2': table2,
                    'column1': column1,
                    'column2': column2,
                })
            
            return parsed_conditions
        
        def apply_joins(records):
            """apply the joins to the records"""
            # check the join conditions and return the parsed conditions
            parsed_conditions = check_join_conditions()

            for condition in parsed_conditions:
                records = [r for r in records if r[f"{condition['table1']}.{condition['column1']}"] == r[f"{condition['table2']}.{condition['column2']}"]]
            
            return records
        
        def generate_cartesian_product():
            """cartesian product of tables in from clause"""
            result = [{}]
            for alias, info in tables_info.items():
                new_result = []
                for record in result:
                    # for each record in the result, add a new record for each record in the table
                    for table_record in info['data']:
                        new_record = record.copy()
                        for col, val in table_record.items():
                            new_record[f"{alias}.{col}"] = val
                        new_result.append(new_record)
                result = new_result

            return result

        def validate_condition(condition):
            """validate the condition"""
            def get_type(operand):
                if operand.get("type") == "column_name":
                    table = resolve_column_reference(operand["column_name"], operand.get("table_name", ""), "WHERE")
                    type_ = tables_info[table]['metadata']['columns_metadata'][operand['column_name']]['type']
                else: # comparable_value
                    type_ = operand["value_type"]

                if type_ == "str" or type_.startswith("char"):
                    type_ = "char"
                
                return type_
            
            if condition["type"] == "null predicate":
                # if null predicate, check if the column is null
                resolve_column_reference(
                    condition["column_name"],
                    condition.get("table_name", ""),
                    "WHERE",
                )
                return
            
            # if comparison type, check if the condition is met
            operand1 = condition["comp_operand_1"]
            operand2 = condition["comp_operand_2"]

            type1 = get_type(operand1)
            type2 = get_type(operand2)

            # type check
            if type1 != type2:
                Messages.IncomparableError()
                raise QueryError
            
            # char type cannot be compared with other operators
            if type1 == type2 == "char" and condition["comp_op"] not in ["=", "!="]:
                Messages.IncomparableError()
                raise QueryError

        def check_condition(record, condition):
            """check if the record matches the condition in where clause"""
            def get_value(operand):
                if operand.get("type") == "column_name":
                    table = resolve_column_reference(operand["column_name"], operand.get("table_name", ""), "WHERE")
                    value = record[f"{table}.{operand['column_name']}"]
                    type_ = tables_info[table]['metadata']['columns_metadata'][operand['column_name']]['type']
                else: # comparable_value
                    value = operand["value"]
                    type_ = operand["value_type"]
                    if type_ == "int":
                        value = int(value)
                    elif type_ == "str":
                        value = value[1:][:-1]

                if type_ == "date" and value is not None:
                    value = datetime.strptime(value, "%Y-%m-%d")
                
                return value
            
            if condition["type"] == "null predicate":
                # if null predicate, check if the column is null
                table = resolve_column_reference(
                    condition["column_name"],
                    condition.get("table_name", ""),
                    "WHERE",
                )
                value = record[f"{table}.{condition['column_name']}"]
                is_null = value is None
                result = (is_null == condition["is_null"])
                return not result if condition["not"] else result
            
            # if comparison type, check if the condition is met
            operand1 = condition["comp_operand_1"]
            operand2 = condition["comp_operand_2"]

            value1 = get_value(operand1)
            value2 = get_value(operand2)

            # cannot compare null value
            if value1 is None or value2 is None:
                Messages.IncomparableError()
                raise QueryError
            
            # comparison operation
            comp_ops = {
                "=": lambda x, y: x == y,
                "!=": lambda x, y: x != y,
                ">": lambda x, y: x > y,
                ">=": lambda x, y: x >= y,
                "<": lambda x, y: x < y,
                "<=": lambda x, y: x <= y,
            }
            result = comp_ops[condition["comp_op"]](value1, value2)
            return not result if condition["not"] else result

        def apply_conditions(records):
            """apply where conditions to the records"""
            if not select_condition_list:
                return records
            
            # 조건 먼저 검증 (records가 비어있어도 조건 검증 반드시 이루어짐)
            if select_condition_list[0] == "SINGLE":
                validate_condition(select_condition_list[1])
            else: # AND / OR
                validate_condition(select_condition_list[1])
                validate_condition(select_condition_list[2])

            filtered_records = []
            for record in records:
                if select_condition_list[0] == "SINGLE":
                    if check_condition(record, select_condition_list[1]):
                        filtered_records.append(record)
                else: # AND / OR
                    result1 = check_condition(record, select_condition_list[1])
                    result2 = check_condition(record, select_condition_list[2])
                    if select_condition_list[0] == "AND" and (result1 and result2):
                        filtered_records.append(record)
                    elif select_condition_list[0] == "OR" and (result1 or result2):
                        filtered_records.append(record)
            
            return filtered_records
        
        def sort_orders(records):
            """sort the records by the order by clause"""
            if not select_order_by_list:
                return records

            for order in reversed(select_order_by_list):
                column_name = order['column_name']
                table_name = resolve_column_reference(column_name, order['table_name'], "ORDER BY")
                reverse = order['direction'].lower() == 'desc'

                column_type = tables_info[table_name]['metadata']['columns_metadata'][column_name]['type']

                def get_sort_key(record):
                    value = record[f"{table_name}.{column_name}"]
                    # When doing an ORDER BY, NULL values are presented first if you do ORDER BY ... ASC and last if you do ORDER BY ... DESC.
                    # smallest 한 값으로 처리
                    if value is None:
                        return (0, None)

                    # type별로 다르게 처리
                    if column_type == 'int':
                        return (1, int(value))
                    elif column_type == 'char':
                        return (1, value)
                    elif column_type == 'date':
                        return (1, datetime.strptime(value, "%Y-%m-%d"))
                    else:
                        return (1, value)
                
                records.sort(key=get_sort_key, reverse=reverse)
                
            return records

        def format_output(records):
            """format and print the query result"""            
            # select * 인 경우 모든 columns 선택
            if not select_column_list:
                all_columns = []
                for alias, info in tables_info.items():
                    for column in info['metadata']['columns']:
                        all_columns.append({
                            'type': 'column',
                            'table_name': alias,
                            'column_name': column,
                            'alias': '',
                        })
                columns_to_process = all_columns
            else:
                columns_to_process = select_column_list
            
            has_aggregate = any(col['type'] in ['aggregate', 'total_count'] for col in columns_to_process)
            has_normal = any(col['type'] == 'column' for col in columns_to_process)

            if has_aggregate and has_normal:
                # group by가 없으므로 aggregate/total_count 타입은 column 이랑 같이 쓰이면 안됨
                Messages.SelectColumnNotGrouped()
                raise QueryError
            
            if has_aggregate:
                headers = []
                values = []

                for col in columns_to_process:
                    if col['type'] == 'total_count':
                        headers.append('count(*)' if col['alias'] == '' else col['alias'])
                        values.append(str(len(records)))
                    else: # aggregate
                        func = col['aggregate_func']
                        col_name = col['column_name']
                        table = col['table_name'] or resolve_column_reference(col_name, None, "SELECT")
                        header = col['alias'] or f"{func}({table}.{col_name})"
                        headers.append(header)

                        # 값들을 뽑아옵니다
                        column_type = tables_info[table]['metadata']['columns_metadata'][col_name]['type']
                        column_values = [r[f"{table}.{col_name}"] for r in records]

                        if func == 'count':
                            values.append(str(len(column_values))) # NULL 포함
                        else:
                            non_null_values = [v for v in column_values if v is not None]
                            if not non_null_values:
                                if func == 'sum':
                                    values.append('0')
                                else:
                                    values.append('NULL')
                                continue
                                
                            if func == 'sum':
                                if column_type != 'int':
                                    values.append('0')
                                else:
                                    values.append(str(sum(non_null_values)))
                            elif func in ['max', 'min']:
                                if column_type == 'date':
                                    processed_values = [datetime.strptime(v, "%Y-%m-%d") for v in non_null_values]
                                else:
                                    processed_values = non_null_values
                                
                                result_value = str(max(processed_values) if func == 'max' else min(processed_values))
                                if column_type == 'date':
                                    result_value = result_value.strftime("%Y-%m-%d")
                                
                                values.append(result_value)

                column_widths = [max(len(h), len(str(v))) for h, v in zip(headers, values)]
                width = sum(w + 3 for w in column_widths) + 1
                
                print("-" * width)
                print(" " + " | ".join(f"{h:<{w}}" for h, w in zip(headers, column_widths)) + " ")
                print(" " + " | ".join(f"{v:<{w}}" for v, w in zip(values, column_widths)) + " ")
                print("-" * width)
                print("1 row in set")
            else:
                # 일반적인 경우 (column 타입만 있는 경우)
                headers = []
                for col in columns_to_process:
                    table = col['table_name'] or resolve_column_reference(col['column_name'], None, "SELECT")
                    header = col['alias'] or f"{table}.{col['column_name']}"
                    headers.append({
                        'display': header,
                        'key': f"{table}.{col['column_name']}"
                    })
                
                if not records:
                    width = sum(len(h['display']) + 3 for h in headers) + 1
                    print("-" * width)
                    print("-" * width)
                    print("0 rows in set")
                    return

                # 각 컬럼의 최대 너비 계산 (헤더 길이와 데이터 길이 중 큰 값)
                column_widths = []
                for h in headers:
                    max_data_width = max(
                        len(str(record.get(h['key'], 'NULL') if record.get(h['key']) is not None else 'NULL'))
                        for record in records
                    )
                    column_widths.append(max(len(h['display']), max_data_width))

                width = sum(w + 3 for w in column_widths) + 1

                print("-" * width)
                print(" " + " | ".join(f"{h['display']:<{w}}" for h, w in zip(headers, column_widths)) + " ")

                for record in records:
                    values = []
                    for h, w in zip(headers, column_widths):
                        value = record[h['key']]
                        value_str = "null" if value is None else str(value)
                        values.append(f"{value_str:<{w}}")
                    print(" " + " | ".join(values) + " ")
                
                print("-" * width)
                print(f"{len(records)} row{'' if len(records) == 1 else 's'} in set")

        # main logic (from/join의 테이블 cartesian product -> apply joins -> apply where conditions -> sort -> format output)
        records = generate_cartesian_product()
        records = apply_joins(records)
        records = apply_conditions(records)
        records = sort_orders(records)
        format_output(records)

    def _insert_query(self, table_name, column_list, value_list):
        """helper function of insert_query"""
        # check if table exists
        table_metadata = self._get_table_metadata(table_name)
        if table_metadata == None:
            Messages.NoSuchTable("Insert")
            raise QueryError

        columns_metadata = table_metadata["columns_metadata"]
        ordered_column_list = table_metadata["columns"]
        ordered_value_list = []
        if len(column_list) == 0:
            # if column_list is empty, value is in column order
            if len(value_list) != len(ordered_column_list):
                Messages.InsertTypeMismatchError()
                raise QueryError

            ordered_value_list = value_list
        else:
            # check if column_list and value_list have the same length
            if len(column_list) != len(value_list):
                Messages.InsertTypeMismatchError()
                raise QueryError
            
            # check if all columns in column_list exist in the table
            for column in column_list:
                if column not in ordered_column_list:
                    Messages.InsertColumnExistenceError(column)
                    raise QueryError

            # if column_list is not empty, order value_list by column_list
            for column in ordered_column_list:
                if column in column_list:
                    ordered_value_list.append(value_list[column_list.index(column)])
                else:
                    ordered_value_list.append({'value': 'null', 'value_type': 'null'})

        new_record = {}
        # check if all values are in correct type and insert them into the new record
        for column_name, value in zip(ordered_column_list, ordered_value_list):
            if value['value_type'] == 'int':
                if columns_metadata[column_name]['type'] != 'int':
                    Messages.InsertTypeMismatchError()
                    raise QueryError
                new_record[column_name] = int(value['value'])
            elif value['value_type'] == 'str':
                if not columns_metadata[column_name]['type'].startswith('char'):
                    Messages.InsertTypeMismatchError()
                    raise QueryError
                # if char and value is longer than the length, truncate the value
                max_length = int(columns_metadata[column_name]['type'].split('(')[1].split(')')[0])
                value = value['value'][1:][:-1]
                new_record[column_name] = value[:max_length]
            elif value['value_type'] == 'date':
                if columns_metadata[column_name]['type'] != 'date':
                    Messages.InsertTypeMismatchError()
                    raise QueryError
                # do nothing for date type
                new_record[column_name] = value['value']
            else: # value['value_type'] == 'null'
                # null is compatible with all types, but the column must be nullable
                if columns_metadata[column_name]['not_null']:
                    Messages.InsertColumnNonNullableError(column_name)
                    raise QueryError
                new_record[column_name] = None
        # insert the new record into the table data
        table_data = self._get_table_data(table_name)
        table_data.append(new_record)
        myDB.put(f'data:{table_name}'.encode(), json.dumps(table_data).encode())

        Messages.InsertResult()

    def _delete_query_check_condition(self, columns_metadata, record, condition):
        """Check if the record matches the condition."""           
        def get_value(operand):
            # get the value and type of the operand (and preprocess the value)
            if operand.get("type") == "column_name":
                value = record[operand["column_name"]]
                type_ = columns_metadata[operand["column_name"]]["type"]
            else: # "comparable_value"
                value = operand["value"]
                type_ = operand["value_type"]
                if type_ == "int":
                    value = int(value)
                elif type_ == "str":
                    value = value[1:][:-1]
            
            # preprocess the value for comparison
            if type_ == "date" and value is not None:
                value = datetime.strptime(value, "%Y-%m-%d")
            
            return value

        # no issue of ambiguous column name
        if condition["type"] == 'null predicate':
            value = record[condition["column_name"]]
            is_null = value is None
            result = (is_null == condition["is_null"])
            # apply not operator
            return not result if condition["not"] else result
        
        operand1 = condition["comp_operand_1"]
        operand2 = condition["comp_operand_2"]
        
        value1 = get_value(operand1)
        value2 = get_value(operand2)
        
        # cannot compare null value
        if value1 is None or value2 is None:
            Messages.IncomparableError()
            raise QueryError
        
        # values are either integer, string, or date
        comp_ops = {
            "=": lambda x, y: x == y,
            "!=": lambda x, y: x != y,
            ">": lambda x, y: x > y,
            ">=": lambda x, y: x >= y,
            "<": lambda x, y: x < y,
            "<=": lambda x, y: x <= y,
        }
        result = comp_ops[condition["comp_op"]](value1, value2)
        # apply not operator
        return not result if condition["not"] else result
    
    def _delete_query_validate_condition(self, table_name, columns_metadata, condition):
        """Validate the condition without checking actual records."""
        def check_table_name(table):
            # check if table name is specified and matches the table name
            if table and table != table_name:
                Messages.TableNotSpecified("WHERE")
                raise QueryError
        
        def check_column_exists(column):
            if column not in columns_metadata:
                Messages.ColumnNotExist("WHERE")
                raise QueryError
            
        def get_type(operand):
            if operand.get("type") == "column_name":
                check_column_exists(operand["column_name"])
                type_ = columns_metadata[operand["column_name"]]["type"]
            else: # "comparable_value"
                type_ = operand["value_type"]
            
            if type_ == "str" or type_.startswith("char"):
                type_ = "char"
            return type_

        # null predicate 검증
        if condition["type"] == 'null predicate':
            check_table_name(condition["table_name"])
            check_column_exists(condition["column_name"])
            return
        
        operand1 = condition["comp_operand_1"]
        operand2 = condition["comp_operand_2"]

        if operand1.get("table_name"):
            check_table_name(operand1["table_name"])
        if operand2.get("table_name"):
            check_table_name(operand2["table_name"])
        
        type1 = get_type(operand1)
        type2 = get_type(operand2)

        # 타입 일치 검증
        if type1 != type2:
            Messages.IncomparableError()
            raise QueryError
        
        # char 타입의 비교 연산자 제약 검증
        if type1 == type2 == "char" and condition["comp_op"] not in ["=", "!="]:
            Messages.IncomparableError()
            raise QueryError

    def _delete_query(self, table_name, condition_list):
        """helper function of delete_query"""
        # check if table exists
        table_metadata = self._get_table_metadata(table_name)
        if table_metadata == None:
            Messages.NoSuchTable("Delete")
            raise QueryError
        
        table_data = self._get_table_data(table_name)
        # if condition_list is empty, delete all rows
        if len(condition_list) == 0:
            num_deleted_rows = len(table_data)
            myDB.put(f'data:{table_name}'.encode(), json.dumps([]).encode())
            Messages.DeleteResult(num_deleted_rows)
            return
        
        delete_index_list = []
        if condition_list[0] == "SINGLE":
            condition = condition_list[1]
            
            # 조건 검증 먼저 (table이 비어있어도 조건 검증 반드시 이루어짐)
            self._delete_query_validate_condition(table_name, table_metadata["columns_metadata"], condition)

            # find the index of the record that matches the condition
            for i, record in enumerate(table_data):
                if self._delete_query_check_condition(table_metadata["columns_metadata"], record, condition):
                    delete_index_list.append(i)
            
            # delete the records
            for index in sorted(delete_index_list, reverse=True):
                del table_data[index]
            myDB.put(f'data:{table_name}'.encode(), json.dumps(table_data).encode())
            Messages.DeleteResult(len(delete_index_list))
            return
        else: # condition_list[0] == "AND" or condition_list[0] == "OR"
            condition1 = condition_list[1]
            condition2 = condition_list[2]
            delete_index_list_1 = []
            delete_index_list_2 = []

            # 조건 검증 먼저 (table이 비어있어도 조건 검증 반드시 이루어짐)
            self._delete_query_validate_condition(table_name, table_metadata["columns_metadata"], condition1)
            self._delete_query_validate_condition(table_name, table_metadata["columns_metadata"], condition2)

            # find the index of the records that match the conditions
            for i, record in enumerate(table_data):
                if self._delete_query_check_condition(table_metadata["columns_metadata"], record, condition1):
                    delete_index_list_1.append(i)
                if self._delete_query_check_condition(table_metadata["columns_metadata"], record, condition2):
                    delete_index_list_2.append(i)
            
            if condition_list[0] == "AND":
                delete_index_list = list(set(delete_index_list_1) & set(delete_index_list_2))
            elif condition_list[0] == "OR":
                delete_index_list = list(set(delete_index_list_1) | set(delete_index_list_2))
        
        # delete the records
        for index in sorted(delete_index_list, reverse=True):
            del table_data[index]
        myDB.put(f'data:{table_name}'.encode(), json.dumps(table_data).encode())
        Messages.DeleteResult(len(delete_index_list))
                    
    # *_query functions handle the SQL queries
    def create_table_query(self, items):
        """Handle the create table query."""
        table_name = items[2].children[0].lower()
        column_definition = []
        primary_key = []
        foreign_key = []

        # get the column definition, primary key, and foreign key
        for column in list(items[3].find_data('column_definition')):
            column_name = column.children[0].children[0].lower()
            data_type = column.children[1].children[0].lower()
            if data_type == 'char':
                data_type += f"({column.children[1].children[2]})"
            is_not_null = True if (column.children[2] != None) else False

            column_definition.append({
                "column_name": column_name,
                "data_type": data_type,
                "not_null": is_not_null
            })
        
        for pkey in list(items[3].find_data('primary_key_constraint')):
            column_list = []
            for column in list(pkey.children[2].find_data('column_name')):
                column_list.append(column.children[0].lower())
            primary_key.append(column_list)
        
        for fkey in list(items[3].find_data('referential_constraint')):
            column_list = []
            for column in list(fkey.children[2].find_data('column_name')):
                column_list.append(column.children[0].lower())
            ref_table_name = fkey.children[4].children[0].lower()
            ref_column_list = []
            for column in list(fkey.children[5].find_data('column_name')):
                ref_column_list.append(column.children[0].lower())
            foreign_key.append({
                "column_list": column_list,
                "ref_table_name": ref_table_name,
                "ref_column_list": ref_column_list
            })

        self._create_table(table_name, column_definition, primary_key, foreign_key)

    def drop_table_query(self, items):
        """Handle the drop table query."""
        table_name = items[2].children[0].lower()
        self._drop_table(table_name)

    def explain_query(self, items):
        """Handle the explain query."""
        table_name = items[1].children[0].lower()
        self._explain_table("Explain", table_name)

    def describe_query(self, items):
        """Handle the describe query."""
        table_name = items[1].children[0].lower()
        self._explain_table("Describe", table_name)

    def desc_query(self, items):
        """Handle the desc query."""
        table_name = items[1].children[0].lower()
        self._explain_table("Desc", table_name)

    def show_tables_query(self, items):
        """Handle the show tables query."""
        self._show_tables()

    def insert_query(self, items):
        """Handle the insert query."""
        table_name = items[2].children[0].lower()
        column_list = []
        value_list = []

        if items[3]: # if column_name_list is not empty
            for column in list(items[3].find_data('column_name')):
                column_list.append(column.children[0].lower())
        for value in list(items[4].find_data('insert_value')):
            # value_type is case sensitive (because it is a value)
            value_list.append({"value": value.children[0].value, "value_type": value.children[0].type.lower()})

        self._insert_query(table_name, column_list, value_list)

    def _parse_comp_operand(self, operand):
        """Parse the comparison operand and return a dictionary."""
        comp_operand = {}
        if len(operand.children) == 1:
            # comparable_value
            comp_operand["type"] = "comparable_value"
            comp_operand["value_type"] = operand.children[0].children[0].type.lower()
            comp_operand["value"] = operand.children[0].children[0].value
        else:
            # column_name
            comp_operand["type"] = "column_name"
            if operand.children[0]:
                comp_operand["table_name"] = operand.children[0].children[0].lower()
            comp_operand["column_name"] = operand.children[1].children[0].lower()
        
        return comp_operand

    def _parse_boolean_factor(self, bool_factor):
        """Parse the boolean factor and return a list of conditions."""
        not_operator = bool_factor.children[0]
        bool_test = bool_factor.children[1]
        condition = {}

        if bool_test.children[0].data == "predicate":
            predicate = bool_test.children[0]
            if predicate.children[0].data == "comparison_predicate":
                comparison_predicate = predicate.children[0]

                # get the comparison operator
                comp_op = comparison_predicate.children[1].value
                
                # get the comparison operand 1
                operand1 = comparison_predicate.children[0]
                comp_operand_1 = self._parse_comp_operand(operand1)
                
                # get the comparison operand 2
                operand2 = comparison_predicate.children[2]
                comp_operand_2 = self._parse_comp_operand(operand2)

                condition = {
                    "not": False,
                    "type": "comparison",
                    "comp_operand_1": comp_operand_1,
                    "comp_op": comp_op,
                    "comp_operand_2": comp_operand_2
                }
            elif predicate.children[0].data == "null_predicate":
                null_predicate = predicate.children[0]
                table_name = ""
                if null_predicate.children[0]:
                    table_name = null_predicate.children[0].children[0].lower()
                column_name = null_predicate.children[-2].children[0].lower()
                is_null = null_predicate.children[-1].children[1] is None

                condition = {
                    "not": False,
                    "type": "null predicate",
                    "table_name": table_name,
                    "column_name": column_name,
                    "is_null": is_null
                }
        elif bool_test.children[0].data == "parenthesized_boolean_expr":
            # simple condition이므로 괄호 안의 boolean_expr는 1개의 boolean_term, 1개의 boolean_factor로 구성
            # flatten the parenthesized boolean expression (재귀적으로)
            parenthesized_bool_expr = bool_test.children[0]
            bool_expr = parenthesized_bool_expr.children[1]
            bool_term = [child for child in bool_expr.children if self._get_rule_name(child) == "boolean_term"][0]
            bool_factor = [child for child in bool_term.children if self._get_rule_name(child) == "boolean_factor"][0]
            condition = self._parse_boolean_factor(bool_factor)
        
        if not_operator:
            condition["not"] = not condition["not"]

        return condition

    def _get_rule_name(self, node):
        if isinstance(node, Tree):
            return node.data
        elif isinstance(node, Token):
            return node.type
        else:
            return None

    def _parse_where_clause(self, where_clause):
        """Parse the where clause and return a list of conditions."""
        condition_list = []
        bool_expr = where_clause.children[1]
        # find_data 안 쓰는 이유: () 안에 있는 bool_expr의 boolean_term은 배제하기 위해, direct children만 탐색
        bool_terms = [child for child in bool_expr.children if self._get_rule_name(child) == "boolean_term"]

        if len(bool_terms) == 1:
            # AND condition (1 boolean_term with 2 boolean_factors) or SINGLE condition (1 boolean_term with 1 boolean_factor)
            factors = [child for child in bool_terms[0].children if self._get_rule_name(child) == "boolean_factor"]
            if len(factors) > 1:
                condition_list = ["AND",
                    self._parse_boolean_factor(factors[0]),
                    self._parse_boolean_factor(factors[1])
                ]
            else:
                condition_list = ["SINGLE", self._parse_boolean_factor(factors[0])]
        elif len(bool_terms) == 2:
            # OR condition (2 boolean_terms with 1 boolean_factor each)
            factors1 = [child for child in bool_terms[0].children if self._get_rule_name(child) == "boolean_factor"]
            factors2 = [child for child in bool_terms[1].children if self._get_rule_name(child) == "boolean_factor"]
            condition_list = ["OR",
                self._parse_boolean_factor(factors1[0]),
                self._parse_boolean_factor(factors2[0])
            ]

        return condition_list

    def delete_query(self, items):
        """Handle the delete query."""
        table_name = items[2].children[0].lower()
        condition_list = []
        where_clause = items[3]

        if where_clause:
            condition_list = self._parse_where_clause(where_clause)

        self._delete_query(table_name, condition_list)

    def select_query(self, items):
        """Handle the select query."""
        select_column_list = []
        select_table_list = []
        select_join_table_list = []
        select_condition_list = []
        select_order_by_list = []

        # parse select_list first
        select_list = items[1]
        # empty select_column_list if "select_all_columns"
        if select_list.data != 'select_all_columns':
            for column in select_list.find_data('selected_column'):
                selected_column = column.children[0]
                data = {}
                if selected_column.data == 'column_ref':
                    # column_ref : [table_name] "." column_name
                    data = {
                        "type": "column",
                        "table_name": "" if selected_column.children[0] is None else selected_column.children[0].children[0].value,
                        "column_name": selected_column.children[1].children[0].value
                    }
                elif selected_column.data == 'aggregate_func':
                    # aggregate_func : (COUNT | SUM | MAX | MIN | AVG) LP column_ref RP
                    data = {
                        "type": "aggregate",
                        "aggregate_func": selected_column.children[0].value.lower(),
                        "table_name": "" if selected_column.children[2].children[0] is None else selected_column.children[2].children[0].children[0].value,
                        "column_name": selected_column.children[2].children[1].children[0].value
                    }
                elif selected_column.data == 'total_count':
                    # total_count : COUNT LP "*" RP
                    data = {
                        "type": "total_count"
                    }
                if data:
                    # add alias if exists
                    data["alias"] = "" if column.children[2] is None else column.children[2].children[0].value
                    select_column_list.append(data)
        
        from_clause = items[2].children[0]
        table_reference_list = from_clause.children[1]
        join_clause_list = from_clause.children[2:]

        # parse table_reference_list
        for referred_table in table_reference_list.find_data('referred_table'):
            table_name = referred_table.children[0].children[0].value
            table_alias = ""
            if referred_table.children[2]:
                table_alias = referred_table.children[2].children[0].value
            select_table_list.append({
                "table_name": table_name,
                "table_alias": table_alias
            })
        
        # parse join_clause_list
        for join_clause in join_clause_list:
            referred_table = join_clause.children[1]
            join_condition = join_clause.children[3]
            select_join_table_list.append({
                "join_table": referred_table.children[0].children[0].value,
                "table1": join_condition.children[0].children[0].children[0].value,
                "table1_column": join_condition.children[0].children[1].children[0].value,
                "table2": join_condition.children[2].children[0].children[0].value,
                "table2_column": join_condition.children[2].children[1].children[0].value
            })

        # parse where_clause
        where_clause = items[2].children[1]
        if where_clause:
            select_condition_list = self._parse_where_clause(where_clause)

        # parse order_by_clause
        order_by_clause = items[2].children[2]
        if order_by_clause:
            order_list = order_by_clause.children[2]
            for order_item in order_list.find_data('order_item'):
                column_ref = order_item.children[0]
                select_order_by_list.append({
                    "table_name": "" if column_ref.children[0] is None else column_ref.children[0].children[0].value,
                    "column_name": column_ref.children[1].children[0].value,
                    "direction": "asc" if order_item.children[1] is None else order_item.children[1].value.lower()
                })
        
        # pass select_list, join_table_list (with join condition), condition_list, order_by info
        self._select_query(select_column_list, select_table_list, select_join_table_list, select_condition_list, select_order_by_list)

    def update_query(self, items):
        pass

    def EXIT(self, items):
        # Close the database
        myDB.close()
        # Exit the program
        exit()

def get_input() -> str:
    """Get the input from the user. The input can be multiple lines."""
    Messages.Prompt()
    input_string = ""
    while True:
        line = input()
        input_string += line + '\n'
        # if the line contains a semicolon, then it is the end of the query
        if ';' in line:
            break
    return input_string.strip()

def main() -> None:
    with open('grammar.lark') as file:
        sql_parser = Lark(file.read(), start="command", lexer="basic")

    transformer = MyTransformer()
    if os.path.exists("myDB.db"):
        myDB.open("myDB.db", dbtype=db.DB_HASH)
    else:
        myDB.open("myDB.db", dbtype=db.DB_HASH, flags=db.DB_CREATE)

    while True:        
        commands = []
        user_inputs = get_input().split(';')
        # split the input into multiple commands and save them in a list
        for user_input in user_inputs[:-1]:
            # add the semicolon back to the command
            commands.append(user_input.strip()+';')

        for command in commands:
            try:
                # Parse the input
                output = sql_parser.parse(command)
            except Exception as e:
                # print the error message and continue to the next command
                # syntax error, use reserved keyword, incorrect data format, etc.
                Messages.SyntaxError()
                break
            
            try:
                # Transform the parsed output into a database operation
                transformer.transform(output)
            except Exception as e:
                break        

if __name__ == '__main__':
    main()