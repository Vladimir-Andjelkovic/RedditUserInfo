import sqlite3


class DatabaseManager:
    def __init__(self, db_name):
        self.db_name = db_name
        self.connection = sqlite3.connect(self.db_name)
        self.cursor = self.connection.cursor()

    def create_table(self, table_name, schema):
        """
        Create table if it doesn't already exist.
        :param table_name: Name of the table.
        :param schema: A string defining table schema, e.g., 'id INTEGER PRIMARY KEY, name TEXT, age INTEGER'.
        """
        create_query = f'CREATE TABLE IF NOT EXISTS {table_name} ({schema})'
        self.cursor.execute(create_query)
        self.connection.commit()

    def add_record(self, table_name, columns, values):
        """
        Add a record to the database.
        :param table_name: Name of the table.
        :param columns: A tuple of column names, e.g., ('name', 'age').
        :param values: A tuple of values to insert, e.g., ('John', 30).
        """
        placeholders = ", ".join(["?"] * len(values))
        column_string = ", ".join(columns)
        insert_query = f'INSERT OR IGNORE INTO {table_name} ({column_string}) VALUES ({placeholders})'
        self.cursor.execute(insert_query, values)
        self.connection.commit()

    def add_column_to_table(self, table_name, column_name, column_type):
        """
        Add new column to the table.
        :param table_name:
        :param column_name:
        :param column_type:
        :return:
        """
        alter_query = f'ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}'
        self.cursor.execute(alter_query)
        self.connection.commit()

    def get_usernames(self, table_name):
        query = f'SELECT username FROM {table_name}'
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def add_user_info(self, account_created_utc, comment_karma, verified_email, reddit_user_id, is_employee,
                      is_gold, link_karma, gone_through, username):
        query = (f'UPDATE users SET account_created_utc = ?, comment_karma = ?, verified_email = ?, reddit_user_id = ?,'
                 f'is_employee = ?, is_gold = ?, link_karma = ?, gone_through = ? WHERE username = ?')
        self.cursor.execute(query, (account_created_utc, comment_karma, verified_email, reddit_user_id,
                                    is_employee, is_gold, link_karma, gone_through, username))
        self.connection.commit()

    def manual_query(self):
        query = f"ALTER TABLE users ADD COLUMN gone_through INTEGER DEFAULT 0"
        self.cursor.execute(query)
        self.connection.commit()

    def close(self):
        self.connection.close()
