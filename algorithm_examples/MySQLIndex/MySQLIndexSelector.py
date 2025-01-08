
class MySQLIndexSelector():

    def GenerateSQLsWithHints(self, sql, tables):
        sqls = []
        sqls.append(sql)

        for table in tables:
            hint = f"/*+ NO_INDEX({table}) */"
            sqls.append(sql.replace("SELECT ", f"SELECT {hint} "))
        
        return sqls