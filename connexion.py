import mysql.connector


class Connexion:


    def __init__(self):
        self.__user = 'erwan'
        self.__host = 'localhost'
        self.__port = 3306
        self.__password = 'root'
        self.__database = 'netflix'
        self.cursor = None
        self.conn = None

    
    def ouvrir(self):
        self.conn = mysql.connector.connect(user=self.__user, host=self.__host, password=self.__password, port=self.__port, database=self.__database)
        self.cursor = self.conn.cursor()
    

    def req(self, req_sql, commit=False):
        self.ouvrir()
        self.cursor.execute(req_sql)

        if commit==False:
            res = [x for x in self.cursor]
            self.fermer()
            return res
        else:
            self.conn.commit()
            self.fermer()

    def req_many(self, req_sql, lis):
        self.ouvrir()
        self.cursor.executemany(req_sql,lis)
        self.conn.commit()
        self.fermer()


    def fermer(self):
        self.conn.close()


if __name__ == "__main__":
    
    co = Connexion()
    print(co.req("SHOW tables;"))
