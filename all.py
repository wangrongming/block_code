import re
import time

import datetime
import os

import execjs

# pg_local
database = "postgres"
user = "postgres"
password = "123456"
host = "127.0.0.1"
port = 5432


class PgOperate(object):
    @staticmethod
    def create_sql(column_item, table_name):
        today = str((datetime.datetime.now()).strftime('%Y%m%d'))
        column = ' ('
        values = ' ('
        for i in column_item:
            if str(column_item[i]) == 'None':
                continue
            column += i + ","
            one_word = re.sub("'", "\"", str(column_item[i]).replace('\n', '').replace('\r', '').replace(
                       '\t', '').replace('\\', ''))
            values += repr(one_word) + ","
        column = column[:-1] + ', insert_time, bizdate'

        values = values[:-1] + ', CURRENT_TIMESTAMP,' + repr(today)
        sql = 'insert into ' + table_name + column + ') values' + values + ');'
        return sql

    @staticmethod
    def generate_insert_many_sql(column_item, table_name):
        today = str((datetime.datetime.now()).strftime('%Y%m%d'))
        column = ' ('
        values = ' ('
        for i in column_item:
            if str(column_item[i]) == 'None':
                continue
            column += i + ","
            values += '%s' + ","
        column = column[:-1] + ',insert_time, bizdate'
        values = values[:-1] + ',CURRENT_TIMESTAMP,' + today
        sql = 'insert into ' + table_name + column + ') values' + values + ');'
        return sql

    def read_text_data_to_pg(self):
        cur = self.sql.cursor()
        try:
            i = 0
            with open(r"poi_school_base_info_hive_20190313.txt", "r", encoding="utf-8") as f:
                for line in f:
                    i = i + 1
                    print(i, line)
                    line = re.sub("\n", "", line)
                    line_info = line.split("\t")
                    item_dict = {
                        "key": line_info[0],
                    }
                    sql = self.create_sql(item_dict, "dzwl_result_to_analysis")
                    print(i)
                    cur.execute(sql)
        finally:
            cur.close()
            self.sql.commit()
            self.sql.close()

    def read_text_data_to_pg_quick(self):
        cur = self.sql.cursor()
        try:
            i = 0
            with open(r"D:\Work\txt\poi_hospital_base_info_hive_20190313.txt", "r", encoding="utf-8") as f:
                sql = None
                args = []
                for line in f:
                    insert_one_list = []
                    i = i + 1
                    print(i)
                    line = re.sub("\n", "", line)
                    line_info = line.split("\t")
                    item_dict = {
                        "key": line_info[0],
                        "id": line_info[1],
                    }
                    args.append([key for key in item_dict.values()])
                    sql = self.generate_insert_many_sql(item_dict, "dzwl_result_to_analysis")
                    if i//1000 == 1:
                        cur.executemany(sql, args)
                        i = 0
                        args = []
                if args:
                    cur.executemany(sql, args)
        finally:
            cur.close()
            self.sql.commit()
            self.sql.close()


class All(object):

    @staticmethod
    def time_info():
        print("-------------------------------time---------------------------------")
        print(time.time())
        print(time.strptime("2019-01-01 12:00:00", "%Y-%m-%d %H:%M:%S"))
        struct_time = time.strptime("2019-01-01 12:00:00", "%Y-%m-%d %H:%M:%S")
        print(time.strftime("%Y-%m-%d %H:%M:%S", struct_time))
        print("-------------------------------datetime---------------------------------")
        print(datetime.datetime.now())
        print(datetime.datetime.strptime("2019-01-01 12:00:00", "%Y-%m-%d %H:%M:%S"))
        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        print(datetime.datetime.now() + datetime.timedelta(days=1))
        print((datetime.datetime.now() - datetime.datetime.strptime("2019-01-01 12:00:00", "%Y-%m-%d %H:%M:%S")).days)

    @staticmethod
    def sorted_info():
        list_info = [1, -3, 2]
        sorted_list_info = sorted(list_info, key=lambda x: x, reverse=False)
        print(sorted_list_info)

        list_info = [1, -3, 5]
        sorted_list_info = sorted(list_info, key=lambda x: (x < 0, x+10), reverse=False)
        print(sorted_list_info)

        list_info = [{"name": "one", "age": 1}, {"name": "two", "age": 2}, {"name": "three", "age": 3}]
        sorted_list_info = sorted(list_info, key=lambda x: (x['age'], x['name']), reverse=False)
        print(sorted_list_info)
        pass

    @staticmethod
    def delete_log():
        start_path = "/root/logs"
        days = 5
        files_list = os.listdir(start_path)
        dict_info = {}
        for i in files_list:
            all_path = os.path.join(start_path, i)
            c_time = os.path.getctime(all_path)
            dict_info[all_path] = c_time
        sorted_path_by_time = sorted(dict_info.items(), key=lambda item: item[1])
        if len(sorted_path_by_time) <= days:
            pass
        else:
            for i in range(len(sorted_path_by_time) - days):
                os.remove(sorted_path_by_time[i][0])
        pass

    @staticmethod
    def exec_js():
        ctx = execjs.compile("""
        function s4() {
        return Math.floor((1 + Math.random()) * 0x10000).toString(16).substring(1);
        };
        function guid() {
        return s4() + s4() + '-' + s4() + '-' + s4() + '-' + s4() + '-' + s4() + s4() + s4();
        }
         """)
        a = ctx.call("s4")
        print(a)
        print(execjs.eval("Math.floor((1 + Math.random()) * 0x10000).toString(16).substring(1)"))
        pass

    def save_to_pg(self):

        pass

    def main(self):
        # self.time_info()  # 时间类型
        # self.sorted_info() # 字典排序
        # self.delete_log()  # 根据创建时间，保留最近几天日志文件，删除其他日志文件
        # self.exec_js()  # 执行js文件
        pass

if __name__ == '__main__':
    all_info = All()
    all_info.main()

    pg_operate = PgOperate()
    pg_operate.read_text_data_to_pg_quick()  # 多条数据保存到pg
    pg_operate.read_text_data_to_pg()  # 单条数据保存到pg
