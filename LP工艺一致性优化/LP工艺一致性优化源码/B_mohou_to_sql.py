"""
逻辑步骤：
1.程序开启
2.从数据库读取nxt_ip
3.遍历nxt_ip实时读取tcp数据
4.解析tcp数据，并存库
"""
from socket import *
import socket
import pymysql
import pandas as pd


# -------------------------------------------------- 提取有用函数 ------------------------------------------------------ #
def extract_data(string):
    """
    定义函数提取
    :param string:
    :return:
    """
    parts = string.split('=')
    return parts[-1]


# --------------------------------------------- 读取nxt_ip_port信息 ---------------------------------------------------- #
def query_mysql_nxt_ip(nxt_ip_table):
    """
    连接 MySQL 数据库，执行查询，并将查询结果转化为 Pandas DataFrame 对象
    :return: Pandas DataFrame 对象
    """

    conn = pymysql.connect(host='localhost', user='root', password='303631ZFMzfm@', db="lp")
    cursor = conn.cursor()

    # 如果未提供数据库连接引擎，则使用 pymysql 库连接 MySQL 数据库
    sql_query = f'''SELECT * FROM {nxt_ip_table}'''
    cursor.execute(sql_query)

    # 获取查询结果
    result = cursor.fetchall()
    nxt_ip_df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])

    # 关闭游标和数据库连接
    cursor.close()
    conn.close()
    return nxt_ip_df


# ---------------------------------------------- 全部膜厚数据写入数据库 -------------------------------------------------- #
def YK_to_mysql(extract_data_dict, machine):
    """
    每条膜厚数据按机台写入表格
    :return:
    """
    try:
        conn = pymysql.connect(host='localhost', user='root', password='303631ZFMzfm@', db="lp")
        # 执行非查询性质SQL
        cursor = conn.cursor()  # 获取到游标对象
        # 使用创建表，data_list[8]是字符串类型管号，用来作为创建表的依据
        sql_1 = f"""CREATE TABLE IF NOT EXISTS mohou_history_machine{machine} (
        id INT PRIMARY KEY AUTO_INCREMENT,
        detect_time timestamp, 
        Thickness varchar(50),
        WaferID varchar(50), 
        LineID varchar(50), 
        TubeID varchar(50),
        BoatID varchar(50),
        MangerID varchar(50),
        WaferPos varchar(50));"""
        cursor.execute(sql_1)

        # 将数据放入数据库
        sql_2 = f'''INSERT INTO mohou_history_machine{machine} (detect_time, Thickness, WaferID, LineID, TubeID, 
        BoatID, MangerID, WaferPos) VALUES('{extract_data_dict[0]}','{extract_data_dict[1]}','{extract_data_dict[2]}',
        '{extract_data_dict[3]}','{extract_data_dict[4]}','{extract_data_dict[5]}',
        '{extract_data_dict[6]}','{extract_data_dict[7]}');'''
        cursor.execute(sql_2)
        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print(f'{machine}膜厚写入错误:{e}')


# ---------------------------------------------- 批次膜厚数据写入数据库 -------------------------------------------------- #
def batch_YK_to_mysal(extract_data_dict, machine):
    """
    每条膜厚数据按机台按管好写入表格
    :return:
    """
    tube = extract_data_dict[4]
    try:
        conn = pymysql.connect(host='localhost', user='root', password='303631ZFMzfm@', db="lp")
        # 执行非查询性质SQL
        cursor = conn.cursor()  # 获取到游标对象
        # 使用创建表，data_list[8]是字符串类型管号，用来作为创建表的依据
        sql_1 = f"""CREATE TABLE IF NOT EXISTS batch_mohou_machine{machine}_tube{tube} (
        id INT PRIMARY KEY AUTO_INCREMENT,
        detect_time timestamp, 
        Thickness varchar(50),
        WaferID varchar(50), 
        LineID varchar(50), 
        TubeID varchar(50),
        BoatID varchar(50),
        MangerID varchar(50),
        WaferPos varchar(50));"""
        cursor.execute(sql_1)

        # 将数据放入数据库
        sql_2 = f'''INSERT INTO batch_mohou_machine{machine}_tube{tube} (detect_time, Thickness, WaferID, LineID, TubeID, 
        BoatID, MangerID, WaferPos) VALUES('{extract_data_dict[0]}','{extract_data_dict[1]}','{extract_data_dict[2]}',
        '{extract_data_dict[3]}','{extract_data_dict[4]}','{extract_data_dict[5]}',
        '{extract_data_dict[6]}','{extract_data_dict[7]}');'''
        cursor.execute(sql_2)
        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print(f'machine{machine}_tube{tube}批次膜厚写入错误:{e}')

# ------------------------------------------------- 读TCP获取膜厚 ------------------------------------------------------ #
def mohou_extraction(ip, port, machine):
    """
    1.从tcp读取膜厚
    2.解析膜厚数据
    3.膜厚数据存库
    :param ip: ip地址
    :param port: 端口号
    :return:
    """
    try:
        tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # 建立链接，要传入服务器ip和port
        tcp_socket.connect((ip, port))
        while True:
            # 接收数据
            data = str(tcp_socket.recv(1024))
            print(data)

            # 筛选数据
            if data.startswith("b'Result\\r\\ntag=") and 'ERROR' not in data:
                # 使用\\r\\n代表..，将数据分割
                data = data.split(sep='\\r\\n')

                # 提取数据，把数据由二维数据变为一维
                extract_data_dict = [extract_data(data[2]), extract_data(data[4]), extract_data(data[16]),
                                     extract_data(data[18]), extract_data(data[20]), extract_data(data[24]),
                                     extract_data(data[30]), extract_data(data[34])]
                print(extract_data_dict)

                # 整体膜厚数据写入数据库
                YK_to_mysql(extract_data_dict, machine)

                # 单批次膜厚数据保存
                batch_YK_to_mysal(extract_data_dict, machine)


        tcp_socket.close()

    except Exception as e:
        print(f'nxt_tcp连接失败:{e}！！！')


# -------------------------------------------------------------------------------------------------------------------- #
if __name__ == '__main__':
    nxt_ip_table = 'nxt_ip'
    nxt_ip_pd = query_mysql_nxt_ip(nxt_ip_table)

    while True:
        # 使用iterrows()遍历每一行，并获取列'ip'，'port'，'machine'的数据
        for index, row in nxt_ip_pd.iterrows():
            machine = row['machine']
            ip = row['ip']
            port = row['port']

            mohou_extraction(ip=ip, port=int(port), machine=machine)