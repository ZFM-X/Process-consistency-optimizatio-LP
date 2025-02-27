"""
逻辑步骤
1.实时监控各个膜厚表，当最后一条数据距离此刻超过2h时，读取膜厚
2.膜厚聚合
3.根据膜厚库名，读取相应lp_ip
4.根据膜厚库名，读取点位
5.根据点位，进行数据采集
6.根据膜厚库名，读取参数
***** 数据使用列表格式流转 *****
[1上，1中，1下，2上，2中，2下，3上，3中，3下，4上，4中，4下，5上，5中，5下，6上，6中，6下，时间]
"""
import pandas as pd
import numpy as np
import pymysql
from opcua import Client, ua
import D_R2R
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import re
from datetime import datetime, timedelta

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


# #################################################### 删除数据 ######################################################## #
# --------------------------------------------------- 删除批次膜厚 ------------------------------------------------------ #
def delete_batch_YK(batch_YK_table_name):
    try:
        conn = pymysql.connect(host='localhost', user='root', password='303631ZFMzfm@', db="lp")
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {batch_YK_table_name}")
        conn.commit()
        conn.close()

    except Exception as e:
        print(f'删除{batch_YK_table_name}批次膜厚时出错：{e}')


# ################################################### 数据获取 ######################################################### #
# --------------------------------------------- 调试规则,膜厚读取函数-SQL ------------------------------------------------ #
def query_entire_table(table_name):
    """
    连接 MySQL 数据库，执行查询，并将查询结果转化为 Pandas DataFrame 对象
    :return: Pandas DataFrame 对象
    """
    conn = pymysql.connect(host='localhost', user='root', password='303631ZFMzfm@', db="lp")
    cursor = conn.cursor()

    # 如果未提供数据库连接引擎，则使用 pymysql 库连接 MySQL 数据库
    sql_query = f'''SELECT * FROM {table_name}'''
    cursor.execute(sql_query)

    # 获取查询结果
    result = cursor.fetchall()
    df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])

    # 关闭游标和数据库连接
    cursor.close()
    conn.close()
    return df


# -------------------------------------------------- 膜厚聚合逻辑  ----------------------------------------------------- #
def Aggregate_mohou(batch_YK_df, YK_target, CUT_LSL, CUT_USL):
    """
    根据单批次数据，聚合计算得到所有YK，并保存到数据库，基本表，空值时使用整体均值
    :return:返回用于计算的YK
    """
    # 根据偏位，创建新列将dataframe划分为上中下
    bins = [0, CUT_LSL[0], CUT_USL[0], CUT_LSL[1], CUT_USL[1], CUT_LSL[2], CUT_USL[2], 100]
    group_names = ['忽略', '上', '忽略1', '中', '忽略2', '下', '忽略3']
    batch_YK_df['pianwei'] = pd.cut(batch_YK_df['WaferPos'].astype(int), bins, labels=group_names)
    batch_YK_df['Thickness'] = batch_YK_df['Thickness'].astype(float)

    # 计算上中下舟的平均膜厚
    batch_data = batch_YK_df.groupby(['MangerID', 'pianwei'])['Thickness'].agg({'mean', 'count'})
    batch_data = batch_data.reset_index()
    batch_data.dropna(inplace=True)
    list1 = ['忽略', '忽略1', '忽略2', '忽略3']
    batch_data = batch_data[batch_data.pianwei.isin(list1) == False]

    # 新建字典用于记录Yk
    zones = [1, [1, 2], [3, 4, 5], [6, 7, 8], [9, 10], 10]
    locations = ['上', '中', '下']

    batch_Yk_list = []
    for zone in zones:
        for location in locations:
            # 温区对应单个小舟
            if isinstance(zone, int):
                if batch_data[(batch_data['MangerID'] == f'{zone}') & (batch_data['pianwei'] == f'{location}')][
                    'mean'].mean() is np.nan:
                    batch_Yk_list.append(YK_target[18])
                else:
                    batch_Yk_list.append(
                        batch_data[(batch_data['MangerID'] == f'{zone}') & (batch_data['pianwei'] == f'{location}')][
                            'mean'].mean())

            # 温区对应两个小舟
            elif len(zone) == 2:
                if batch_data[(batch_data['MangerID'] == f'{zone[0]}') | (batch_data['MangerID'] == f'{zone[1]}') & (
                        batch_data['pianwei'] == f'{location}')]['mean'].mean() is np.nan:
                    batch_Yk_list.append(YK_target[18])
                else:
                    batch_Yk_list.append(
                        batch_data[
                            (batch_data['MangerID'] == f'{zone[0]}') | (batch_data['MangerID'] == f'{zone[1]}') & (
                                    batch_data['pianwei'] == f'{location}')][
                            'mean'].mean())

            # 温区对应三个小舟
            elif len(zone) == 3:
                if batch_data[(batch_data['MangerID'] == f'{zone[0]}') | (batch_data['MangerID'] == f'{zone[1]}') | (
                        batch_data['MangerID'] == f'{zone[2]}') & (
                                      batch_data['pianwei'] == f'{location}')]['mean'].mean() is np.nan:
                    batch_Yk_list.append(YK_target[18])
                else:
                    batch_Yk_list.append(
                        batch_data[
                            (batch_data['MangerID'] == f'{zone[0]}') | (batch_data['MangerID'] == f'{zone[1]}') | (
                                    batch_data['MangerID'] == f'{zone[2]}') & (
                                    batch_data['pianwei'] == f'{location}')][
                            'mean'].mean())

    if batch_data['mean'].mean() is np.nan:
        batch_Yk_list.append(YK_target[18])
    else:
        batch_Yk_list.append(batch_data['mean'].mean())

    print(10 * '-' + '批次膜厚聚合完成' + 10 * '-')

    # 字典中加入时间，管号 便于放入数据库中查看
    return batch_Yk_list


# ----------------------------------------------- 输入DLV读取函数-OPC --------------------------------------------------- #
def query_opc_dlv(opc_dlv, ip, port, tube):
    """
    定义函数，连接OPC，接收时间、温度数据，并将数据保存
    :return: input_dict
    """
    opc_dlv = opc_dlv[opc_dlv['category'] == '1'][['opc_prefix', 'opc_suffix']].iloc[:19]

    opc_combined_list = [prefix + str(tube) + suffix for prefix, suffix in
                         zip(opc_dlv['opc_prefix'], opc_dlv['opc_suffix'])]

    client = Client(f'opc.tcp://{ip}:{port}')
    client.connect()

    DLV_in_list = []
    for opc in opc_combined_list:
        # 所有温区中部温度值
        myvar = client.get_node(opc)
        valuetmp = myvar.get_value()
        DLV_in_list.append(valuetmp)

    client.disconnect()
    return DLV_in_list


# ------------------------------------------------- CK或DLV_out获取 --------------------------------------------------- #
def query_mysql_data(data_name, machine, tube):
    """
    获取最新一条CK
    :param machine: 机台号
    :param tube: 管号
    :return:
    """
    conn = pymysql.connect(host='localhost', user='root', password='303631ZFMzfm@', db="lp")
    cursor = conn.cursor()

    sql = "show tables;"
    cursor.execute(sql)
    tables = [cursor.fetchall()]
    table_list = re.findall('(\'.*?\')', str(tables))
    table_list = [re.sub("'", '', each) for each in table_list]

    table_name = f'{data_name}_history_machine{machine}_tube{tube}'

    if table_name not in table_list:
        data_list = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

        cursor.close()
        conn.close()

        return data_list

    else:
        sql_query = f"""SELECT * FROM {data_name}_history_machine{machine}_tube{tube} ORDER BY jointime DESC LIMIT 1"""
        cursor.execute(sql_query)

        result = cursor.fetchall()
        data = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        data_list = data.iloc[0].tolist()
        data_list.pop()

        cursor.close()
        conn.close()

        return data_list


# #################################################### 数据保存 ######################################################## #
# ------------------------------------------------ CK或DLV_out写入数据库 ------------------------------------------------ #
def data_to_mysql(data_name, machine, tube, CK_name, CK_list):
    """
    每条膜厚数据按机台写入表格
    :return:
    """
    # 建立到MySQL数据库的引擎
    code = '303631ZFMzfm@'
    code = quote_plus(code)
    engine = create_engine(f'mysql+pymysql://root:{code}@localhost/lp')

    # 列名处理
    time_column_name = 'jointime'
    name = CK_name + [time_column_name]

    # 数据处理
    time = str(pd.Timestamp.now())
    value = CK_list + [time]

    df = pd.DataFrame([value], columns=name)
    df.to_sql(f'{data_name}_history_machine{machine}_tube{tube}', engine, if_exists='append', index=False)


# --------------------------------------------------- 调整记录保存 ------------------------------------------------------ #
def adjust_hiastory_to_mysql(machine, tube, mohou_name, mohou_list, CK_name, CK_list, DLV_in_name, DLV_in_list,
                             DLV_out_name, DLV_out_list, YK_pred_name, YK_pred_list):
    """
    前馈参数保存
    :param machine: 机台号
    :param tube: 管号
    :param mohou_name: 膜厚列名
    :param mohou_list: 膜厚
    :param CK_name: CK列名
    :param CK_list: Ck值
    :param DLV_in_name: 输入列名
    :param DLV_in_list: 输入值
    :param DLV_out_name:输出列名
    :param DLV_out_list: 输出值
    :param YK_pred_name:输出列名
    :param YK_pred_list: 输出值
    :return:
    """
    # 建立到MySQL数据库的引擎
    code = '303631ZFMzfm@'
    code = quote_plus(code)
    engine = create_engine(f'mysql+pymysql://root:{code}@localhost/lp')

    # 设计列名
    time_column_name = 'time'
    tube_name = 'tube'
    name = mohou_name + CK_name + DLV_in_name + DLV_out_name + YK_pred_name
    name.append(tube_name)
    name.append(time_column_name)

    # 设计值
    time = str(pd.Timestamp.now())
    tube_num = tube
    value = mohou_list + CK_list + DLV_in_list + DLV_out_list + YK_pred_list
    value.append(tube_num)
    value.append(time)

    df = pd.DataFrame([value], columns=name)
    df.to_sql(f'adjust_history_{machine}', engine, if_exists='append', index=False)


# #################################################### 命令下发 ######################################################## #
# ----------------------------------------------------- 命令下发 ------------------------------------------------------- #
def command_delivery(opc_dlv, ip, port, tube, DLV_out_list):
    """
    定义函数，连接OPC，接收时间、温度数据，并将数据保存
    :return: input_dict
    """
    opc_dlv = opc_dlv[opc_dlv['category'] == '1'][['opc_prefix', 'opc_suffix']]
    opc_dlv = opc_dlv.iloc[:19]  # 取前19行点位

    opc_combined_list = [prefix + str(tube) + suffix for prefix, suffix in
                         zip(opc_dlv['opc_prefix'], opc_dlv['opc_suffix'])]

    client = Client(f'opc.tcp://{ip}:{port}')
    client.connect()

    for i, opc in enumerate(opc_combined_list):
        if i == 18:
            # 所有温区中部温度值
            myvar = client.get_node(opc)
            var = ua.Variant(int(DLV_out_list[i]), ua.VariantType.Int32)  # 时间为整数类型
            myvar.set_value(var)

        else:
            # 所有温区中部温度值
            myvar = client.get_node(opc)
            var = ua.Variant(float(DLV_out_list[i]), ua.VariantType.Float)  # 温度为浮点类型
            myvar.set_value(var)

    client.disconnect()
    print(10 * '-' + 'DLV_out命令下发完成' + 10 * '-')


# -------------------------------------------------------------------------------------------------------------------- #
if __name__ == '__main__':
    # 初始字典，用于判断机台是否需要初始化
    run_once = {}
    # 参数提取
    para_df = query_entire_table('para')  # 读取调整参数
    opc_dlv = query_entire_table('opc_dlv')  # 读取opv_dlv点位
    lp_ip = query_entire_table('lp_ip')  # 读取lp_ip
    # -------------------------------------------------- 读取计算数据 -------------------------------------------------- #
    zone = para_df['zone'].tolist()
    A = para_df['A'].tolist()
    A_m = para_df['A_m'].tolist()
    W = para_df['W'].tolist()
    YK_LSL_warning = para_df['YK_LSL_warning'].tolist()
    YK_USL_warning = para_df['YK_USL_warning'].tolist()
    YK_target = para_df['YK_TARGET'].tolist()
    max_change = para_df['MAX_CHANGE'].tolist()
    DLV_out_LSL = para_df['DLV_LSL'].tolist()
    DLV_out_USL = para_df['DLV_USL'].tolist()
    CUT_LSL = para_df['CUT_LSL'].tolist()
    CUT_USL = para_df['CUT_USL'].tolist()

    A = [float(x) for x in A]
    A_m = [float(x) for x in A_m if x is not None]
    W = [float(x) for x in W]
    YK_LSL_warning = [float(x) for x in YK_LSL_warning]
    YK_USL_warning = [float(x) for x in YK_USL_warning]
    YK_target = [float(x) for x in YK_target]
    max_change = [float(x) for x in max_change]
    DLV_out_LSL = [float(x) for x in DLV_out_LSL]
    DLV_out_USL = [float(x) for x in DLV_out_USL]
    CUT_LSL = [int(x) for x in CUT_LSL]
    CUT_USL = [int(x) for x in CUT_USL]

    # 根据zone新建列表，用于存储数据时的列名
    CK_name = [item + '_CK' for item in zone]
    mohou_name = [item + '_mohou' for item in zone]
    mohou_pred_name = [item + '_mohou_pred' for item in zone]
    DLV_out_name = [item + '_DLV_out' for item in zone]
    DLV_in_name = [item + '_DLV_in' for item in zone]
    YK_pred_name = [item + '_YK_pred' for item in zone]

    # ---------------------------------------------- 循环监控批次膜厚数据库 ----------------------------------------------- #
    while True:
        try:
            conn = pymysql.connect(host='localhost', user='root', password='303631ZFMzfm@', db="lp")
            cursor = conn.cursor()

            # 执行SQL查询以获取以batch_mohou开头的表名，并转化为列表
            sql = "SHOW TABLES LIKE 'batch_mohou%'"
            cursor.execute(sql)
            table_list = [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print('batch_mohou数据库访问失败！！！')
        else:
            # 遍历每张表，获取机台号和管号
            for batch_YK_table_name in table_list:
                machine = batch_YK_table_name[-10:-6]
                tube = batch_YK_table_name[-1]
                ip = lp_ip.loc[lp_ip['machine'] == f'{machine}', 'ip'].iloc[0]
                port = lp_ip.loc[lp_ip['machine'] == f'{machine}', 'port'].iloc[0]
                status = lp_ip.loc[lp_ip['machine'] == f'{machine}', 'status'].iloc[0]

                if machine not in run_once:
                    run_once[machine] = {}  # 创建当前机台状态字典

                # --------------------------------------- 膜厚触发计算逻辑 ---------------------------------------------- #
                batch_YK_df = query_entire_table(batch_YK_table_name)

                if not batch_YK_df.empty and status == '1':
                    batch_YK_df['detect_time'] = pd.to_datetime(batch_YK_df['detect_time'])
                    latest_timestamp = batch_YK_df['detect_time'].max()
                    current_time = datetime.now()
                    time_difference = current_time - latest_timestamp

                    if time_difference > timedelta(minutes=20):
                        print(f'{batch_YK_table_name}距当前超过20分钟，开始计算')

                        # ---------------------------------------- 各参数初始化 ----------------------------------------- #
                        CK_list = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                        CK_1_list = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                        deta1_list = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                        deta2_list = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                        YK_pred_list = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

                        # ------------------------------------------- 监控人为调整 ------------------------------------- #
                        # 读取上批DLV_out和本批DLV_in，对比判断是否有人为干预
                        DLV_in_list = query_opc_dlv(opc_dlv, ip, port, tube)  # 读取DLV_in_list
                        # 读取上批DLV_out_list
                        DLV_out_list = query_mysql_data('dlv_out', machine, tube)
                        print(f'上批DLV_out：{DLV_out_list}')
                        print(f'本批DLV_in：{DLV_in_list}')
                        if DLV_in_list != DLV_out_list:
                            print('''DLV_in!=DLV_out:原因可能为 1.首次计算，2.人为调整，进行参数初始化设置！！！''')

                        # ------------------------------------------- 程序初始化 --------------------------------------- #
                        if run_once.get(f'{machine}', {}).get(f'Tube_{tube}', None) is None or DLV_in_list != DLV_out_list:
                            run_once[machine][f'Tube_{tube}'] = 1  # 管状态变更
                            batch_YK_list = YK_target  # 使用目标值做初始化
                            for i in range(len(batch_YK_list)):
                                CK_1 = D_R2R.R2R_initial(batch_YK_list[i], DLV_in_list[i], A[i])
                                CK_1_list[i] = CK_1

                            data_to_mysql('ck', machine, tube, CK_name, CK_1_list)  # 初始化CK保存

                            # DLV_out更新和保存
                            DLV_out_list = DLV_in_list
                            data_to_mysql('dlv_out', machine, tube, DLV_out_name, DLV_out_list)

                            # 删除批次膜厚
                            delete_batch_YK(batch_YK_table_name)

                            print(f'batch_YK_list:{batch_YK_list}')
                            print(f'CK_1_list:{CK_1_list}')
                            print(30 * '#' + f'_机台{machine}_管{tube}初始化计算完成_' + 30 * '#')

                        # ------------------------------------------ 正常计算 ------------------------------------------ #
                        elif run_once[machine][f'Tube_{tube}'] == 1 and DLV_in_list == DLV_out_list:
                            batch_YK_list = Aggregate_mohou(batch_YK_df, YK_target, CUT_LSL, CUT_USL)  # 获取聚合膜厚
                            CK_1_list = query_mysql_data('ck', machine, tube)

                            # ------------------------------------ 调整反应时间 ---------------------------------------- #
                            i = 18
                            CK, DLV_out, deta1, YK_pred = D_R2R.R2R_time(batch_YK_list[i], DLV_in_list[i], A[i],
                                                                         W[i],
                                                                         YK_LSL_warning[i], YK_USL_warning[i],
                                                                         YK_target[i],
                                                                         CK_1_list[i], max_change[i],
                                                                         DLV_out_LSL[i],
                                                                         DLV_out_USL[i])
                            CK_list[i] = CK
                            DLV_out_list[i] = DLV_out
                            deta1_list[i] = deta1
                            YK_pred_list[i] = YK_pred

                            # ----------------------------------- 调整2-5温度 ------------------------------------------ #
                            # 2.2-5温区温度
                            for i in range(3, 15):
                                CK, DLV_out, deta2, YK_pred = D_R2R.R2R_Temp_first_step(batch_YK_list[i],
                                                                                        DLV_in_list[i],
                                                                                        A[18],
                                                                                        A[i], W[i], deta1_list[18],
                                                                                        YK_LSL_warning[i],
                                                                                        YK_USL_warning[i],
                                                                                        YK_target[i], CK_1_list[i],
                                                                                        max_change[i],
                                                                                        DLV_out_LSL[i],
                                                                                        DLV_out_USL[i])

                                CK_list[i] = CK
                                DLV_out_list[i] = DLV_out
                                deta2_list[i] = deta2
                                YK_pred_list[i] = YK_pred

                            # ---------------------------------- 调整1，6温度 ------------------------------------------ #
                            for i in [0, 1, 2, 15, 16, 17]:
                                if i in [0, 1, 2]:
                                    CK, DLV_out, YK_pred = D_R2R.R2R_Temp_second_step(batch_YK_list[i],
                                                                                      DLV_in_list[i],
                                                                                      A[18],
                                                                                      A[i], A_m[i], W[i],
                                                                                      deta1_list[18],
                                                                                      deta2_list[i + 3],
                                                                                      YK_LSL_warning[i],
                                                                                      YK_USL_warning[i],
                                                                                      YK_target[i], CK_1_list[i],
                                                                                      max_change[i],
                                                                                      DLV_out_LSL[i],
                                                                                      DLV_out_USL[i])

                                else:
                                    CK, DLV_out, YK_pred = D_R2R.R2R_Temp_second_step(batch_YK_list[i],
                                                                                      DLV_in_list[i],
                                                                                      A[18],
                                                                                      A[i], A_m[i], W[i],
                                                                                      deta1_list[18],
                                                                                      deta2_list[i - 3],
                                                                                      YK_LSL_warning[i],
                                                                                      YK_USL_warning[i],
                                                                                      YK_target[i],
                                                                                      CK_1_list[i],
                                                                                      max_change[i], DLV_out_LSL[i],
                                                                                      DLV_out_USL[i])

                                CK_list[i] = CK
                                DLV_out_list[i] = DLV_out
                                YK_pred_list[i] = YK_pred

                            # 命令下发
                            command_delivery(opc_dlv, ip, port, tube, DLV_out_list)

                            # CK保存
                            data_to_mysql('ck', machine, tube, CK_name, CK_list)

                            # DLV_out保存
                            data_to_mysql('dlv_out', machine, tube, DLV_out_name, DLV_out_list)

                            # 调整过程参数保存
                            adjust_hiastory_to_mysql(machine, tube, mohou_name, batch_YK_list, CK_name, CK_list,
                                                     DLV_in_name,
                                                     DLV_in_list, DLV_out_name, DLV_out_list, YK_pred_name,
                                                     YK_pred_list)
                            
                            # 删除批次膜厚
                            delete_batch_YK(batch_YK_table_name)

                            print(f'上批batch_YK_list:{batch_YK_list}')
                            print(f'本批CK_1_list:{CK_1_list}')
                            print(f'本批CK_list:{CK_list}')
                            print(f'本批DLV_out_list:{DLV_out_list}')
                            print(f'本批YK_pred_list:{YK_pred_list}')
                            print(30 * '#' + f'_机台{machine}_管{tube}初始化计算完成_' + 30 * '#')