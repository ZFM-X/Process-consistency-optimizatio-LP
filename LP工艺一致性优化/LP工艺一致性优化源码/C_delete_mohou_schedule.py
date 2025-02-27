"""
定时删除7天前所有膜厚数据
"""
import pymysql
import datetime
import schedule
from datetime import datetime, timedelta
import time


# ------------------------------------------------ 每天定时删除膜厚数据 -------------------------------------------------- #
def delete_mohou_schedule():
    """
    删除以mohou_history开头的表格，7天前的数据
    :return:
    """
    # 数据库连接信息
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': '303631ZFMzfm@',
        'db': 'lp',
        'charset': 'utf8mb4'
    }

    # 计算7天前的日期
    seven_days_ago = datetime.now() - timedelta(days=7)
    seven_days_ago_str = seven_days_ago.strftime('%Y-%m-%d')

    try:
        # 连接到数据库
        connection = pymysql.connect(**db_config)
        with connection.cursor() as cursor:
            # 查询所有以'mohou_history'开头的表名
            cursor.execute("SHOW TABLES LIKE 'mohou_history%'")
            tables = [table[0] for table in cursor.fetchall()]

            # 遍历每个表，并删除7天前的数据
            for table in tables:
                sql = f"DELETE FROM {table} WHERE detect_time < %s"
                try:
                    # 注意：这里我们使用了一个参数化查询的占位符 %s，但表名仍然是直接插入的
                    cursor.execute(sql, (seven_days_ago_str,))
                    # 提交事务
                    connection.commit()
                    print(f"Deleted records before {seven_days_ago_str} from {table}")
                except (pymysql.MySQLError, Exception) as e:
                    # 发生错误时回滚事务
                    connection.rollback()
                    print(f"Error deleting records from {table}: {e}")
        # 关闭数据库连接
        connection.close()

    except Exception as e:
        print(f'数据库连接失败:{e}')


# ------------------------------------------------ 每天定时删除膜厚数据 -------------------------------------------------- #
if __name__ == '__main__':
    # 使用schedule库定时执行函数（例如，每天凌晨1点）
    schedule.every().day.at("16:00").do(delete_mohou_schedule)
    while True:
        schedule.run_pending()
        time.sleep(1)