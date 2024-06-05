import asyncio
import websockets
import json
from datetime import datetime, timezone
import pymysql


def time_format_conversion(time_str):
    time_str = time_str[:19]
    time_format = "%Y-%m-%d %H:%M:%S"
    time_obj = datetime.strptime(time_str, time_format)
    return time_obj


def pre_process_values(list_of_values):
    for i in range(len(list_of_values)):
        item = list_of_values[i]
        if isinstance(item, dict):
            if 'Month' in item and 'Day' in item and 'Hour' in item and 'Minute' in item:
                list_of_values[i] = f"{item['Month']}/{item['Day']} {item['Hour']}:{item['Minute']}"
            else:
                json_str = json.dumps(item)
                list_of_values[i] = json_str
        elif isinstance(item, str):
            if item.endswith('UTC'):
                list_of_values[i] = time_format_conversion(item)
            elif len(item) == 0:
                list_of_values[i] = 'null'
        elif isinstance(item, bytes):
            list_of_values[i] = 'BinaryData'
        elif item is None:
            list_of_values[i] = 'None'
    # handle ETA
    # handle Dimension
    return tuple(list_of_values)


def insert_into_database(connection, json_str):
    # pre-process
    message = json.loads(json_str)
    message_type = message["MessageType"]
    merged_data = {**message['MetaData'], **message['Message'][message_type]}
    merged_json = json.dumps(merged_data, indent=4)

    data_dict = json.loads(merged_json)
    deduplicate_dict = {}
    for key, value in data_dict.items():
        new_key = key.lower()  # 将键转换为小写形式
        if new_key not in deduplicate_dict:
            deduplicate_dict[new_key] = value
    if 'binarydata' in deduplicate_dict:
        deduplicate_dict['binarydata'] = 'BinaryData'

    keys = deduplicate_dict.keys()
    values = deduplicate_dict.values()
    columns = ', '.join(keys)
    column_space_holder = (len(values)-1) * "%s, " + "%s"

    try:
        with connection.cursor() as cursor:
            # 插入数据的 SQL 语句
            sql = "INSERT INTO {} ({})VALUES ({})".format(message_type, columns, column_space_holder)
            # 提取 JSON 数据并执行插入操作
            paras = pre_process_values(list(values))
            string_elements = [str(element) for element in paras]
            para_string = ",".join(string_elements)
            # print("SQL: {}\nPARAS: {}".format(sql, para_string))

            cursor.execute(sql, paras)

            # 提交事务
            connection.commit()
            print("Data inserted successfully\n")
    except Exception as e:
        print(f"Error: {e}")


async def connect_ais_stream():
    #1.2959502366963822, 103.74622874002395
    #1.222700429276618, 103.83548120620112
    async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
        subscribe_message = {"APIKey": "9f0a044d87e428da413689e2d736523b637b8599", "BoundingBoxes": [[[1.49072, 103.33058], [1.22598, 104.61901]]]}

        subscribe_message_json = json.dumps(subscribe_message)
        await websocket.send(subscribe_message_json)
        
        message_type_set = set()

        # 数据库连接配置
        db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': 'C4NGP_nus',
            'database': 'test1',
            'port': 30001
        }

        # 连接到 MySQL 数据库
        connection = pymysql.connect(**db_config)

        async for message_json in websocket:
            message = json.loads(message_json)
            message_type = message["MessageType"]
            insert_into_database(connection, message_json)
            
            if message_type not in message_type_set:
                message_type_set.add(message_type)
                print("[MSG TYPE", len(message_type_set), "]", message_type)
                print(message_json.decode('utf-8'))
                # print('--------------------------------------\n')
        
        # connection.close()
        print('**************************************')
        print(message_type_set)


if __name__ == "__main__":
    asyncio.run(connect_ais_stream())



