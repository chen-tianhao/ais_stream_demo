import asyncio
import websockets
import json
from datetime import datetime, timezone
import pymysql


def pre_process_values(list_of_values):
    # handle ETA
    # handle Dimension
    return ""


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

    keys = deduplicate_dict.keys()
    values = deduplicate_dict.values()
    columns = ', '.join(keys)
    column_space_holder = (len(values)-1) * "%s, " + "%s"

    try:
        with connection.cursor() as cursor:
            # 插入数据的 SQL 语句
            sql = "INSERT INTO {} ({})VALUES ({})".format(message_type, columns, column_space_holder)
            # 提取 JSON 数据并执行插入操作
            cursor.execute(sql, tuple(list(values)))
            # 提交事务
            connection.commit()
            print("Data inserted successfully")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        connection.close()


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
            # print(message_json)
            
            insert_into_database(connection, message_json)

            message_type = message["MessageType"]
            if message_type not in message_type_set:
                message_type_set.add(message_type)
                print("[MSG TYPE", len(message_type_set), "]", message_type)
                print(message_json.decode('utf-8'))
            
            '''
            if message_type == "PositionReport":
                # the message parameter contains a key of the message type which contains the message itself
                ais_message = message['Message']['PositionReport']
                print(f"[{datetime.now(timezone.utc)}] ShipId: {ais_message['UserID']} Latitude: {ais_message['Latitude']} Longitude: {ais_message['Longitude']}")
            '''
        print('**********************')
        print(message_type_set)


if __name__ == "__main__":
    asyncio.run(connect_ais_stream())



