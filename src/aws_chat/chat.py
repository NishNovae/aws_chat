# src/aws_chat/chat.py

from kafka import KafkaProducer, KafkaConsumer
from json import loads, dumps
import time
import threading

# SENDER
def pchat(chatroom, username):
    sender = KafkaProducer(
        # testing at localhost first
#        bootstrap_servers = ['localhost:9092'],
        bootstrap_servers = ['ec2-43-203-182-252.ap-northeast-2.compute.amazonaws.com:9092'],
        value_serializer = lambda x: dumps(x).encode('utf-8'),
    )

    while True:
        message = ""
        while message == "":
            print(f"{username}: ", end="")
            message = input()
                
        data = {'sender': username, 'message': message, 'time': time.time()}       
#        print(f"[DEBUG] sender.send({chatroom}, value={data}")
        sender.send(chatroom, value=data)
        sender.flush()
        time.sleep(0.5)

    
# RECEIVER
def cchat(chatroom, username):
    receiver = KafkaConsumer(
        # testing at localhost first
        chatroom,
       # bootstrap_servers = ['localhost:9092'],
        bootstrap_servers = ['ec2-43-203-182-252.ap-northeast-2.compute.amazonaws.com:9092'],
#        auto_offset_reset = 'earliest',
        enable_auto_commit = True,
        value_deserializer = lambda x: loads(x.decode('utf-8'))
    )

    try:
        for message in receiver:
            data = message.value
            if data['sender'] != username:
                print()
                print(f"{data['sender']}: {data['message']}")
                print(f"{username}: ", end="")

    except KeyboardInterrupt:
        print("Ending chat...")

    finally:
        receiver.close()

# Threading
chatroom = input("Input chatroom name: ")
username = input("Input Username: ")

print()
print(f"[INFO] Initializing chatroom [{chatroom}] for user [{username}]...")
print(f"[INFO] Initialize complete! Enjoy chatting!")
print()

thread_1 = threading.Thread(target = pchat, args = (chatroom, username))
thread_2 = threading.Thread(target = cchat, args = (chatroom, username))

thread_1.start()
thread_2.start()
