# src/aws_chat/chat.py

from kafka import KafkaProducer, KafkaConsumer
from json import loads, dumps
import threading

#THREAD_RUNNING = True

def create_data(username, message, end):
    return {'sender': username, 'message': message, 'end': end}

# SENDER
def pchat(chatroom, username):
    sender = KafkaProducer(
        bootstrap_servers = ['localhost:9092'],
        # bootstrap_servers = ['ec2-43-203-182-252.ap-northeast-2.compute.amazonaws.com:9092'],
        value_serializer = lambda x: dumps(x).encode('utf-8'),
    )

    try:
        initial_msg = f"User [{username}] has entered the chat!"
        end = False

        data = create_data(username, initial_msg, end)
        sender.send(chatroom, data)
        sender.flush()        

        while True:
            message = ""
            while message == "":
                print(f"{username}: ", end="")
                message = input()
        
            if message == 'exit':
                message = f"User [{username}] has exited the chatroom."
                end = True

            data = {'sender': username, 'message': message, 'end': end}       
            sender.send(chatroom, value=data)
            sender.flush()

            if end == True:
                print("Exiting chat...")
                return    

    except KeyboardInterrupt:
        print("Encountered keyboard interrupt. Finishing chat...")
        return
    return

    
# RECEIVER
def cchat(chatroom, username):
    receiver = KafkaConsumer(
        chatroom,
        bootstrap_servers = ['localhost:9092'],
        # bootstrap_servers = ['ec2-43-203-182-252.ap-northeast-2.compute.amazonaws.com:9092'],
        enable_auto_commit = True,
        value_deserializer = lambda x: loads(x.decode('utf-8'))
    )

    try:
        for message in receiver:
            data = message.value

            if data['end'] == True:
                # someone not me has exited
                if data['sender'] != username:
                    print()
                    print(f"User {data['sender']} has exited the chat.")
                    print("Type in 'exit' to also finish the chat.")
                    print(f"{username}: ", end="")

                # I'm exiting!! finish thread
                else:
                    return

            elif data['sender'] != username:
                print()
                print(f"{data['sender']}: {data['message']}")
                print(f"{username}: ", end="")

    except KeyboardInterrupt:
        print("Exiting chat...")
        return

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

thread_1.join()
thread_2.join()
