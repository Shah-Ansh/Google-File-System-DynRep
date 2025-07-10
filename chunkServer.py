import os
import socket
import json
import time
import threading
import random

def load_env_file():
    with open("../ds_cp/ds-course_project/.env") as file:
        for line in file:
            if line.startswith("#") or not line.strip():
                continue
            key, value = line.strip().split("=", 1)
            os.environ[key] = value


load_env_file()
listOfChunks=[]
serverclientPort = str(os.environ["SERVER_CHUNKSERVER_PORT"])
# lock = threading.Lock()
listenPort = 0
currLoad=0
lock=threading.Lock()
def sendPing(listenAddr,listePort):
    global listOfChunks
    global serverclientPort
    global currLoad
    global lock
    while True:

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        currListOfChunks = []
        with lock:
            currListOfChunks = listOfChunks.copy()
        sock.connect(("127.0.0.1", int(serverclientPort)))
        dataToSend = {"operation": "ping","chunkAddr":listenAddr,"chunkPort":listePort,"chunks":currListOfChunks,"load":currLoad}
        dataToSend = json.dumps(dataToSend)
        sock.send(dataToSend.encode())
        with lock:
            currLoad=0
        message = sock.recv(1024)
        # print(message.decode())
        sock.close()
        time.sleep(3)

def uploadChunk(chunHandle,chunkData,listofRecipients):
    global listOfChunks
    global listenPort
    global lock
    with lock:

        listOfChunks.append(chunHandle)
    chunHandle=str(chunHandle)
    if ["127.0.0.1",int(listenPort)] in listofRecipients:
        with open(chunHandle, "w") as f:
            f.write(chunkData)
        print(type(listofRecipients[0]))
        listofRecipients.remove(["127.0.0.1",int(listenPort)])
    if len(listofRecipients) == 0:
        return
    randomReciever = random.choice(listofRecipients)
    toSend = {
        "operation": "upload",
        "chunkData":chunkData,
        "chunkHandle": chunHandle,
        "listofRecipients": listofRecipients,
    }
    toSend = json.dumps(toSend)
    toSend = toSend.encode("utf-8")
    lenToSend = len(toSend)
    currSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    currSock.connect((randomReciever[0], randomReciever[1]))
    currSock.send(str(lenToSend).encode("utf-8").ljust(10))
    currSock.sendall(toSend)
    currSock.close()
# def writechunk(chunkHandle, data, offset, listOfRecipients):
#     global listOfChunks
#     global listenPort
#     try :
#         with open(chunkHandle, "r+") as f:
#             print("Offset: " + str(offset))
#             f.seek(offset)
#             f.write(data)
#         listOfRecipients.remove(["127.0.0.1",listenPort])
#         if len(listOfRecipients) == 0:
#             return "writeSuccess"
#         randomReciever = random.choice(listOfRecipients)
#         toSend = {
#             "operation": "write",
#             "chunkHandle": chunkHandle,
#             "data": data,
#             "offset": offset,
#             "listOfRecipients": listOfRecipients,
#         }
#         toSend = json.dumps(toSend)
#         toSend = toSend.encode("utf-8")
#         lenToSend = len(toSend)
#         currSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#         currSock.connect((randomReciever[0], randomReciever[1]))
#         currSock.send(str(lenToSend).encode("utf-8").ljust(10))
#         currSock.sendall(toSend)
#         lenTorecieve = int(currSock.recv(10).decode("utf-8").strip())
#         recivedData = bytearray()
#         while len(recivedData) < lenTorecieve:
#             toRecieveLength = min(lenTorecieve - len(recivedData), 2048)
#             recivedData.extend(currSock.recv(toRecieveLength))
#         recivedData = recivedData.decode("utf-8")
#         recivedData = json.loads(recivedData)
#         response = recivedData["response"]
#         if response == "writeSuccess":
#             return "writeSuccess"
#         else:
#             return "writeFailed"
#     except:
#         return "writeFailed"

def writechunk(chunkHandle, data, offset, listOfRecipients):
    global listOfChunks
    global listenPort
    try:
        with open(str(chunkHandle), "r+") as f:
            print("Offset:", offset)
            f.seek(offset)

            # Ensure data is a string for text mode writing
            if not isinstance(data, str):
                data = str(data)

            f.write(data)

            # Confirm write action
            print(f"Data '{data}' written to file at offset {offset}.")

        # Remove local IP from recipients
        listOfRecipients = [r for r in listOfRecipients if r != ["127.0.0.1", listenPort]]
        if not listOfRecipients:
            return "writeSuccess"

        # Send data to another recipient
        randomReciever = random.choice(listOfRecipients)
        toSend = {
            "operation": "write",
            "chunkHandle": chunkHandle,
            "data": data,
            "offset": offset,
            "listOfRecipients": listOfRecipients,
        }
        toSend = json.dumps(toSend).encode("utf-8")
        lenToSend = len(toSend)

        # Set up socket connection
        currSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        currSock.connect((randomReciever[0], randomReciever[1]))
        currSock.send(str(lenToSend).encode("utf-8").ljust(10))
        currSock.sendall(toSend)

        # Receive confirmation
        lenToReceive = int(currSock.recv(10).decode("utf-8").strip())
        receivedData = bytearray()
        while len(receivedData) < lenToReceive:
            toReceiveLength = min(lenToReceive - len(receivedData), 2048)
            receivedData.extend(currSock.recv(toReceiveLength))
        receivedData = json.loads(receivedData.decode("utf-8"))
        
        response = receivedData.get("response")
        return "writeSuccess" if response == "writeSuccess" else "writeFailed"
    except Exception as e:
        print(f"Error occurred: {e}")
        return "writeFailed"

def clientOperation(currSock):
    global listOfChunks
    global currLoad
    global lock
    # global currLoad
    with lock:
        currLoad+=1
    numberOfBytes = currSock.recv(10).decode("utf-8").strip()
    numberOfBytes = int(numberOfBytes)
    recivedData = bytearray()
    while len(recivedData) < numberOfBytes:
        toRecieveLength = min(numberOfBytes - len(recivedData), 2048)
        recivedData.extend(currSock.recv(toRecieveLength))
    recivedData = recivedData.decode("utf-8")
    # print("Recived Data: " + recivedData)
    recivedData = json.loads(recivedData)
    operation = recivedData["operation"]
    if operation == "upload":
        chunkData = recivedData["chunkData"]
        chunkHandle = recivedData["chunkHandle"]
        listofRecipients = recivedData["listofRecipients"]
        uploadChunk(chunkHandle,chunkData,listofRecipients)
    elif operation == "read":
        chunkHandle = recivedData["chunkHandle"]
        start = int(recivedData["start"])
        end = int(recivedData["end"])
        try:
            with open(str(chunkHandle), "r") as f:
                f.seek(start)
                data = f.read(end-start)
                toSend = {
                    "response":"readSuccess",
                    "data":data
                }
                toSend = json.dumps(toSend)
                toSend = toSend.encode("utf-8")
                currSock.send(str(len(toSend)).encode("utf-8").ljust(10))
                currSock.sendall(toSend)
        except:
            toSend = {
                "response":"readFailed"
            }
            toSend = json.dumps(toSend)
            toSend = toSend.encode("utf-8")
            currSock.send(str(len(toSend)).encode("utf-8").ljust(10))
            currSock.sendall(toSend)
    elif operation == "write":
        chunkHandle = recivedData["chunkHandle"]
        data = recivedData["data"]
        offset = int(recivedData["offset"])
        res=""
        try:
            res = writechunk(chunkHandle, data, offset, recivedData["listOfRecipients"])
        except:
            res = "writeFailed"

        response = {
            "response":res
        }
        response = json.dumps(response)
        response = response.encode("utf-8")
        currSock.send(str(len(response)).encode("utf-8").ljust(10))
        currSock.sendall(response)
    elif operation == "get_size":
        chunkHandle = recivedData["chunkHandle"]
        try:
            print(f"Getting size of file '{chunkHandle}'")
            # outside current directory so need to specify path
            with open(str(chunkHandle), 'r', encoding="utf-8") as file:
                contents = file.read()
                size = len(contents)  # Number of characters in the file
                print(f"Size of file '{chunkHandle}': {size}")

            response = {
                "response": "sizeSuccess",
                "size": size
            }
            response = json.dumps(response)
            response = response.encode("utf-8")
            currSock.send(str(len(response)).encode("utf-8").ljust(10))
            currSock.sendall(response)
            print(f"Size of file '{chunkHandle}' sent to client")
        except Exception as e:
            response = {
                "response": "sizeFailed",
                "error": str(e)
            }
            response = json.dumps(response)
            response = response.encode("utf-8")
            currSock.send(str(len(response)).encode("utf-8").ljust(10))
            currSock.sendall(response)

    elif operation == "replicate":
        chunkHandle = recivedData["chunkHandle"]
        listOfRecipients = recivedData["listOfRecipients"]
        try:
            with open(str(chunkHandle), "r") as f:
                chunkData = f.read()
                uploadChunk(chunkHandle, chunkData, listOfRecipients)

            response = {
                "response": "replicateSuccess"
            }
            response = json.dumps(response)
            response = response.encode("utf-8")
            currSock.send(str(len(response)).encode("utf-8").ljust(10))
            currSock.sendall(response)
        except Exception as e:
            print(f"Error occurred: {e}")
            response = {
                "response": "replicateFailed",
                "error": str(e)
            }
            response = json.dumps(response)
            response = response.encode("utf-8")
            currSock.send(str(len(response)).encode("utf-8").ljust(10))
            currSock.sendall(response)

    elif operation == "delete":
        print("Deleting chunk")
        # global listOfChunks
        # global lock
        chunkHandle = recivedData["chunkHandle"]
        try:
            os.remove(str(chunkHandle))
            with lock:
                listOfChunks.remove(str(chunkHandle))
            response = {
                "response": "deleteSuccess"
            }
            response = json.dumps(response)
            response = response.encode("utf-8")
            currSock.send(str(len(response)).encode("utf-8").ljust(10))
            currSock.sendall(response)
        except Exception as e:
            response = {
                "response": "deleteFailed",
                "error": str(e)
            }
            response = json.dumps(response)
            response = response.encode("utf-8")
            currSock.send(str(len(response)).encode("utf-8").ljust(10))
            currSock.sendall(response)

    currSock.close()


def clientThread(clientSocket):
    global listOfChunks
    while True:
        client, addr = clientSocket.accept()
        print("Connection from: " + str(addr))
        clientOperation(client)
        # messageRecieved = client.recv(1024)
        # messageRecieved = messageRecieved.decode()
        # print("Message Recieved: " + messageRecieved)
        # messageRecieved=json.loads(messageRecieved)
        # operation=messageRecieved["operation"]
        # print("Operation: "+operation)

        # if operation=="upload":
        #     chunkHandle = messageRecieved["fileName"]
        #     print("chunkHandle: " + chunkHandle)
        #     listOfChunks.append(chunkHandle)

        # client.send("Hello from chunk server".encode())


if __name__ == "__main__":
    clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientSocket.bind(("127.0.0.1", 0))
    clientSocket.listen(100)
    listenPort = clientSocket.getsockname()[1]
    pingThread = threading.Thread(target=sendPing, args=("127.0.0.1",listenPort))
    pingThread.start()

    cltThread = threading.Thread(target=clientThread, args=(clientSocket,))
    cltThread.start()
    pingThread.join()
    cltThread.join()
