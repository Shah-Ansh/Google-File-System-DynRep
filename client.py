import os
import socket
import json
import base64
def load_env_file():
    with open(".env") as file:
        for line in file:
            if line.startswith("#") or not line.strip():
                continue
            key, value = line.strip().split("=", 1)
            os.environ[key] = value

load_env_file()

serverclientPort = str(os.environ["SERVER_CLIENT_PORT"])

def uploadChunkToChunkServer(chunkServerPort,chunkServerAddr,chunkData,listofRecipients,chunkHandle):
    currSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    currSock.connect((chunkServerAddr, int(chunkServerPort)))
    print(type(chunkData))
    if type(chunkData) == bytes:
        chunkData = chunkData.decode("utf-8")
    # chunkData = chunkData.decode("utf-8")
    print(type(chunkData))
    toSend = {
        "operation": "upload",
        "chunkData":chunkData,
        "chunkHandle": chunkHandle,
        "listofRecipients": listofRecipients,
    }

    toSend = json.dumps(toSend)
    toSend = toSend.encode("utf-8")
    lenToSend = len(toSend)
    currSock.send(str(lenToSend).encode("utf-8").ljust(10))
    currSock.sendall(toSend)
    currSock.close()


def readFromChunk(start,end,chunkHandle):
    print(start,end)
    
    currSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    currSock.connect(("127.0.0.1",int(serverclientPort)))
    dataToSend ={
        "operation":"locateChunk",
        "chunkHandle":chunkHandle,
    }
    dataToSend = json.dumps(dataToSend)
    dataToSend = dataToSend.encode("utf-8")
    currSock.send(str(len(dataToSend)).encode("utf-8").ljust(10))
    currSock.sendall(dataToSend)
    message = currSock.recv(10)
    numberOfBytes = message.decode("utf-8").strip()
    numberOfBytes = int(numberOfBytes)
    dataReceived = bytearray()
    while len(dataReceived) < numberOfBytes:
        temp = currSock.recv(min(numberOfBytes - len(dataReceived), 2048))
        if temp:
            dataReceived.extend(temp)
        else:
            raise ConnectionError("Connection closed before all data recieved")
    message = dataReceived
    message = json.loads(message)
    print(message)
    currSock.close()

    flag=0
    for currchunkserver in message["chunkServers"]:
        try:
            currSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            currSock.connect((currchunkserver[0],currchunkserver[1]))
            dataToSend = {
                "operation":"read",
                "chunkHandle":chunkHandle,
                "start":start,
                "end":end
            }
            dataToSend = json.dumps(dataToSend)
            dataToSend = dataToSend.encode("utf-8")
            currSock.send(str(len(dataToSend)).encode("utf-8").ljust(10))
            currSock.sendall(dataToSend)
            message = currSock.recv(10)
            numberOfBytes = message.decode("utf-8").strip()
            numberOfBytes = int(numberOfBytes)
            dataReceived = bytearray()
            while len(dataReceived) < numberOfBytes:
                temp = currSock.recv(min(numberOfBytes - len(dataReceived), 2048))
                if temp:
                    dataReceived.extend(temp)
                else:
                    raise ConnectionError("Connection closed before all data recieved")
            message = dataReceived
            print(message.decode("utf-8"))
            currSock.close()
            message = json.loads(message)
            if message["response"] == "readSuccess":
                flag=1
                return message["data"]
        except:
            continue


    return "failure"

def readHelper(offset,readSize,listOfChunkHandles,chunkSize):
    start = offset
    end = offset + readSize
    ans = ""
    for i in range(0,len(listOfChunkHandles)):
        currStart = i*chunkSize
        currEnd = (i+1)*chunkSize
        print(i,currStart,currEnd,sep=" ")
        if currStart >= end:
            continue
        if currEnd <= start:
            continue
        toStart = max(start,currStart) - currStart
        toEnd = min(end,currEnd) - currStart
        amountToRead = toEnd - toStart
        res = readFromChunk(toStart, amountToRead, listOfChunkHandles[i])
        if res == "failure":
            print("Error in reading")
            return
        ans += res

    print(ans)


def writeHelper(offset,dataToWrite,chunk,listOfRecipients):
    currSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    currSock.connect((listOfRecipients[0][0],listOfRecipients[0][1]))
    dataToSend = {
        "operation":"write",
        "chunkHandle":chunk,
        "data":dataToWrite,
        "offset":offset,
        "listOfRecipients":listOfRecipients
    }
    dataToSend = json.dumps(dataToSend)
    dataToSend = dataToSend.encode("utf-8")
    currSock.send(str(len(dataToSend)).encode("utf-8").ljust(10))
    currSock.sendall(dataToSend)
    message = currSock.recv(10)
    numberOfBytes = message.decode("utf-8").strip()
    numberOfBytes = int(numberOfBytes)
    dataReceived = bytearray()
    while len(dataReceived) < numberOfBytes:
        temp = currSock.recv(min(numberOfBytes - len(dataReceived), 2048))
        if temp:
            dataReceived.extend(temp)
        else:
            raise ConnectionError("Connection closed before all data recieved")
    message = dataReceived
    print(message.decode("utf-8"))
    currSock.close()
    print(message)
    pass


def handleWrite(offset,dataToWrite,chunkHandles,chunkSize,fileName):
    start = offset
    if offset >= len(chunkHandles)*chunkSize:
        print("Offset out of bounds")
        return
    currChunkIdx = start//chunkSize
    remDataLen = len(dataToWrite)
    while remDataLen>0:
        # currChunk = listOfChunks[startChunkidx]
        if currChunkIdx  < len(chunkHandles):
            currChunkHandle = chunkHandles[currChunkIdx]
            curroffset=0
            if (currChunkIdx==start//chunkSize):
                curroffset=start%chunkSize
            dataToSend = {
                "operation":"locateChunk",
                "chunkHandle":currChunkHandle
            }
            dataToSend = json.dumps(dataToSend)
            dataToSend = dataToSend.encode("utf-8")
            currSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            currSock.connect(("127.0.0.1",int(serverclientPort)))
            currSock.send(str(len(dataToSend)).encode("utf-8").ljust(10))
            currSock.sendall(dataToSend)
            message = currSock.recv(10)
            numberOfBytes = message.decode("utf-8").strip()
            numberOfBytes = int(numberOfBytes)
            dataReceived = bytearray()
            while len(dataReceived) < numberOfBytes:
                temp = currSock.recv(min(numberOfBytes - len(dataReceived), 2048))
                if temp:
                    dataReceived.extend(temp)
                else:
                    raise ConnectionError("Connection closed before all data recieved")
            message = dataReceived
            message = json.loads(message)
            
            currSock.close()
            lenOfData = min(remDataLen,chunkSize-curroffset)
            dataToWriteCurr = dataToWrite[:lenOfData]
            dataToWrite = dataToWrite[lenOfData:]
            remDataLen -= lenOfData
            writeHelper(curroffset,dataToWriteCurr,currChunkHandle,message["chunkServers"])
            currChunkIdx += 1
        else:
            currSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            currSock.connect(("127.0.0.1",int(serverclientPort)))
            dataToSend = {
                "operation":"getnextnewChunkHandle",
                "fileName": fileName
            }
            dataToSend = json.dumps(dataToSend)
            dataToSend = dataToSend.encode("utf-8")
            currSock.send(str(len(dataToSend)).encode("utf-8").ljust(10))
            currSock.sendall(dataToSend)
            message = currSock.recv(10)
            numberOfBytes = message.decode("utf-8").strip()
            numberOfBytes = int(numberOfBytes)
            dataReceived = bytearray()
            while len(dataReceived) < numberOfBytes:
                temp = currSock.recv(min(numberOfBytes - len(dataReceived), 2048))
                if temp:
                    dataReceived.extend(temp)
                else:
                    raise ConnectionError("Connection closed before all data recieved")
            message = dataReceived
            message = json.loads(message)
            currChunkHandle = message["chunkHandle"]
            currChunkIdx += 1
            listofRecipients=message["listAssigned"]
            curroffset=0
            if (currChunkIdx==start//chunkSize):
                curroffset=start%chunkSize
            lenOfData = min(remDataLen,chunkSize-curroffset)
            dataToWriteCurr = dataToWrite[:lenOfData]
            dataToWrite = dataToWrite[lenOfData:]
            remDataLen -= lenOfData
            # writeHelper(curroffset,dataToWriteCurr,currChunkHandle,listofRecipients)
            uploadChunkToChunkServer(listofRecipients[0][1],listofRecipients[0][0],dataToWriteCurr,listofRecipients,currChunkHandle)
            currSock.close()


        pass

    pass



def get_size_from_chunk(chunkSize, chunkHandle, chunkServers):
    currSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    currSock.connect((str(chunkServers[0][0]), int(chunkServers[0][1])))
    dataToSend = {
        "operation":"get_size",
        "chunkHandle":chunkHandle
    }
    dataToSend = json.dumps(dataToSend)
    dataToSend = dataToSend.encode("utf-8")
    currSock.send(str(len(dataToSend)).encode("utf-8").ljust(10))
    currSock.sendall(dataToSend)
    message = currSock.recv(10)
    numberOfBytes = message.decode("utf-8").strip()
    numberOfBytes = int(numberOfBytes)
    dataReceived = bytearray()
    while len(dataReceived) < numberOfBytes:
        temp = currSock.recv(min(numberOfBytes - len(dataReceived), 2048))
        if temp:
            dataReceived.extend(temp)
        else:
            raise ConnectionError("Connection closed before all data recieved")
    message = dataReceived
    message = json.loads(message)
    currSock.close()
    print(message)
    return message["size"]

def get_offset(fileName, chunkSize, chunkHandles):

    offset = 0
    for i in range(len(chunkHandles)):
        currChunkHandle = chunkHandles[i]
        currSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        currSock.connect(("127.0.0.1",int(serverclientPort)))
        dataToSend = {
            "operation":"locateChunk",
            "chunkHandle":currChunkHandle
        }
        dataToSend = json.dumps(dataToSend)
        dataToSend = dataToSend.encode("utf-8")
        currSock.send(str(len(dataToSend)).encode("utf-8").ljust(10))
        currSock.sendall(dataToSend)
        message = currSock.recv(10)
        numberOfBytes = message.decode("utf-8").strip()
        numberOfBytes = int(numberOfBytes)
        dataReceived = bytearray()
        while len(dataReceived) < numberOfBytes:
            temp = currSock.recv(min(numberOfBytes - len(dataReceived), 2048))
            if temp:
                dataReceived.extend(temp)
            else:
                raise ConnectionError("Connection closed before all data recieved")
        message = dataReceived
        message = json.loads(message)
        currSock.close()
        offset += get_size_from_chunk(chunkSize, chunkHandles[i],message["chunkServers"])
        print(offset)
    return offset
    

def get_file_size(fileName):
    # get chunk handels form master server
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("localhost", int(serverclientPort)))
    dataToSend = {
            "operation": "write_append", 
            "fileName": fileName, 
        }
    dataToSend = json.dumps(dataToSend)
    dataToSend = dataToSend.encode("utf-8")
    sock.send(str(len(dataToSend)).encode("utf-8").ljust(10))
    sock.sendall(dataToSend)
    message = sock.recv(10)
    numberOfBytes = message.decode("utf-8").strip()
    numberOfBytes = int(numberOfBytes)
    dataReceived = bytearray()
    while len(dataReceived) < numberOfBytes:
        temp = sock.recv(min(numberOfBytes - len(dataReceived), 2048))
        if temp:
            dataReceived.extend(temp)
        else:
            raise ConnectionError("Connection closed before all data recieved")
    message = dataReceived
    print(message.decode("utf-8"))
    message = json.loads(message)
    if message["response"] == "fileFound":
        print("File found")
        chunkHandles = message["chunks"]
        chunkSize = int(os.getenv("CHUNK_SIZE"))
        return get_offset(fileName, chunkSize, chunkHandles)
    else:
        print("File not found")
        return -1


while True:
    ch = int(input("1. Read File\n2.Upload File\n3.Write\n4.Exit\nEnter choice: "))
    # print(ch)
    # if ch==5:
    #     import time
    #     t= float(input("Enter time to sleep: "))
    #     time.sleep(t)
    #     continue
    if ch == 4:
        break
    if ch != 1 and ch != 2 and ch != 3:
        print("Invalid choice")
        continue
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("localhost", int(serverclientPort)))
    if ch == 1:
        fileName=input("Enter file name: ")
        # print(fileName)
        offset = int(input("Enter file offset: "))
        # print(offset)
        readSize = int(input("Enter read size: "))
        # print(readSize)
        dataToSend = {
            "operation": "read", 
            "fileName": fileName, 
            # "offset": offset, 
            # "readSize": readSize
        }
        dataToSend = json.dumps(dataToSend)
        dataToSend = dataToSend.encode("utf-8")
        sock.send(str(len(dataToSend)).encode("utf-8").ljust(10))
        sock.sendall(dataToSend)
        message = sock.recv(10)
        numberOfBytes = message.decode("utf-8").strip()
        numberOfBytes = int(numberOfBytes)
        dataReceived = bytearray()
        while len(dataReceived) < numberOfBytes:
            temp = sock.recv(min(numberOfBytes - len(dataReceived), 2048))
            if temp:
                dataReceived.extend(temp)
            else:
                raise ConnectionError("Connection closed before all data recieved")
        message = dataReceived
        print(message.decode("utf-8"))
        message = json.loads(message)
        if message["response"] == "fileFound":
            print("File found")
            chunkHandles = message["chunks"]
            # print(chunkHandles)
            chunkSize = int(os.getenv("CHUNK_SIZE"))
            readHelper(offset,readSize,chunkHandles,chunkSize)
        else:
            print("File not found")
    elif ch == 2:
        filepath = input("Enter file path: ")
        # print(filepath)
        fileName = input("Enter file name to save as: ")
        # print(fileName)
        listOfChunks = []
        chunksize = os.getenv("CHUNK_SIZE")
        try:
            with open(filepath, "rb") as file:
                chunk = file.read(int(chunksize))
                while chunk:
                    listOfChunks.append(chunk)
                    chunk = file.read(int(chunksize))
        except Exception as e:
            print(e)
            print("Error in reading file")
            continue
        dataToSend = {"operation": "upload", "fileName": fileName,"filesize":os.path.getsize(filepath)}  
        dataToSend = json.dumps(dataToSend)
        dataToSend = dataToSend.encode("utf-8")
        sock.send(str(len(dataToSend)).encode("utf-8").ljust(10))
        sock.sendall(dataToSend)
        message = sock.recv(10)
        numberOfBytes = message.decode("utf-8").strip()
        numberOfBytes = int(numberOfBytes)
        dataReceived = bytearray()
        while len(dataReceived) < numberOfBytes:
            temp = sock.recv(min(numberOfBytes - len(dataReceived), 2048))
            if temp:
                dataReceived.extend(temp)
            else:
                raise ConnectionError("Connection closed before all data recieved")
        message = dataReceived
        print(message.decode("utf-8"))
        
        message = json.loads(message)
        sortedKeys = sorted(message.keys())
        for ind in range(len(sortedKeys)):
            chunkData = listOfChunks[ind]
            chunkHandle = sortedKeys[ind]
            listofRecipients = message[str(chunkHandle)]
            firstRecipient = listofRecipients[0]
            uploadChunkToChunkServer(firstRecipient[1],firstRecipient[0],chunkData,listofRecipients,chunkHandle)
    elif ch==3:
        print("Write")
        write_app = input("Enter 1 for write and 2 for write append: ")
        fileName=input("Enter file name: ")
        if write_app == "1":
            offset=int(input("Enter offset: "))
        else:
            offset = get_file_size(fileName)
            print("Offset: ", offset)
        data = input("Enter data: ")
        dataToSend ={
            "operation":"write",
            "fileName":fileName,
        }
        dataToSend = json.dumps(dataToSend)
        dataToSend = dataToSend.encode("utf-8")
        sock.send(str(len(dataToSend)).encode("utf-8").ljust(10))
        sock.sendall(dataToSend)
        message = sock.recv(10)
        numberOfBytes = message.decode("utf-8").strip()
        numberOfBytes = int(numberOfBytes)
        dataReceived = bytearray()
        while len(dataReceived) < numberOfBytes:
            temp = sock.recv(min(numberOfBytes - len(dataReceived), 2048))
            if temp:
                dataReceived.extend(temp)
            else:
                raise ConnectionError("Connection closed before all data recieved")
        message = dataReceived
        print(message.decode("utf-8"))
        message = json.loads(message)
        if message["response"] == "fileFound":
            print("File found")
            chunkHandles = message["chunks"]
            
            # print(chunkHandles)
            chunkSize = int(os.getenv("CHUNK_SIZE"))
            if offset == -1:
                offset = len(chunkHandles)*chunkSize
            handleWrite(offset,data,chunkHandles,chunkSize,fileName)

        else:
            print("File not found")
    sock.close()
