import socket
import threading
import os
import math
import json
import time
import os
import random
from collections import deque

### Global Variables
chunkToChunkServers = {}
listOfChunkServers = []
nextnewChunkHandle=0
fileToChunks = {}
chunkAccesCt = {}
lastFifteenSeconds={}
chunkLoadRate = {}
prevChunkCalc = {}
mappingOfChunkserverload = {}
prevChunkserverPing = {}
lowerBoundOfRequests=2
upperBoundOfRequests=15

###
lock = threading.Lock()
newLock = threading.Lock()
# Function to load environment variables from .env file
def load_env_file(filepath=".env"):
    with open(filepath) as file:
        for line in file:
            # Ignore comments and empty lines
            if line.startswith("#") or not line.strip():
                continue
            # Parse key-value pair
            key, value = line.strip().split("=", 1)
            os.environ[key] = value  # Set as an environment variable


# Load the .env file

def writeChunkLoadToFile():
    global chunkAccesCt
    global chunkToChunkServers
    global lastFifteenSeconds
    while True:
        time.sleep(1)
        with open("chunkLoad.txt","a") as f:
            for key in lastFifteenSeconds.keys():
                f.write(f"{lastFifteenSeconds[key][0]},{len(chunkToChunkServers[str(key)])}\n")

    pass


def remove_dead_chunkserver(chunkserver):
    """Remove a dead chunk server and update metadata"""
    global chunkToChunkServers, listOfChunkServers, prevChunkserverPing

    # with lock:
    if chunkserver in listOfChunkServers:
        listOfChunkServers.remove(chunkserver)

    # Remove from last ping tracking
    if chunkserver in prevChunkserverPing:
        del prevChunkserverPing[chunkserver]

    # Remove from chunk to chunk servers mapping
    for chunk_handle in list(chunkToChunkServers.keys()):
        if chunkserver in chunkToChunkServers[chunk_handle]:
            chunkToChunkServers[chunk_handle].remove(chunkserver)

    print(f"Chunk server {chunkserver} marked as dead and removed from metadata")


def check_chunk_servers():
    """Periodically check for dead chunk servers"""
    while True:
        current_time = time.time()
        dead_servers = []

        # with lock:
        for chunkserver, last_ping in prevChunkserverPing.items():
            if current_time - last_ping > 7:
                dead_servers.append(chunkserver)

        # Remove dead servers
        for server in dead_servers:
            remove_dead_chunkserver(server)

        time.sleep(12)  


def increaseChunkReplica(chunHandle,numReplica):
    # print(f"increaseChunkReplica{numReplica}")
    global chunkToChunkServers
    global listOfChunkServers
    ct = 0
    with lock:
        ct  = len(chunkToChunkServers[str(chunHandle)])
    
    numReplica = min(numReplica, len(listOfChunkServers) - ct)

    if numReplica == 0:
        return
    
    randomChunkServers = []
    chunkserversAccordingToLoad = sorted(mappingOfChunkserverload.items(), key=lambda x: x[1])
    for i in range(numReplica):
        randomChunkServers.append(chunkserversAccordingToLoad[i][0])
    
    
    dataToSend = {
        "operation": "replicate",
        "chunkHandle": chunHandle,
        "listOfRecipients": randomChunkServers
    }
    dataToSend = json.dumps(dataToSend)
    dataToSend = dataToSend.encode("utf-8")
    lenToSend = len(dataToSend)
    currChunkServerWithChunk = chunkToChunkServers[str(chunHandle)][0]
    currSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    currSock.connect((currChunkServerWithChunk[0], currChunkServerWithChunk[1]))
    currSock.sendall(str(lenToSend).encode("utf-8").ljust(10))
    currSock.sendall(dataToSend)
    lenToRecieve = currSock.recv(10)
    lenToRecieve = lenToRecieve.decode("utf-8").strip()
    lenToRecieve = int(lenToRecieve)
    messageRecieved = bytearray()
    while len(messageRecieved) < lenToRecieve:
        toRecieveLength = min(lenToRecieve - len(messageRecieved), 2048)
        messageRecieved.extend(currSock.recv(toRecieveLength))
    messageRecieved = messageRecieved.decode("utf-8")
    messageRecieved = json.loads(messageRecieved)
    # print("Message Recieved: ", messageRecieved)
    currSock.close()


def decreaseChunkReplica(chunHandle,numReplica):
    global chunkToChunkServers
    global listOfChunkServers
    currNumReplica = 0
    with lock:
        currNumReplica = len(chunkToChunkServers[str(chunHandle)])
    # with open("log.txt","a") as f:
    #     f.write(f"ChunkHandle: {chunHandle} NumReplica: {currNumReplica}\n")
    if currNumReplica<=2:
        return
    numReplica = min(numReplica, currNumReplica - 2)
    print(currNumReplica,numReplica)
    if numReplica <= 0:
        return
    randomChunkServers = []

    randomChunkServers =[]
    chunkserversAccordingToLoad = sorted(mappingOfChunkserverload.items(), key=lambda x: x[1],reverse=True)
    for i in range(numReplica):
        randomChunkServers.append(chunkserversAccordingToLoad[i][0])

    for currChunkServerWithChunk in randomChunkServers:
        dataToSend = {
            "operation": "delete",
            "chunkHandle": chunHandle,
        }
        dataToSend = json.dumps(dataToSend)
        dataToSend = dataToSend.encode("utf-8")
        lenToSend = len(dataToSend)
        currSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        currSock.connect((currChunkServerWithChunk[0], currChunkServerWithChunk[1]))
        currSock.sendall(str(lenToSend).encode("utf-8").ljust(10))
        currSock.sendall(dataToSend)
        lenToRecieve = currSock.recv(10)
        lenToRecieve = lenToRecieve.decode("utf-8").strip()
        lenToRecieve = int(lenToRecieve)
        messageRecieved = bytearray()
        while len(messageRecieved) < lenToRecieve:
            toRecieveLength = min(lenToRecieve - len(messageRecieved), 2048)
            messageRecieved.extend(currSock.recv(toRecieveLength))
        messageRecieved = messageRecieved.decode("utf-8")
        messageRecieved = json.loads(messageRecieved)
        print("Message Recieved: ", messageRecieved)
    with lock:
        for chunkserverdeleting in randomChunkServers:
            if chunkserverdeleting in chunkToChunkServers[str(chunHandle)]:
                chunkToChunkServers[str(chunHandle)].remove(chunkserverdeleting)
        time.sleep(2)

        currSock.close()
    pass

def chunkLoadTracker():
    global chunkAccesCt
    global chunkLoadRate

    while True:
        time.sleep(15)
        for currChunk in chunkToChunkServers.keys():
            currChunk = int(currChunk)
            if currChunk not in prevChunkCalc:
                prevChunkCalc[currChunk] = time.time()
            if currChunk not in chunkAccesCt:
                chunkAccesCt[currChunk] = 0
            currCt = 0
            with newLock:
                currCt = chunkAccesCt[currChunk]
                chunkAccesCt[currChunk] = 0

            if currChunk not in lastFifteenSeconds:
                lastFifteenSeconds[currChunk] = [0,deque()]
            if len(lastFifteenSeconds[currChunk][1]) == 1:
                leftMost = lastFifteenSeconds[currChunk][1].popleft()
                lastFifteenSeconds[currChunk][0]-=leftMost
            lastFifteenSeconds[currChunk][1].append(currCt)
            lastFifteenSeconds[currChunk][0]+=currCt

            currNumReplica=len(chunkToChunkServers[str(currChunk)])
            print("currNumReplica ",currNumReplica)
            if currNumReplica < (lastFifteenSeconds[currChunk][0]//upperBoundOfRequests):
                toAdd = (lastFifteenSeconds[currChunk][0]//upperBoundOfRequests)-currNumReplica

                # threading.Thread(target = increaseChunkReplica,args=(currChunk,toAdd)).start()
                increaseChunkReplica(currChunk,toAdd)
            elif currNumReplica > (lastFifteenSeconds[currChunk][0]//lowerBoundOfRequests):
                toRem = currNumReplica - (
                    lastFifteenSeconds[currChunk][0] // lowerBoundOfRequests
                )
                # threading.Thread(target=decreaseChunkReplica,args=(currChunk,toRem)).start()
                decreaseChunkReplica(currChunk,toRem)


def chunkServerOperation(chunkServer):
    messageRecieved = chunkServer.recv(1024)
    messageRecieved = messageRecieved.decode()
    # print("Message Recieved: ",  messageRecieved)
    messageRecieved=json.loads(messageRecieved)
    # print("Message Recieved: ", messageRecieved)
    operation = messageRecieved["operation"]
    if operation == "ping":
        chunkserverListenPort = messageRecieved["chunkPort"]
        chunkserverListenAddr = messageRecieved["chunkAddr"]
        chunks = messageRecieved["chunks"]
        prevChunkserverPing[(chunkserverListenAddr, chunkserverListenPort)] = time.time()
        with lock:
            if (chunkserverListenAddr, chunkserverListenPort) not in listOfChunkServers:
                listOfChunkServers.append((chunkserverListenAddr, chunkserverListenPort))
            mappingOfChunkserverload[(chunkserverListenAddr, chunkserverListenPort)] = int(messageRecieved["load"])
            for chunkHandle in chunks:
                if chunkHandle not in chunkToChunkServers:
                    chunkToChunkServers[chunkHandle] = []
                # chunkToChunkServers[chunkHandle].append((chunkserverListenAddr, chunkserverListenPort))
                if (chunkserverListenAddr, chunkserverListenPort) not in chunkToChunkServers[chunkHandle]:
                    chunkToChunkServers[chunkHandle].append((chunkserverListenAddr, chunkserverListenPort))
            for chunkHandle in chunkToChunkServers:
                if chunkHandle not in chunks:
                    if (chunkserverListenAddr, chunkserverListenPort) in chunkToChunkServers[chunkHandle]:
                        chunkToChunkServers[chunkHandle].remove((chunkserverListenAddr, chunkserverListenPort))

    chunkServer.send("Hello from master server".encode())
def chunkServerthread(chunkServerSocket):
    while True:
        chunkServer, addr = chunkServerSocket.accept()
        # print("Connection from: " + str(addr))
        chunkServerThread = threading.Thread(target=chunkServerOperation, args=(chunkServer,))
        chunkServerThread.start()


def assignChunkToChunkServer(listOfChunkHandles):
    global listOfChunkServers
    ans={}
    for chunkHandle in listOfChunkHandles:
        ans[chunkHandle]=random.sample(listOfChunkServers, 3)
    return ans


def clientOperation(clientSocket):
    global nextnewChunkHandle
    lenOfMessage = clientSocket.recv(10)
    lenOfMessage = lenOfMessage.decode("utf-8").strip()
    lenOfMessage = int(lenOfMessage)

    messageRecieved = bytearray()
    while len(messageRecieved) < lenOfMessage:
        toRecieveLength = min(lenOfMessage - len(messageRecieved), 2048)
        messageRecieved.extend(clientSocket.recv(toRecieveLength))

    messageRecieved = messageRecieved.decode("utf-8")
    print("Message Recieved client: " + messageRecieved)
    messageRecieved=json.loads(messageRecieved)
    operation=messageRecieved["operation"]
    print("Operation: "+operation)

    if operation == "upload":
        fileName = messageRecieved["fileName"]
        print("File Name: "+fileName)
        print("File Size: ", messageRecieved["filesize"])
        chunk_size=os.environ.get("CHUNK_SIZE")
        number_of_chunks=math.ceil(int(messageRecieved["filesize"])/int(chunk_size))
        print("Number of chunks: ", number_of_chunks)

        chunkHandles=[]
        with lock:
            for i in range(number_of_chunks):
                chunkHandles.append(nextnewChunkHandle)
                nextnewChunkHandle+=1
            fileToChunks[fileName]=chunkHandles

        assigned = assignChunkToChunkServer(chunkHandles)
        assigned = json.dumps(assigned)
        assigned = assigned.encode("utf-8")
        clientSocket.sendall(str(len(assigned)).encode("utf-8").ljust(10))
        clientSocket.sendall(assigned)

    if operation == "read":
        fileName = messageRecieved["fileName"]
        print("File Name: " + fileName)
        res = ""
        if fileName not in fileToChunks:
            res = "File not found"
        else:
            res = "fileFound"

        dataToSend = {
            "response": res,
        }

        if res == "fileFound":
            chunkHandles = fileToChunks[fileName]
            dataToSend["chunks"] = chunkHandles

        dataToSend = json.dumps(dataToSend)
        dataToSend = dataToSend.encode("utf-8")
        lenToSend = len(dataToSend)
        clientSocket.sendall(str(lenToSend).encode("utf-8").ljust(10))
        clientSocket.sendall(dataToSend)

    if operation == "write_append":
        fileName = messageRecieved["fileName"]
        res = ""
        if fileName not in fileToChunks:
            res = "File not found"
        else:
            res = "fileFound"

        dataToSend = {
            "response": res,
        }

        if res == "fileFound":
            chunkHandles = fileToChunks[fileName]
            dataToSend["chunks"] = chunkHandles

        dataToSend = json.dumps(dataToSend)
        dataToSend = dataToSend.encode("utf-8")
        lenToSend = len(dataToSend)
        clientSocket.sendall(str(lenToSend).encode("utf-8").ljust(10))
        clientSocket.sendall(dataToSend)

    if operation == "locateChunk":
        res = ""
        
        chunkHandle = str(messageRecieved["chunkHandle"])
        if chunkHandle not in chunkToChunkServers:
            res = "Chunk not found"
        else:
            res = "Chunk found"
        with newLock:
            
            if int(chunkHandle) not in chunkAccesCt:
                chunkAccesCt[int(chunkHandle)] = 0
            chunkAccesCt[int(chunkHandle)] += 1
        dataToSend = {
            "response": res,
        }
        if res == "Chunk found":
            dataToSend["chunkServers"] = chunkToChunkServers[chunkHandle]
        dataToSend = json.dumps(dataToSend)
        dataToSend = dataToSend.encode("utf-8")
        lenToSend = len(dataToSend)
        clientSocket.sendall(str(lenToSend).encode("utf-8").ljust(10))
        clientSocket.sendall(dataToSend)

    if operation == "write":
        fileName = messageRecieved["fileName"]
        res = ""
        if fileName not in fileToChunks:
            res = "File not found"
        else:
            res = "fileFound"
        dataToSend = {
            "response": res,
        }
        if res == "fileFound":
            chunkHandles = fileToChunks[fileName]
            dataToSend["chunks"] = chunkHandles
        dataToSend = json.dumps(dataToSend)
        dataToSend = dataToSend.encode("utf-8")
        lenToSend = len(dataToSend)
        clientSocket.sendall(str(lenToSend).encode("utf-8").ljust(10))
        clientSocket.sendall(dataToSend)

    if operation == "getnextnewChunkHandle":
        ans=""
        fileName = messageRecieved["fileName"]
        with lock:
            ans=nextnewChunkHandle
            fileToChunks[fileName].append(ans)
            nextnewChunkHandle+=1
        fileName = messageRecieved["fileName"]
        assignedTo = assignChunkToChunkServer([ans])
        # assignedTo = json.dumps(assignedTo)

        dataToSend={
            "chunkHandle":ans,
            "listAssigned":assignedTo[ans]
        }

        dataToSend = json.dumps(dataToSend)
        dataToSend = dataToSend.encode("utf-8")
        lenToSend = len(dataToSend)
        clientSocket.sendall(str(lenToSend).encode("utf-8").ljust(10))
        clientSocket.sendall(dataToSend)

    clientSocket.close()


def listenClientThread(serverSocket):
    while True:
        clientSocket, addr = serverSocket.accept()
        print("Connection from: " + str(addr))
        clientThread = threading.Thread(target=clientOperation, args=(clientSocket,))
        clientThread.start()


if __name__ == "__main__":
    # Load the .env file
    load_env_file()

    # Get the port number from the environment variable
    serverclientport = int(os.getenv("SERVER_CLIENT_PORT"))
    serverchunkport = int(os.getenv("SERVER_CHUNKSERVER_PORT"))

    serverClientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serverClientSocket.bind(("127.0.0.1", serverclientport))
    serverClientSocket.listen(100)

    serverChunkSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serverChunkSocket.bind(("127.0.0.1", serverchunkport))
    serverChunkSocket.listen(100)
    failure_detector = threading.Thread(target=check_chunk_servers)
    failure_detector.daemon = True  # Thread will exit when main program exits
    failure_detector.start()
    t1=threading.Thread(target=listenClientThread, args=(serverClientSocket,))
    t2=threading.Thread(target=chunkServerthread, args=(serverChunkSocket,))
    t3=threading.Thread(target=chunkLoadTracker)
    t4=threading.Thread(target=writeChunkLoadToFile)
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t1.join()
    t2.join()
    t3.join()
    t4.join()

    serverClientSocket.close()
    serverChunkSocket.close()
    pass
