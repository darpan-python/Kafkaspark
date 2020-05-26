import socket
import selectors
from time import sleep
def socketIntializer(host, post):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        return s
    except Exception as msg:
        print("connection is still open " + str(msg))
        s.close()

def preparingsocket(s):
    try:
        s.bind(('', port))
        print("socket binded")
        # put the socket into listening mode
        s.listen(2)
        print("socket is listening")
        c, addr = s.accept()
        return [c, addr]
    except Exception as e:
        # Close the connection with the client
        print("Error occurred while creating an error " + str(e))
        s.close()
        c.close()


def sendingdataToSocket(c):
    while True:
        try:
            msg1 = "Hello"
            # send a thank you message to the client.
            c.send(msg1.encode())
            print(" Received message is '{}'".format(msg1))
            msg = c.recv(1024).decode()
            print(" Received message is '{}'".format(msg))
        finally:
            # Close the connection with the client
            print("Message sent ")
            c.close()
            break

def readingfromsocket(s: socket):
    try:
        s.connect((host, port))
        while True:
            data = s.recv(255)
            print(data)
    except Exception as e:
        print("error occurred " + str(e))
        pass
    finally:
        s.close()

def readingdataFromCSV():
    import csv

    s = preparingsocket(socketIntializer(host, port))[0]
    with open(r"/home/darpan/spark-2.4.0-bin-hadoop2.7/python/sales_info.csv", 'r') as CSVFile:
        csvreader = csv.reader(CSVFile)
        linecount = 0
        for row in CSVFile:
            if linecount == 0:
                linecount = linecount + 1
                continue
            else:
                test = str(map(lambda x: str.join(x), row))
                s.send(str(row).encode())
                sleep(5)
                print(linecount)
                linecount = linecount + 1
                pass



if __name__ == "__main__":
    sel = selectors.DefaultSelector()
    host = 'localhost'
    port = 12345

    #sendingdataToSocket(preparingsocket(socketIntializer(host, port))[0])
    #readingfromsocket(socketIntializer(host, port))

    readingdataFromCSV()

