import os
import socket
import threading
import json
import uuid
import sys

class Message:
    def __init__(self):
        self.uuid = uuid.uuid4()
        self.flag = 0

        # leader_id isn't set until leader found
        self.leader_id: uuid.UUID | None = None

    # convert this class to JSON
    def toJSON(self):
        msg_json = {
                "uuid": str(self.uuid),
                "flag": self.flag
               }
        
        # when flag is 1, leader_id has been set
        if self.flag == 1:
            msg_json["leader_id"] = str(self.leader_id)
        return msg_json

# encode message to send through socket
def encode(message):
    return json.dumps(message.toJSON()).encode()

# decode JSON to Message object
def decode(msg_json):
    msg = Message()
    msg.uuid = uuid.UUID(msg_json["uuid"])
    msg.flag = msg_json["flag"]
    if msg_json["flag"] == 1:
        msg.leader_id = uuid.UUID(msg_json["leader_id"])
    return msg

# read config.txt and return dictionary of:
# server: (host, port)
# client: (host, port)
def read_config(filename):
    config = {}
    with open(filename, "r") as f:
        lines = f.readlines()
        server_sock = lines[0].strip().split(",")
        config["server"] = (server_sock[0], int(server_sock[1]))
        client_sock = lines[1].strip().split(",")
        config["client"] = (client_sock[0], int(client_sock[1]))
    return config

# get name of next log file to write and append to
# start at log1.txt, if it already exists then try log2.txt, etc.
def get_log_filename():
    log_count = 1
    while True:
        filename = f'log{log_count}.txt'
        try:
            # create file if it doesn't exist
            # fail if exists to prevent multiple processes opening same file
            fd = os.open(filename, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
            return filename
        except FileExistsError:
            log_count += 1  # file already exists so just increment log count

# append message to file
# automatically concat newline char to msg
def write_log(filename, msg):
    with open(filename, 'a') as f:
        f.write(msg + "\n")

# function to use in server thread
def accept_client(server_sock, conn_info):
    conn, addr = server_sock.accept()
    conn_info["conn"] = conn
    conn_info["addr"] = addr

# open connections to two neighbors in async non-anonymous ring
def main():

    # read config file, defaulting to config.txt if no arguments passed in terminal
    config_filename = "config.txt"
    if len(sys.argv) > 1:
        config_filename = sys.argv[1]
    config = read_config(config_filename)
    
    # extract server/client host and port numbers
    SERVER_HOST, SERVER_PORT = config["server"]
    CLIENT_HOST, CLIENT_PORT = config["client"]

    BUFFER_SIZE = 1024

    # set up server
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((SERVER_HOST, SERVER_PORT))
    server_sock.listen()
    print(f"[Server] Waiting for connection at port {SERVER_PORT}")

    # accept connection for server in new thread
    # store connection socket in conn_info
    conn_info = {}
    t = threading.Thread(
        target=accept_client,
        args=(server_sock, conn_info),
        daemon=True,
    )
    t.start()

    # don't connect until circle is ready
    input("press Enter when everyone is ready.")

    # connect as client in main thread
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_sock:
        client_sock.connect((CLIENT_HOST, CLIENT_PORT))
        print(f"[Client] Connected to {CLIENT_HOST}:{CLIENT_PORT}")

        # wait for server thread to finish so that we have correct conn_info
        t.join()

        my_message = Message()

        # get log filename to write to for the remainder of this process
        log_filename = get_log_filename()

        write_log(log_filename, f"New process started, uuid={my_message.uuid}")

        # send id in first round
        client_sock.sendall(encode(my_message))
        write_log(log_filename, f"Sent: {my_message.uuid}, flag={my_message.flag}")

        conn = conn_info["conn"]
        buf = ""
        leader_found = False

        while not leader_found:

            # keep receiving data until "}" found, in which case 
            # we have complete message to parse
            data = conn.recv(BUFFER_SIZE)
            if not data: 
                break
            buf += data.decode()
            
            # parse the current message(s)
            while "}" in buf:
                complete_res, buf = buf.split("}", 1)
                complete_res += "}"

                # create Message object from JSON serialized Message
                res_as_message = decode(json.loads(complete_res))

                # leader has been elected
                if res_as_message.flag == 1:

                    # notify others who leader is
                    # leader doesn't need to notify since they already have
                    if my_message.uuid != res_as_message.uuid:
                        write_log(log_filename, f"Leader is decided to {res_as_message.leader_id}.")
                        client_sock.sendall(encode(res_as_message))
                        write_log(log_filename, f"Sent: {res_as_message.uuid}, flag={res_as_message.flag}")
                    leader_found = True
                    break

                # incoming message is less than current, ignore it
                if my_message.uuid > res_as_message.uuid:
                    write_log(log_filename, f"Received: {res_as_message.uuid}, flag={res_as_message.flag}, less, 0. [Message ignored]")

                # incoming message is greater than current, forward it
                elif my_message.uuid < res_as_message.uuid:
                    write_log(log_filename, f"Received: {res_as_message.uuid}, flag={res_as_message.flag}, greater, 0")
                    client_sock.sendall(encode(res_as_message))
                    write_log(log_filename, f"Sent: {res_as_message.uuid}, flag={res_as_message.flag}")

                # incoming message is equal to current, this process is the leader
                # set flag, leader_id, and tell others who the leader is
                else:
                    my_message.flag = 1
                    my_message.leader_id = my_message.uuid
                    client_sock.sendall(encode(my_message))
                    write_log(log_filename, f"Leader decided. Sent: {my_message.uuid}, flag={my_message.flag}")
        
    # close server sockets
    # client socket is already closed
    conn = conn_info["conn"]
    conn.close()
    server_sock.close()

if __name__ == "__main__":
    main()
