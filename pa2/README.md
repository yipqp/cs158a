# pa2: Leader Election

This program runs the leader election algorithm with an asynchronous non-anonymous ring using the O(n^2) algorithm. 

## How to run

To run the program, `cd` into `pa2/` and run `python myleprocess.py {config}` where `{config}` is the name of the config file to be used in the program. If no config is provided, the default is `config.txt`. 

### Config file

The config file contains exactly two lines. For example: 

```
127.0.0.1,65434
127.0.0.1,65435
```

The first line is the IP address and port number of the server, which another student will connect to. The second line is the IP address and port number to connect to as a client (provided by another student).

## Execution Examples

This is an example of running the leader election algorithm in a ring of 3 processes.

### Running the program
<img width="371" height="760" alt="image" src="https://github.com/user-attachments/assets/ae4df5c9-ddfa-4d68-a8ef-0eeedb90a5f0" />

### Output of log files
<img width="664" height="800" alt="image" src="https://github.com/user-attachments/assets/32ea0ba3-033d-40da-9faf-01c2719431de" />



