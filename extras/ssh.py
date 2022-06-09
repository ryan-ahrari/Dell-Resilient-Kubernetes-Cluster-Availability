import paramiko
import time


def get_connection(ip, log, paswd):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip, username=log, password=paswd, timeout=100)
    console = ssh.invoke_shell()
    console.keep_this = ssh
    print(console)
    return console

def move_data_dir():

    # Source info
    ip_source = "192.168.7.164"
    user_source = "generic"
    pass_source = "generic"

    # Dest info
    ip_dest = "192.168.7.163"
    user_dest = user_source  # Dest/Source have same username
    pass_dest = pass_source  # Dest/Source have same password

    tar_file = "etcd.tar.gz" # Name of tar file

    commands = [
            "sudo su",
            pass_source,
            "cd /root",
            "cd data", # Change this line and line below if etcd mounted somewhere else
            "cd myss",
            "tar -cvzf "+tar_file+" .",
            "sudo scp "+tar_file+" "+user_dest+"@"+ip_dest+":/home/"+user_dest,
            pass_dest,
            "ssh "+user_dest+"@"+ip_dest,
            pass_dest,
            "sudo su",
            pass_dest,
            "cd /root",
            "cd data", # Change this line and line below if you want to mount etcd somewhere else
            "cd myss",
            "mv /home/"+user_dest+"/"+tar_file+" .",
            "tar -xvf "+tar_file,
            "rm -r "+tar_file,
            "exit",
            "exit",
            "rm -r "+tar_file
            ]

    channel = get_connection(ip_source, user_source, user_dest)
    index = 0 # Index to put in new commands

    for command in commands:

        channel.send(command+"\n")
        while not channel.recv_ready():
            time.sleep(0.01)
        time.sleep(0.5)
        output = channel.recv(9999)
        output = output.decode('utf-8')

        print('Output: ', output)

        # Handle creating new dir if necessary
        if "No such file or directory" in output:
            clist = command.split(" ")
            commands.insert(index+1, ("mkdir "+clist[1]))
            commands.insert(index+2, command)

        time.sleep(0.5)
        index += 1
    channel.close()

move_data_dir()
