# pip install paramiko
import paramiko


sourcePath =''
destinationPath = ''
#Client1
c1 = paramiko.SSHClient()
c1.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c1.connect("xxx.xxx.xxx.xxx",22,username=xxx,password='',timeout=4)

sftp = c1.open_sftp()
sftp.put(sourcePath, destinationPath)

#Client2
c2 = paramiko.SSHClient()
c2.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c2.connect("xxx.xxx.xxx.xxx",22,username=xxx,password='',timeout=4)

sftp = c2.open_sftp()
sftp.put(sourcePath, destinationPath)

#client3
c3 = paramiko.SSHClient()
c3.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c3.connect("xxx.xxx.xxx.xxx",22,username=xxx,password='',timeout=4)

sftp = c3.open_sftp()
sftp.put(sourcePath, destinationPath)

#client4
c4 = paramiko.SSHClient()
c4.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c4.connect("xxx.xxx.xxx.xxx",22,username=xxx,password='',timeout=4)

sftp = c4.open_sftp()
sftp.put(sourcePath, destinationPath)