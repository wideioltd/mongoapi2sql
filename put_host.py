# This program puts up information about the host on the database every 10 seconds.
# If the information exists, it is updated, otherwise the database record is inserted into the table.
# The primary key is HOSTNAME. However, in the DBMS, the key is set to "id".
# Todo: In database: set HOSTNAME to unique.
# Configuation/ design decision:
#     memory size and free disk sizes are in kB.
# HostType uses a customized format.

import os
import socket

import sys
sys.path.append('/home/sohail/wsrc/mongoapi2sql/mongo')
import mongo_connector

DEBUG=0

def remove_backslashN(s):
    #Remove if there is a "\n" at end, remove it.
    #print("char=%s"%(s[-1])) #not s[:0]
    while(s[-1]=='\n'):
        s=s[:-1];
        if(DEBUG):
            print("N removed")
    return s


def get_local_sshkey():
    local_sshkey=os.path.expanduser("~/.ssh/id_rsa")
    if (not(os.path.exists(local_sshkey+".pub"))):
      os.system("ssh-keygen -q -t rsa -b 4096 -p -N '' -f "+local_sshkey)
    return remove_backslashN(file(local_sshkey+".pub").read() )
    
def get_local_ip():
  s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  s.connect(("8.8.8.8",80))
  return s.getsockname()[0]

def get_cpu_count():
    import multiprocessing
    return multiprocessing.cpu_count()

def meminfo(): #problem: in Kb, and adds "kb" to the string
        mi=dict()
        for line in file('/proc/meminfo',"r").readlines():
            mi[line.split(':')[0]] = line.split(':')[1].strip()
        return mi
    
def total_memory_kb():
    #file("free|awk '/^Mem:/{print $2}'").read()
    #from subprocess import Popen, PIPE
    #(stdout, stderr) = Popen(["cat","foo.txt"], stdout=PIPE).communicate()
    #print stdout
    '''
    FLDR="~/temp/"
    cmd2="free|awk '/^Mem:/{print $2}'>"+FLDR+"temp.txt"
    print(cmd2)
    os.system(cmd2)
    cmd3="free|awk '/^Mem:/{print $2}'"
    print(cmd3)
    FNM=FLDR+"temp.txt"
    print(FNM)
    import time
    while(not os.path.exists(FNM) ): #may lock it
        time.sleep(1)
        print(" %s "%FNM)
        print(" %d "%os.path.exists(FNM))
    return file(FNM).read()
    '''
    #
    kb=meminfo()["MemTotal"]
    #print("memory")
    #print(kb+"|")
    #print(kb[-3:]+"|")
    if(kb[-3:]==' kB'):
        #print("kB")
        #print(kb)
        s=kb[:-3];
        #print(s)
        s=int(s)
        #print(s*1024)
    else:
        s=int(kb)
    return s;

def availmem_bytes_kb():
    kb=meminfo()["MemFree"]
    if(kb[-3:]==' kB'):
        s=kb[:-3];
        s=int(s)
    else:
        s=int(kb)
    return s;

   
#print(availmem_bytes_kb())
#exit()
    
def uptime():
        return remove_backslashN(file('/proc/uptime',"r").read() ).split(" ")

def get_hostname():
    import socket
    return socket.gethostname()
    #import platform
    #platform.node()
    #
    #import socket
    #socket.gethostbyaddr(socket.gethostname())[0]

#print(get_hostname())
#exit()

def run_output(cmdl_argv):
    import subprocess
    #cmdl_argv=['ls', '-l']
    sp = subprocess.Popen(cmdl_argv, stdout=subprocess.PIPE)
    output, _ = sp.communicate()
    #print "Status:", sp.wait()
    status=sp.wait()
    #assert this is zero
    if (not (status==0)):
        print('Warning: error in running %r'%cmdl_argv)
    #print "Output:"
    #print output
    return output

def mymap(s, mapdic):
    if  s in mapdic.keys():
        s=mapdic[s]
    return s
    
#does this run on our minimalistic LXC contrianers?
#Make test routines for it.
def get_host_type():
    '''
    Geerates a 4 part string: lxub-amd64-trusty   which is lx + ub + amd64 + trusty
    '''
    #old method:
    #    codename2=run_output(['lsb_release', '-c']) # 'Codename:\ttrusty\n'
    #    codename = remove_backslashN(codename2.split('\t')[1])
    #
    #platform.dist()  ---> ('Ubuntu', '14.04', 'trusty')
    #platform.dist() ---> (distname='', version='', id='')
    #  supported_dists=('SuSE', 'debian', 'fedora', 'redhat', 'centos', 'mandrake', 'mandriva', 'rocks', 'slackware', 'yellowdog', 'gentoo', 'UnitedLinux', 'turbolinux', 'Ubuntu'))
    import platform
    codename = platform.dist()[2]
    #old method:
    #    machine_hdwr_name=run_output(['uname', '-m']) #x86_64
    #
    #os.uname() --> (sysname, nodename, release, version, machine)
    #   ('Linux',  'ss-desktop',  '3.13.0-39-generic',  '#66-Ubuntu SMP Tue Oct 28 13:30:27 UTC 2014',  'x86_64')
    #machinename=os.uname()[1]
    #also (same result): platform.machine(), platform.processor(), ...
    machine_hdwr_name=os.uname()[4] #x86_64
    machine_hdwr_name =mymap(machine_hdwr_name, { 'x86_64':'amd64', })
    #convert_map={ 'x86_64':'amd64' } #convert_mtype_d
    #if  machine_hdwr_name in convert_map.keys():
    #    machine_hdwr_name=convert_map[machine_hdwr_name]
    #
    linux_distrib_name = platform.dist()[0]  # 'Ubuntu'
    osid = os.uname()[0]  # 'Linux'
    linux_distrib_name=mymap(linux_distrib_name,{'Ubuntu':'ub',})
    osid=mymap(osid,{'Linux':'lx',})
    #print(linux_distrib_name ) 
    #print(osid)
    code1 = osid + linux_distrib_name;
    #
    #lsb_release
    # lxub-amd64-trusty
    # lsb_release -c  ---> Codename:       trusty
    # uname -m --> x86_64
    # Linux ss-desktop 3.13.0-39-generic #66-Ubuntu SMP Tue Oct 28 13:30:27 UTC 2014 x86_64 x86_64 x86_64 GNU/Linux
    #
    #On converting x86_64 to amd64: http://www.x86-64.org/pipermail/discuss/2006-July/009055.html
    #return codename+"-"+machine_hdwr_name
    return code1 + "-" + machine_hdwr_name+ "-" + codename 

#print(get_host_type())
#exit()

def free_disk_kb(path): #
    os.statvfs(path)  
    #os.statvfs('/')  #or '.' #todo: as parameter
    #posix.statvfs_result(f_bsize=4096, f_frsize=4096, f_blocks=89937581, f_bfree=86954340, f_bavail=82380004, f_files=22855680, f_ffree=22519452, f_favail=22519452, f_flag=4096, f_namemax=255)
    stat = os.statvfs('/')
    kb_free = (stat.f_bavail * stat.f_frsize) / 1024
    return kb_free


if 0:
    print( get_local_sshkey()  ) #includes a newline?
    print( get_local_ip()  ) # 192.168.1.92
    print( get_cpu_count()  ) # 4
    #print( meminfo()  ) # a long dic: {'WritebackTmp': '0 kB', ...}
    print( total_memory_kb()  ) #
    print( total_memory_kb()  ) #
    print( total_memory_kb()  ) #
    print( total_memory_kb()  ) #
    print( uptime()  ) #['93670.91', '217686.12\n']

#mi=meminfo()
#tm=mi['MemTotal']
#print( tm )

#{'WritebackTmp': '0 kB', 'SwapTotal': '8123388 kB', 'Active(anon)': '1513548 kB', 'SwapFree': '7733260 kB', 'DirectMap4k': '83584 kB', 'KernelStack': '5776 kB', 'MemFree': '3768520 kB', 'HugePages_Rsvd': '0', 'Committed_AS': '6774684 kB', 'Active(file)': '623244 kB', 'NFS_Unstable': '0 kB', 'VmallocChunk': '34359360736 kB', 'Writeback': '0 kB', 'Inactive(file)': '1140856 kB', 'MemTotal': '7918368 kB', 'VmallocUsed': '295512 kB', 'DirectMap1G': '5242880 kB', 'HugePages_Free': '0', 'AnonHugePages': '524288 kB', 'AnonPages': '1754004 kB', 'Active': '2136792 kB', 'Inactive(anon)': '501084 kB', 'CommitLimit': '12082572 kB', 'Hugepagesize': '2048 kB', 'Cached': '1708904 kB', 'SwapCached': '216400 kB', 'VmallocTotal': '34359738367 kB', 'Shmem': '112828 kB', 'Mapped': '231324 kB', 'SUnreclaim': '46492 kB', 'Unevictable': '64 kB', 'SReclaimable': '126752 kB', 'Mlocked': '64 kB', 'DirectMap2M': '2799616 kB', 'HugePages_Surp': '0', 'Bounce': '0 kB', 'Inactive': '1641940 kB', 'PageTables': '63420 kB', 'HardwareCorrupted': '0 kB', 'HugePages_Total': '0', 'Slab': '173244 kB', 'Buffers': '168024 kB', 'Dirty': '132 kB'}


#import mongo_connector
cnctr=mongo_connector.NuodbConnector()


#cnctr.close()
#cnctr.CLOUD_NODES.find()
conn=cnctr.connect("test_blue","wio-000:48004", "wideio_test", "wideio_test", {'schema':"django"})
#cursor = conn.cursor() #not used. We are using another layer on top of PyNuoDB.

print("%r"%conn) #output: "None"
print("connected")
table=cnctr.CLOUD_NODE
if 0:
    z=table.find() #or list(z) ?
    print("%r"%(list(z)))

print("-----------------")



import datetime
#datetime.datetime.now()
#datetime.datetime.now().time()
#datetime(2009, 1, 6, 15, 8, 24, 78915)
#now_time=datetime.datetime.now().time()
#time.strftime("%H:%M:%S")
#print now_time.strftime('%Y/%m/%d %H:%M:%S')
now_str=str(datetime.datetime.now())


#exit()

import uuid;


hname=get_hostname()
#basetype_="co"
avlmem=availmem_bytes_kb()
created_at_=now_str;
print("WARNING")
started_at_=now_str;
print("WARNING")

#rec=dict(hostname="test2")
#rec=dict(hostname="test2",ID="a")
#rec=dict(hostname="tesr3",ID="b")
rec=dict(
    hostname=hname,
    hostip=get_local_ip(),
    # ?
    basetype="co",
    wiostate="V",
    #
    hosttype=get_host_type(),
    pub_key_ssh=get_local_sshkey(),
    cpu_count=get_cpu_count(),
    total_memory=total_memory_kb(), #int(total_memory_byes()/1024),
    available_memory=availmem_bytes_kb(),
    #
    nworkers="-1", #warning (todo)
    load="(1.5,1.5,.15)",  #CPU laod, last 5min, laST 10 MIN, LAST ...
    remaining_free_hd=free_disk_kb("."), #in KB
    last_ping=  now_str, #problem
    #region=  #NULL
    #location=? #NULL
    created_at=now_str,  #2014-11-13 16:34:14.445949
    started_at=now_str,
    updated_at=now_str,
    #ID=str(uuid.uuid4()),
    )

'''
{
    'hostname': 'ss-desktop', 
    'hostip': '192.168.1.92', 
    'basetype': 'co', 
    'hosttype': '-----'
    'pub_key_ssh': 'ssh-rsa ', 
    'cpu_count': 4, 
    'total_memory': 7918368, 
    'available_memory': 2493656, 
    nworkers:-1,
    load =???? #CPU laod, last 5min, laST 10 MIN, LAST ...
    #remaining_free_hd=0
    last_ping = 
    #region=""
    #location=""
    'created_at': '2014-11-13 17:44:59.774095', 
    'started_at': '2014-11-13 17:44:59.774095', 
    'updated_at': '2014-11-13 17:44:59.774095', 
    #'id': uuid
    'wiostate': 'V', 
}
'''


#print("%r"%(rec))

#exit()
if 0:
    table.insert(rec)
    
    z=table.find() #or list(z) ?
    print("%r"%(list(z)))

print("=========")
#find({"a": 5})
#The (logical) primary key is hostname
ffl=list(table.find({"hostname" : get_hostname()}))
print len(ffl)
#assert(len(ffl)==1)
if len(ffl)==0:
    table.insert(rec)
    z=table.find() #or list(z) ?
    print("%r"%(list(z)))
    print ("INSERTED")
    exit()

if len(ffl)>1:
    print("Warning: more than one DB entries found associated with this hostname. Taking the first item.")
e=ffl[0]
#print("%r"%(list(ff)))
print("+%r"%(e))


#print rec.id
print e["id"]
rec["id"]=e["id"]
#del rec["id"]
e=rec; #replace! later: keep some older properties.

e["basetype"]="updated"

#table.update({}, e) #Didn't work: (adds another recortd?).

#table.update({id: }, e)


table.update({'id': e["id"]}, e, { 'upsert': True })#


#from mongo python:
# To avoid inserting the same document more than once, only use upsert: true if the query field is uniquely indexed.

#conn.commit() #

cnctr.close()
exit()

#10 sec

# To avoid inserting the same document more than once, only use upsert: true if the query field is uniquely indexed.




# [{
#   'load': '', 
#   'created_at': datetime.datetime(1970, 1, 1, 1, 0),
#   'last_ping': datetime.datetime(1970, 1, 1, 1, 0),
#   'cpu_count': 0,
#   'total_memory_byes': 0,
#   'available_memory': 0,
#   'basetype': '', 'region': '', 
#   'hostname': '3', 
#   'updated_at': datetime.datetime(1970, 1, 1, 1, 0),
#   'wiostate': '', 'nworkers': 0, 'location': '', 'hostip': '', 
#   'remaining_free_hd': 0, 'started_at': datetime.datetime(1970, 1, 1, 1, 0),
#   'id': '', 'hosttype': '', 'pub_key_ssh': ''
# }]
#
#print("%r"%(q[:]))


# table.find({"hostname":"3"}).remove()
