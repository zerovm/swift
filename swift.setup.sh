#!/bin/bash
uname=$(id -nu)
gname=$(id -ng)
if [[ "${1::5}" == "/dev/" ]]; then
	dev=$1
elif [[ -n "$1" ]]; then
	echo <<END
Usage: 
  $0 <partition name>
     Partiton will be ERASED!
  $0
     Regular operation with loopback device in file
END
fi
if [[ -n "$dev" ]]; then
sudo mkfs.xfs -f -i size=1024 "$dev"
sudo sed -i '/\s\/mnt\/sdb1\s/d' /etc/fstab
sudo su -c 'echo -e "'$dev'\t/mnt/sdb1\txfs\tloop,noatime,nodiratime,nobarrier,logbufs=8\t0\t0" >> /etc/fstab'
else
sudo mkdir -p /srv
sudo dd if=/dev/zero of=/srv/swift-disk bs=1024 count=0 seek=1000000
sudo mkfs.xfs -f -i size=1024 /srv/swift-disk
sudo sed -i '/\s\/mnt\/sdb1\s/d' /etc/fstab
sudo su -c 'echo -e "/srv/swift-disk\t/mnt/sdb1\txfs\tloop,noatime,nodiratime,nobarrier,logbufs=8\t0\t0" >> /etc/fstab'
fi
sudo mkdir -p /mnt/sdb1
sudo mount /mnt/sdb1
sudo mkdir -p /mnt/sdb1/1 /mnt/sdb1/2 /mnt/sdb1/3 /mnt/sdb1/4
sudo chown $uname:$gname /mnt/sdb1/*
sudo su -c 'for x in {1..4}; do ln -s /mnt/sdb1/$x /srv/$x; done'
sudo mkdir -p /etc/swift/object-server /etc/swift/container-server /etc/swift/account-server /srv/1/node/sdb1 /srv/2/node/sdb2 /srv/3/node/sdb3 /srv/4/node/sdb4 /var/run/swift
sudo chown -R $uname:$gname /etc/swift /srv/[1-4]/ /var/run/swift
sudo sed -i '/\/var\/cache\/swift/d;/\/var\/run\/swift/d' /etc/rc.local
sudo sed -i '/^exit 0/i mkdir /var/cache/swift1 /var/cache/swift2 /var/cache/swift3 /var/cache/swift4\nchown '$uname:$gname' /var/cache/swift*\nmkdir /var/run/swift\nchown '$uname:$gname' /var/run/swift' /etc/rc.local
sudo su -c 'cat > /etc/rsyncd.conf' <<END
uid = $uname
gid = $gname
log file = /var/log/rsyncd.log
pid file = /var/run/rsyncd.pid
address = 127.0.0.1

[account6012]
max connections = 25
path = /srv/1/node/
read only = false
lock file = /var/lock/account6012.lock

[account6022]
max connections = 25
path = /srv/2/node/
read only = false
lock file = /var/lock/account6022.lock

[account6032]
max connections = 25
path = /srv/3/node/
read only = false
lock file = /var/lock/account6032.lock

[account6042]
max connections = 25
path = /srv/4/node/
read only = false
lock file = /var/lock/account6042.lock


[container6011]
max connections = 25
path = /srv/1/node/
read only = false
lock file = /var/lock/container6011.lock

[container6021]
max connections = 25
path = /srv/2/node/
read only = false
lock file = /var/lock/container6021.lock

[container6031]
max connections = 25
path = /srv/3/node/
read only = false
lock file = /var/lock/container6031.lock

[container6041]
max connections = 25
path = /srv/4/node/
read only = false
lock file = /var/lock/container6041.lock


[object6010]
max connections = 25
path = /srv/1/node/
read only = false
lock file = /var/lock/object6010.lock

[object6020]
max connections = 25
path = /srv/2/node/
read only = false
lock file = /var/lock/object6020.lock

[object6030]
max connections = 25
path = /srv/3/node/
read only = false
lock file = /var/lock/object6030.lock

[object6040]
max connections = 25
path = /srv/4/node/
read only = false
lock file = /var/lock/object6040.lock

END

sudo sed -i 's/.*\(RSYNC_ENABLE=\).*/\1true/' /etc/default/rsync
sudo service rsync restart
sudo su -c 'cat > /etc/rsyslog.d/10-swift.conf' <<'END'
# Uncomment the following to have a log containing all logs together
local1,local2,local3,local4,local5.*   /var/log/swift/all.log

# Uncomment the following to have hourly proxy logs for stats processing
#$template HourlyProxyLog,"/var/log/swift/hourly/%$YEAR%%$MONTH%%$DAY%%$HOUR%"
#local1.*;local1.!notice ?HourlyProxyLog

local1.*;local1.!notice /var/log/swift/proxy.log
local1.notice           /var/log/swift/proxy.error
local1.*                ~

local2.*;local2.!notice /var/log/swift/storage1.log
local2.notice           /var/log/swift/storage1.error
local2.*                ~

local3.*;local3.!notice /var/log/swift/storage2.log
local3.notice           /var/log/swift/storage2.error
local3.*                ~

local4.*;local4.!notice /var/log/swift/storage3.log
local4.notice           /var/log/swift/storage3.error
local4.*                ~

local5.*;local5.!notice /var/log/swift/storage4.log
local5.notice           /var/log/swift/storage4.error
local5.*                ~

END

sudo sed -i 's/\(PrivDropToGroup\s\).*/\1adm/' /etc/rsyslog.conf
sudo mkdir -p /var/log/swift/hourly
sudo chown -R syslog.adm /var/log/swift
sudo service rsyslog restart

cd
mkdir -p ~/bin
sudo rm -fr ~/swift
git clone https://github.com/Dazo-org/swift.git ~/swift
cd ~/swift
sudo python setup.py develop
cd 
sudo rm -fr ~/python-swiftclient
git clone https://github.com/Dazo-org/python-swiftclient.git ~/python-swiftclient
cd ~/python-swiftclient
sudo python setup.py develop
cd
sed -i '/SWIFT_TEST_CONFIG_FILE/d; /export PATH=.*~\/bin/d' ~/.bashrc
cat >> ~/.bashrc <<'END'
export SWIFT_TEST_CONFIG_FILE=/etc/swift/test.conf
export PATH=${PATH}:~/bin
END

. ~/.bashrc

echo "creating proxy config"
cat > /etc/swift/proxy-server.conf <<END
[DEFAULT]
bind_port = 8080
user = $uname
log_facility = LOG_LOCAL1

[pipeline:main]
pipeline = healthcheck cache tempauth proxy-logging proxy-query proxy-server

[app:proxy-server]
use = egg:swift#proxy
allow_account_management = true
account_autocreate = true

[filter:tempauth]
use = egg:swift#tempauth
user_admin_admin = admin .admin .reseller_admin
user_test_tester = testing .admin
user_test2_tester2 = testing2 .admin
user_test_tester3 = testing3

[filter:healthcheck]
use = egg:swift#healthcheck

[filter:cache]
use = egg:swift#memcache

[filter:proxy-logging]
use = egg:swift#proxy_logging

[filter:proxy-query]
use = egg:swift#proxy_query

END

echo "creating swift config"
cat > /etc/swift/swift.conf <<END
[swift-hash]
swift_hash_path_suffix = litestack
END

echo "creating account config"
for i in 1 2 3 4; do
cat > /etc/swift/account-server/${i}.conf <<END
[DEFAULT]
devices = /srv/${i}/node
mount_check = false
bind_port = 60${i}2
user = $uname
log_facility = LOG_LOCAL$(($i + 1))
recon_cache_path = /var/cache/swift${i}

[pipeline:main]
pipeline = recon account-server

[app:account-server]
use = egg:swift#account

[filter:recon]
use = egg:swift#recon

[account-replicator]
vm_test_mode = yes

[account-auditor]

[account-reaper]

END
done

echo "creating container config"
for i in 1 2 3 4; do
cat > /etc/swift/container-server/${i}.conf <<END
[DEFAULT]
devices = /srv/${i}/node
mount_check = false
bind_port = 60${i}1
user = $uname
log_facility = LOG_LOCAL$(($i + 1))
recon_cache_path = /var/cache/swift${i}

[pipeline:main]
pipeline = recon container-server

[app:container-server]
use = egg:swift#container

[filter:recon]
use = egg:swift#recon

[container-replicator]
vm_test_mode = yes

[container-updater]

[container-auditor]

[container-sync]

END
done

echo "creating object config"
for i in 1 2 3 4; do
cat > /etc/swift/object-server/${i}.conf <<END
[DEFAULT]
devices = /srv/${i}/node
mount_check = false
bind_port = 60${i}0
user = $uname
log_facility = LOG_LOCAL$(($i + 1))
recon_cache_path = /var/cache/swift${i}

[pipeline:main]
pipeline = recon object-query object-server

[app:object-server]
use = egg:swift#object

[filter:recon]
use = egg:swift#recon

[filter:object-query]
use = egg:swift#object_query

[object-replicator]
vm_test_mode = yes

[object-updater]

[object-auditor]

END
done

echo "creating resetswift script"
if [ -z "$dev" ]; then
	dev="/srv/swift-disk"
fi
cat > ~/bin/resetswift <<END
#!/bin/bash

swift-init all stop
find /var/log/swift -type f -exec rm -f {} \;
sudo umount /mnt/sdb1
sudo mkfs.xfs -f -i size=1024 $dev
sudo mount /mnt/sdb1
sudo mkdir -p /mnt/sdb1/1 /mnt/sdb1/2 /mnt/sdb1/3 /mnt/sdb1/4
sudo chown $uname:$gname /mnt/sdb1/*
mkdir -p /srv/1/node/sdb1 /srv/2/node/sdb2 /srv/3/node/sdb3 /srv/4/node/sdb4
sudo rm -f /var/log/debug /var/log/messages /var/log/rsyncd.log /var/log/syslog
find /var/cache/swift* -type f -name *.recon -exec -rm -f {} \;
sudo service rsyslog restart
sudo service memcached restart

END
chmod +x ~/bin/resetswift

echo "creating remakerings script"
cat > ~/bin/remakerings <<END
#!/bin/bash

cd /etc/swift

rm -f *.builder *.ring.gz backups/*.builder backups/*.ring.gz

swift-ring-builder object.builder create 18 3 1
swift-ring-builder object.builder add z1-127.0.0.1:6010/sdb1 1
swift-ring-builder object.builder add z2-127.0.0.1:6020/sdb2 1
swift-ring-builder object.builder add z3-127.0.0.1:6030/sdb3 1
swift-ring-builder object.builder add z4-127.0.0.1:6040/sdb4 1
swift-ring-builder object.builder rebalance
swift-ring-builder container.builder create 18 3 1
swift-ring-builder container.builder add z1-127.0.0.1:6011/sdb1 1
swift-ring-builder container.builder add z2-127.0.0.1:6021/sdb2 1
swift-ring-builder container.builder add z3-127.0.0.1:6031/sdb3 1
swift-ring-builder container.builder add z4-127.0.0.1:6041/sdb4 1
swift-ring-builder container.builder rebalance
swift-ring-builder account.builder create 18 3 1
swift-ring-builder account.builder add z1-127.0.0.1:6012/sdb1 1
swift-ring-builder account.builder add z2-127.0.0.1:6022/sdb2 1
swift-ring-builder account.builder add z3-127.0.0.1:6032/sdb3 1
swift-ring-builder account.builder add z4-127.0.0.1:6042/sdb4 1
swift-ring-builder account.builder rebalance

END
chmod +x ~/bin/remakerings

echo "creating startmain script"
cat > ~/bin/startmain <<END
#!/bin/bash

swift-init main start
END
chmod +x ~/bin/startmain

echo "creating startrest script"
cat > ~/bin/startrest <<END
#!/bin/bash

swift-init rest start
END
chmod +x ~/bin/startrest

~/bin/remakerings


