#!/bin/bash
set -e

# KDC 数据库持久化检查: 数据库在容器层, 每次重建容器需重新初始化
if [ ! -f /etc/krb5kdc/principal ]; then
    echo "=== Initializing KDC database ==="
    kdb5_util create -r EXAMPLE.COM -s -P masterkey

    echo "=== Adding principals ==="
    kadmin.local -q "addprinc -randkey kafka/broker.example.com@EXAMPLE.COM"
    kadmin.local -q "addprinc -randkey kafka/localhost@EXAMPLE.COM"
    kadmin.local -q "addprinc -randkey kafka/127.0.0.1@EXAMPLE.COM"
    kadmin.local -q "addprinc -randkey client@EXAMPLE.COM"
fi

# 每次启动重新导出 keytab, 确保 KVNO 与数据库一致
# (keytab 通过 bind mount 共享给主机和 Kafka, 使用前删除陈旧文件)
echo "=== (Re)exporting keytabs ==="
mkdir -p /etc/keytabs
rm -f /etc/keytabs/*.keytab
kadmin.local -q "ktadd -k /etc/keytabs/kafka.keytab kafka/broker.example.com@EXAMPLE.COM"
# broker 的 JAAS principal 是 kafka/localhost@EXAMPLE.COM, 也需导出
kadmin.local -q "ktadd -k /etc/keytabs/kafka.keytab kafka/localhost@EXAMPLE.COM"
kadmin.local -q "ktadd -k /etc/keytabs/kafka.keytab kafka/127.0.0.1@EXAMPLE.COM"
kadmin.local -q "ktadd -k /etc/keytabs/client.keytab client@EXAMPLE.COM"
chmod 644 /etc/keytabs/*.keytab
kadmin.local -q "listprincs"
echo "=== Keytabs exported ==="

echo "=== Starting KDC ==="
exec krb5kdc -n
