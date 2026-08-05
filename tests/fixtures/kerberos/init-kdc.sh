#!/bin/bash
set -e

# 参数化的 KDC 初始化脚本, 由单节点和多 broker 两个 stack 共用。
#
# 环境变量:
#   KRB5_REALM       Kerberos realm (默认 EXAMPLE.COM)
#   KRB5_KDC_HOST    KDC 在 compose 网络中的主机名 (默认 kdc.example.com)
#   KRB5_PRINCIPALS  空格分隔的服务 principal 列表 (不含 @REALM),
#                    每个都会导出到 /etc/keytabs/kafka.keytab
#   KRB5_CLIENT      客户端 principal (默认 client), 导出到 client.keytab
#
# 两个 stack 使用不同的 realm 和不同的 KDC 主机别名, 因此可以同时运行而
# 互不干扰 (此前共用 EXAMPLE.COM + kdc.example.com 会导致 broker 拿着 A 的
# keytab 去向 B 认证, 报 "Checksum failed")。
REALM="${KRB5_REALM:-EXAMPLE.COM}"
KDC_HOST="${KRB5_KDC_HOST:-kdc.example.com}"
CLIENT="${KRB5_CLIENT:-client}"
PRINCIPALS="${KRB5_PRINCIPALS:-kafka/broker.example.com kafka/localhost kafka/127.0.0.1}"

# krb5.conf / kdc.conf / kadm5.acl 按实际 realm 渲染, 镜像里存的是模板。
echo "=== Rendering Kerberos config for realm ${REALM} (kdc=${KDC_HOST}) ==="
sed -e "s/@@REALM@@/${REALM}/g" -e "s/@@KDC_HOST@@/${KDC_HOST}/g" \
    /etc/krb5.conf.tmpl > /etc/krb5.conf
sed "s/@@REALM@@/${REALM}/g" /etc/krb5kdc/kdc.conf.tmpl > /etc/krb5kdc/kdc.conf
sed "s/@@REALM@@/${REALM}/g" /etc/krb5kdc/kadm5.acl.tmpl > /etc/krb5kdc/kadm5.acl

# KDC 数据库持久化检查: 数据库在容器层, 每次重建容器需重新初始化。
#
# 重要: principal 创建与 keytab 导出必须在同一个 if 分支内完成。
# `ktadd` 默认会为 principal 生成新的随机密钥并递增 KVNO, 因此若每次容器
# 启动都重新导出, 已经加载了旧 keytab 的 Kafka broker 会因为密钥不匹配而
# 报 "Checksum failed" 并启动失败。数据库和 keytab 同时持久化, 保证二者
# 始终来自同一次导出。
if [ ! -f /etc/krb5kdc/principal ]; then
    echo "=== Initializing KDC database (realm ${REALM}) ==="
    kdb5_util create -r "${REALM}" -s -P masterkey

    echo "=== Adding principals ==="
    for p in ${PRINCIPALS}; do
        kadmin.local -q "addprinc -randkey ${p}@${REALM}"
    done
    kadmin.local -q "addprinc -randkey ${CLIENT}@${REALM}"

    # keytab 通过 bind mount 共享给主机和 Kafka, 导出前清理陈旧文件
    # (上一次运行遗留的 keytab 对应已删除的数据库, 密钥必然不匹配)
    echo "=== Exporting keytabs ==="
    mkdir -p /etc/keytabs
    rm -f /etc/keytabs/*.keytab
    for p in ${PRINCIPALS}; do
        kadmin.local -q "ktadd -k /etc/keytabs/kafka.keytab ${p}@${REALM}"
    done
    kadmin.local -q "ktadd -k /etc/keytabs/client.keytab ${CLIENT}@${REALM}"
    chmod 644 /etc/keytabs/*.keytab
    echo "=== Keytabs exported ==="
else
    echo "=== Reusing existing KDC database and keytabs ==="
fi

kadmin.local -q "listprincs"

echo "=== Starting KDC ==="
exec krb5kdc -n
