#!/usr/bin/python -OO
# -*- coding:utf-8 -*-

# ORM(sqlalchemy)を利用したバッチ処理です。内容は3年前の仕事で作ったものなので
# セキュリティの都合上多少変更を加えました。(path,コマンド、DBの設定など)
# DB上の2つのテーブル(v_mails, v_alias)のカラムemail(v_mails), src_email(VirAlias)のメールアドレスを
# ファイルに書き込みgzipで圧縮するものです。(元々は別のコマンドでしたが代用しました)
# また多重起動はないものとします。
# 日本語は使用しないのでcharsetは指定していません
# src_emailはlatin1にしています
# user_idはserialにしています

# [動作確認した環境]
# CentOS7(64bit)
# python2.7.5
# MySQL5.6

import sys
import os
import subprocess
from sqlalchemy import *
from sqlalchemy.orm import *
import urllib

# マッピング用のクラスです
class VirMails(object):
    pass

# マッピング用のクラスです
class VirAlias(object):
    pass

# マッピング処理をするクラスです
class TableMap(object):
    # table_name
    v_mails = "v_mails"
    v_alias = "v_alias"

    def mapping(self, engine):
        metadata = MetaData(bind=engine)

        metatable1 = Table(self.v_mails, metadata, Column("user_id", Integer, primary_key=True), autoload=True)
        metatable2 = Table(self.v_alias, metadata, Column("src_email", String(256), primary_key=True), autoload=True)

        mapper(VirMails, metatable1)
        mapper(VirAlias, metatable2)

# DB接続用クラスです
class DB(object):

    def __init__(self):
        self.db_host = "localhost"
        self.db_name = "sample"
        self.db_user = "sample"
        self.db_pass = "sample"
        self.db_port = "3306"


    def connect(self):
        try:
            engine = create_engine("mysql+pymysql://%(user)s:%(pass)s@%(host)s:%(port)s/%(name)s" %
                                  {"host":urllib.quote(self.db_host),
                                   "port":self.db_port,
                                   "name":urllib.quote(self.db_name),
                                   "user":urllib.quote(self.db_user),
                                   "pass":urllib.quote(self.db_pass)}, convert_unicode=True)


            table_map = TableMap()
            table_map.mapping(engine)
            Session = sessionmaker(bind=engine)
            return Session()
        except Exception as e:
            raise

# 実際にDBの2テーブルからアドレスを取得しメモリに持ち返します。
# 全てメモリに乗るものとします。
def get_address_list():
    from_address = []
    session = None
    try:
        db = DB()
        session = db.connect()

        q1 = session.query(VirMails.email)
        q2 = session.query(VirAlias.src_email)
        query = q1.union(q2)
        if query.count() <= 0:
            print "Error:It was not possible to retrieve the data from Database"
            session.close()
            return []

        for row in query:
            from_address.append(row.email+'\tOK\n')

    except Exception as e:
        print "message:" + e.message
    finally:
        if session != None:
            session.close()

    return from_address

# get_address_list関数(内部でDB処理)を呼び出しアドレスを取得し、ファイル(permit_sender_list)に書き込んだ後
# gzipで圧縮します。
def main():
    tmp_dir = "/var/tmp"
    gzip_path = "/usr/bin/gzip"

    filename = tmp_dir + "/permit_sender_list"
    exe_command = gzip_path + " " + tmp_dir + "/permit_sender_list"

    if os.path.isdir(tmp_dir) == False:
        print "Error: There is no directory " + tmp_dir
        return -1

    if os.path.isfile(gzip_path) == False:
        print "Error: There is no file " + gzip_path
        return -1

    from_address = get_address_list()

    if len(from_address) <= 0:
        print "Error:There is not from_address"
        return -1

    fw = None
    try:
        fw = open(filename, 'w')
        fw.writelines(from_address)
    except Exception as e:
        print "message:" + e.message
    finally:
        if fw != None:
            fw.close() 

    if os.path.isfile(filename) == False:
        print "Error: There is no file " + filename
        return -1

    ret = subprocess.call(exe_command, shell=True)
    return ret

if __name__ == '__main__':
    main()
