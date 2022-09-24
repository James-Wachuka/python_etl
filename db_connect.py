
#import pymysql
import psycopg2
import mysql.connector

def get_conn_mysql():
    conn = mysql.connector.connect(host="localhost", port=3306, user="root", password="jewachu26", db="classicmodels")
    # start a connection
    cur = conn.cursor()
    return cur, conn

def get_conn_postgresql():
    conn = psycopg2.connect(host="localhost",database="classicmodels_analysis",user="postgres",password="jewachu26")
    # start a connection
    cur = conn.cursor()
    return cur, conn
