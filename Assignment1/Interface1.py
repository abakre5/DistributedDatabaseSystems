import math

import psycopg2
import time
import os
import sys


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def getValueArgumentInsertionString(valueList, cursor):
    return b','.join(cursor.mogrify("(%s,%s,%s)", x) for x in valueList).decode()


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cursor = openconnection.cursor()
    deleteTables('ALL', openconnection)
    createRatingsTableQuery = "CREATE TABLE " + ratingstablename + " (userid int, test1 varchar(10), movieid int, test2 varchar(10), rating float, test3 varchar(10), test4 int);"
    executeQuery(createRatingsTableQuery, cursor, openconnection)
    createMetadataTableQuery = "CREATE TABLE METADATA (noofrangepartitions float, noofroundrobinpartitions int, lastroundrobininsertedfragment int);"
    executeQuery(createMetadataTableQuery, cursor, openconnection)
    insertDefaultRowMetaData = "INSERT INTO METADATA VALUES (0.0, 0, 0);"
    executeQuery(insertDefaultRowMetaData, cursor, openconnection)
    dataFile = open(ratingsfilepath)
    cursor.copy_from(dataFile, ratingstablename, sep=':', size=8192)
    dropUnwantedColumns(cursor, openconnection)
    dataFile.close()
    cursor.close()


# Fragment RATINGS table data as range partitions
def rangePartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions > 0:
        cursor = openconnection.cursor()
        ratingsTableData = getRatingTableData(ratingstablename, cursor)
        rangePartitionRatingTable('range_ratings_part', numberofpartitions, ratingsTableData, openconnection, cursor)
        cursor.close()


# Fragment RATINGS table data as roundRobin partitions
def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions:
        cursor = openconnection.cursor()
        ratingsTableData = getRatingTableData(ratingstablename, cursor)
        roundRobinPartitionRatingTable('round_robin_ratings_part', numberofpartitions, ratingsTableData, openconnection,
                                       cursor)
        cursor.close()


# Insert row into range partitions
def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor()
    noOfRangePartitions = getMetaData(1, cursor)
    if noOfRangePartitions > 0:
        index = getRowPartitionIndex(rating, noOfRangePartitions)
        row = (userid, itemid, rating)
        insertRowInRatingsTable(ratingstablename, row, cursor, openconnection)
        insertRowInPartitionTable('range_ratings_part', index, row, cursor, openconnection)
    cursor.close()


# Insert row into round robin partitions
def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor()
    noOfRoundRobinPartitions = getMetaData(2, cursor)
    if noOfRoundRobinPartitions > 0:
        lastRoundRobinInsertedFragment = getMetaData(3, cursor)
        loadDataInTableQuery = "INSERT INTO {} (USERID, MOVIEID, RATING) VALUES (%s, %s, %s)"
        query = loadDataInTableQuery.format(
            'round_robin_ratings_part' + str(lastRoundRobinInsertedFragment % noOfRoundRobinPartitions))
        row = (userid, itemid, rating)
        insertRowInRatingsTable(ratingstablename, row, cursor, openconnection)
        cursor.execute(query, row)
        openconnection.commit()
        lastRoundRobinInsertedFragment += 1
        updateMetaDataTable(lastRoundRobinInsertedFragment, 3, openconnection, cursor)
    cursor.close()


# Get data within the specified range from range and round robin partitions
def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    outputFile = open(outputPath, "w")
    cursor = openconnection.cursor()
    noOfRangePartitions = getMetaData(1, cursor)
    if noOfRangePartitions > 0:
        startIndex = getRowPartitionIndex(ratingMinValue, noOfRangePartitions)
        endIndex = getRowPartitionIndex(ratingMaxValue, noOfRangePartitions)
        searchRangeQueryInRangePartition(startIndex, endIndex, ratingMinValue, ratingMaxValue, outputFile, cursor)
    searchRangeQueryInRoundRobinPartition(ratingMinValue, ratingMaxValue, outputFile, cursor)
    cursor.close()


# Get the specified data from range and round robin partitions
def pointQuery(ratingValue, openconnection, outputPath):
    outputFile = open(outputPath, "w")
    cursor = openconnection.cursor()
    noOfRangePartitions = getMetaData(1, cursor)
    if noOfRangePartitions:
        index = getRowPartitionIndex(ratingValue, noOfRangePartitions)
        searchPointQueryInRangePartition(index, outputFile, ratingValue, cursor)
    searchPointQueryInRoundRobinPartition(outputFile, ratingValue, cursor)
    cursor.close()


def createDB(dbname='postgres'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()


def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()


# execute a query
def executeQuery(query, cursor, openConnection):
    cursor.execute(query)
    openConnection.commit()


# Get all the values from data file as a list
def getInsertionValueList(dataFile):
    valueList = []
    for row in dataFile:
        dataArr = row.split("::")
        values = (dataArr[0], dataArr[1], dataArr[2])
        valueList.append(values)
    return valueList


# Drop the redundant columns from RATINGS table
def dropUnwantedColumns(cursor, openConnection):
    executeQuery("ALTER TABLE ratings DROP COLUMN test1, DROP test2, DROP test3,  DROP test4", cursor, openConnection)


# get all data of RATINGS table
def getRatingTableData(ratingstablename, cursor):
    selectRatingDataQuery = "SELECT * FROM " + ratingstablename
    cursor.execute(selectRatingDataQuery)
    return cursor.fetchall()


# Create partition tables for desired schema
def createPartitionTables(ratingstablename, numberofpartitions, openConnection, cursor):
    tableCreationQuery = "CREATE TABLE IF NOT EXISTS {} (USERID INT, MOVIEID INT, RATING FLOAT);"
    for index in range(numberofpartitions):
        query = tableCreationQuery.format(ratingstablename + str(index))
        cursor.execute(query)
        openConnection.commit()


# Insert row into the ratings table
def insertRowInRatingsTable(tableName, row, cursor, openConnection):
    argStr = getValueArgumentInsertionString([row], cursor)
    query = "INSERT INTO {} VALUES "
    query = query.format(tableName)
    executeQuery(query + argStr, cursor, openConnection)


# UPDATE Metadata table with specified data
def updateMetaDataTable(value, index, openConnection, cursor):
    column = ""
    if index == 1:
        column = "noofrangepartitions"
    elif index == 2:
        column = "noofroundrobinpartitions"
    else:
        column = "lastroundrobininsertedfragment"
    query = "UPDATE METADATA SET " + column + " = " + str(value)
    executeQuery(query, cursor, openConnection)


# Get Partition index for each rating
def getRowPartitionIndex(rating, numberOfPartitions):
    index = math.ceil((rating * numberOfPartitions) / 5) - 1
    if index < 0:
        index = 0
    return index


# Add respective data into partition
def addDataIntoRespectivePartitionTable(tableName, dataMatrix, openConnection, cursor):
    for index in range(len(dataMatrix)):
        valueList = dataMatrix[index]
        argStr = getValueArgumentInsertionString(valueList, cursor)
        if argStr != '':
            query = "INSERT INTO {} VALUES "
            query = query.format(tableName + str(index))
            executeQuery(query + argStr, cursor, openConnection)


# Perform range partitioning
def rangePartitionRatingTable(tableName, numberofpartitions, ratingsTableData, openConnection, cursor):
    createPartitionTables(tableName, numberofpartitions, openConnection, cursor)
    updateMetaDataTable(numberofpartitions, 1, openConnection, cursor)
    distributeRangePartitionData(ratingsTableData, numberofpartitions, openConnection, cursor)


# Distribute rating table data into range partitions
def distributeRangePartitionData(ratingsTableData, numberOfPartitions, openConnection, cursor):
    dataMatrix = []
    for i in range(numberOfPartitions):
        dataMatrix.append([])
    for row in ratingsTableData:
        index = getRowPartitionIndex(row[2], numberOfPartitions)
        dataMatrix[index].append(row)
    addDataIntoRespectivePartitionTable('range_ratings_part', dataMatrix, openConnection, cursor)


# Perform round robin partitioning
def roundRobinPartitionRatingTable(tableName, numberofpartitions, ratingsTableData, openConnection, cursor):
    createPartitionTables(tableName, numberofpartitions, openConnection, cursor)
    updateMetaDataTable(numberofpartitions, 2, openConnection, cursor)
    distributeRoundRobinPartitionData(ratingsTableData, numberofpartitions, openConnection, cursor)


# Distribute rating table data into round robin partitions
def distributeRoundRobinPartitionData(ratingsTableData, noOfRoundRobinPartitions, openConnection, cursor):
    dataMatrix = []
    for i in range(noOfRoundRobinPartitions):
        dataMatrix.append([])
    lastRoundRobinInsertedFragment = getMetaData(3, cursor)
    for row in ratingsTableData:
        dataMatrix[lastRoundRobinInsertedFragment % noOfRoundRobinPartitions].append(row)
        lastRoundRobinInsertedFragment += 1
    updateMetaDataTable(lastRoundRobinInsertedFragment, 3, openConnection, cursor)
    addDataIntoRespectivePartitionTable('round_robin_ratings_part', dataMatrix, openConnection, cursor)


# Insert the given row into the given partition
def insertRowInPartitionTable(tableName, index, row, cursor, openConnection):
    argStr = getValueArgumentInsertionString([row], cursor)
    query = "INSERT INTO {} VALUES "
    query = query.format(tableName + str(index))
    executeQuery(query + argStr, cursor, openConnection)


# Search the rating value in the corresponding range partition
def searchPointQueryInRangePartition(index, outputFile, ratingValue, cursor):
    query = "SELECT USERID, MOVIEID FROM range_ratings_part" + str(index) + " WHERE RATING =" + str(ratingValue)
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
        strToWrite = 'range_ratings_part' + str(index) + "," + str(row[0]) + "," + str(row[1]) + "," + str(
            ratingValue) + "\n"
        outputFile.write(strToWrite)


# Search the ratings between(including) given values in round robin partitions
def searchRangeQueryInRoundRobinPartition(ratingMinValue, ratingMaxValue, outputFile, cursor):
    selectQuery = "SELECT USERID, MOVIEID, RATING FROM round_robin_ratings_part" + "{} WHERE RATING >=" + str(
        ratingMinValue) + " and RATING <= " + str(ratingMaxValue)
    noOfRoundRobinPartitions = getMetaData(2, cursor)
    for index in range(noOfRoundRobinPartitions):
        query = selectQuery.format(str(index))
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            strToWrite = 'round_robin_ratings_part' + str(index) + "," + str(row[0]) + "," + str(row[1]) + "," + str(
                row[2]) + "\n"
            outputFile.write(strToWrite)


# Search rating value in the round robin partitions
def searchPointQueryInRoundRobinPartition(outputFile, ratingValue, cursor):
    selectQuery = "SELECT USERID, MOVIEID FROM round_robin_ratings_part{} WHERE RATING =" + str(ratingValue)
    noOfRoundRobinPartitions = getMetaData(2, cursor)
    for index in range(noOfRoundRobinPartitions):
        query = selectQuery.format(str(index))
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            strToWrite = 'round_robin_ratings_part' + str(index) + "," + str(row[0]) + "," + str(row[1]) + "," + str(
                ratingValue) + "\n"
            outputFile.write(strToWrite)


# Search the ratings between(including) given values in range partitions
def searchRangeQueryInRangePartition(startIndex, endIndex, ratingMinValue, ratingMaxValue, outputFile, cursor):
    while startIndex <= endIndex:
        query = "SELECT USERID, MOVIEID, RATING FROM range_ratings_part" + str(
            startIndex) + " WHERE RATING BETWEEN " + str(ratingMinValue) + " AND " + str(ratingMaxValue)
        cursor.execute(query)
        rows = cursor.fetchall()
        i = 0
        for row in rows:
            strToWrite = 'range_ratings_part' + str(startIndex) + "," + str(row[0]) + "," + str(row[1]) + "," + str(
                row[2]) + "\n"
            outputFile.write(strToWrite)
            i += 1
        startIndex += 1


# Get metadata for corresponding column
def getMetaData(index, cursor):
    column = ""
    if index == 1:
        column = "noofrangepartitions"
    elif index == 2:
        column = "noofroundrobinpartitions"
    else:
        column = "lastroundrobininsertedfragment"
    query = "SELECT " + column + " FROM METADATA"
    cursor.execute(query)
    data = cursor.fetchall()
    return data[0][0]
