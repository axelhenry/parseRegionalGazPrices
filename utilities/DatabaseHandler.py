# -*- coding: utf-8 -*-
import psycopg2.pool as pool
import logging
import utilities.sql.functions as sql


class DatabaseHandler:

    _myHost = None
    _myDbName = None
    _myUser = None
    _myPassword = ''
    _logger = None
    _myPool = None
    _myPort = 5432
    _myArgString = 'host={} port={} dbname={} user={} password={}'

    def __init__(self, aHost, aPort, aDbName, aUser, aPassword=''):
        self._logger = logging.getLogger()
        # on met le niveau du logger à DEBUG, comme ça il écrit tout
        self._logger.setLevel(logging.DEBUG)
        steam_handler = logging.StreamHandler()
        steam_handler.setLevel(logging.DEBUG)
        self._logger.addHandler(steam_handler)
        self._myHost = aHost
        self._myPort = aPort
        self._logger.info('DatabaseHandler :: %s : %s', 'host', self._myHost)
        self._myDbName = aDbName
        self._logger.info(
            'DatabaseHandler :: %s : %s', 'dbname', self._myDbName)
        self._myUser = aUser
        self._logger.info('DatabaseHandler :: %s : %s', 'user', self._myUser)
        if aPassword != '':
            self._myPassword = aPassword
            self._logger.info(
                'DatabaseHandler :: %s : %s', 'password', self._myPassword)
        myStr = str.format(self._myArgString, self._myHost, self._myPort,
                           self._myDbName, self._myUser, self._myPassword)
        self._logger.info('DatabaseHandler :: %s : %s', 'myStr', myStr)
        self._myPool = pool.SimpleConnectionPool(1, 10, myStr)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self._myPool:
            self._myPool.closeall()

    def getPrices(self, aRegion):
        id = self.getRegionId(aRegion)
        if id:
            dict = sql.processFunctionOnDb(self._myPool,
                                           self._logger, sql.getPricesSQL, id)
            return dict
        else:
            return None

    def addPrices(self, aRegion, aDict):
        id = self.getRegionId(aRegion)
        self._logger.info('DatabaseHandler :: addPrices : id : %s', id)
        if id is None:
            id = self.addRegionToDb(aRegion)
        self._logger.info(
            'DatabaseHandler :: addPrices : values deleted : %s',
            self.deleteValues(id))
        sql.processFunctionOnDb(self._myPool,
                                self._logger, sql.addPricesSQL, id, aDict)

    def getRegionId(self, aRegion):
        result = sql.processFunctionOnDb(self._myPool,
                                         self._logger,
                                         sql.checkForRegionSQL, aRegion)
        if result:
            return result
        else:
            None

    def addRegionToDb(self, aRegion):
        result = sql.processFunctionOnDb(self._myPool,
                                         self._logger,
                                         sql.addRegionToDbSQL, aRegion)
        return result

    def deleteValues(self, aId):
        return sql.processFunctionOnDb(self._myPool,
                                       self._logger, sql.deleteValuesSQL, aId)
