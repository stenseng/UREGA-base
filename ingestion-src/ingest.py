#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Lars Stenseng
@mail: lars@stenseng.net
"""

import argparse
import asyncio
import logging
from psycopg2 import connect, Error
from ntripstreams import NtripStream, Rtcm3
from signal import SIGINT, SIGTERM, signal
from time import strftime, gmtime

from settings import DbSettings, CasterSettings


def procSigint(signum, frame):
    logging.warning("Received SIGINT. Shutting down, Adjø!")
    exit(3)


def procSigterm(signum, frame):
    logging.warning("Received SIGTERM. Shutting down, Adjø!")
    exit(4)


signal(SIGINT, procSigint)
signal(SIGTERM, procSigterm)


async def procRtcmStream(
    casterSettings: CasterSettings,
    mountPoint: str,
    dbSettings: DbSettings = None,
    fail: int = 0,
    retry: int = 3
):
    dbConnection = None
    ntripstream = NtripStream()
    rtcmMessage = Rtcm3()
    try:
        await ntripstream.requestNtripStream(
            casterSettings.casterUrl,
            mountPoint,
            casterSettings.user,
            casterSettings.password
        )
    except (OSError):
        sleepTime = 30
        logging.error(f"Will retry NTRIP connection in {sleepTime} seconds!")
        await asyncio.sleep(sleepTime)
        await procRtcmStream(casterSettings, mountPoint, dbSettings, fail)
    if dbSettings:
        try:
            dbConnection = dbConnect(dbSettings)
            dbCursor = dbConnection.cursor()
        except (Exception, Error) as error:
            logging.error(
                "Failed to connect to database server: "
                f"{dbSettings.database}@{dbSettings.host} "
                f"with error: {error}"
            )
            logging.error(f"Will retry database connection in {sleepTime} seconds!")
            if dbConnection:
                dbCursor.close()
                dbConnection.close()
            await asyncio.sleep(sleepTime)
            await procRtcmStream(casterSettings, mountPoint, dbSettings, fail)
        logging.info(f"Connected to database: {dbSettings.database}@{dbSettings.host}.")
    while True:
        try:
            rtcmFrame, timeStamp = await ntripstream.getRtcmFrame()
        except (ConnectionError, IOError):
            if dbConnection:
                dbCursor.close()
                dbConnection.close()
            if fail >= retry:
                fail += 1
                sleepTime = 5 * fail
                if sleepTime > 300:
                    sleepTime = 300
                logging.error(
                    f"{mountPoint}: {fail} failed attempt to reconnect. "
                    f"Will retry in {sleepTime} seconds!"
                )
                await asyncio.sleep(sleepTime)
                await procRtcmStream(casterSettings, mountPoint, dbSettings, fail)
            else:
                fail += 1
                logging.warning(f"{mountPoint}: Reconnecting. Attempt no. {fail}.")
                await asyncio.sleep(2)
                await procRtcmStream(casterSettings, mountPoint, dbSettings, fail)
        fail = 0
        try:
            messageType, data = rtcmMessage.decodeRtcmFrame(rtcmFrame)
            description = rtcmMessage.messageDescription(messageType)
        except Exception:
            logging.info("Failed to decode RTCM frame.")
            break
        logging.debug(f"{mountPoint}: RTCM message #: {messageType} '{description}'.")
        messageSize = len(rtcmFrame)
        obsEpoch = None
        satCount = None
        rtcmPackageId = None
        if (
            (messageType >= 1001 and messageType <= 1004)
            or (messageType >= 1009 and messageType <= 1012)
            or (messageType >= 1071 and messageType <= 1077)
            or (messageType >= 1081 and messageType <= 1087)
            or (messageType >= 1091 and messageType <= 1097)
            or (messageType >= 1101 and messageType <= 1107)
            or (messageType >= 1111 and messageType <= 1117)
            or (messageType >= 1121 and messageType <= 1127)
        ):
            satCount = len(data[1])
            if messageType >= 1001 and messageType <= 1004:
                obsEpoch = data[0][2] / 1000.0
            elif messageType >= 1009 and messageType <= 1012:
                obsEpoch = data[0][2] / 1000.0
            elif messageType >= 1071 and messageType <= 1127:
                obsEpoch = data[0][2] / 1000.0
        if dbConnection:
            us = int(timeStamp % 1 * 1000000)
            timeStampStr = strftime(
                f"%Y-%m-%d %H:%M:%S.{us}", gmtime(timeStamp)
            )
            try:
                dbCursor.execute(
                    "INSERT INTO rtcm_packages"
                    "(receive_time, mountpoint, rtcm_obs_epoch, rtcm_msg_type, "
                    "rtcm_msg_size, rtcm_sat_count) VALUES (%s, %s, %s, %s, %s, %s) "
                    "RETURNING rtcm_package_id",
                    (
                        timeStampStr,
                        mountPoint,
                        obsEpoch,
                        messageType,
                        messageSize,
                        satCount
                    )
                )
                dbConnection.commit()
                rtcmPackageId = dbCursor.fetchone()[0]
                logging.debug(rtcmPackageId)
            except (Exception, Error) as error:
                logging.error(
                    f"Failed to insert and commit data to databse with: {error}"
                )
                logging.error(
                    f" Data values is: {timeStampStr}, {mountPoint}, {obsEpoch}, "
                    f"{messageType}, {messageSize}"
                )
            if (
                (messageType >= 1001 and messageType <= 1004)
                or (messageType >= 1009 and messageType <= 1012)
                or (messageType >= 1071 and messageType <= 1077)
                or (messageType >= 1081 and messageType <= 1087)
                or (messageType >= 1091 and messageType <= 1097)
                or (messageType >= 1101 and messageType <= 1107)
                or (messageType >= 1111 and messageType <= 1117)
                or (messageType >= 1121 and messageType <= 1127)
            ):
                obsEpochStr = strftime(
                    f"%Y-%m-%d %H:%M:%S.{us}", gmtime(timeStamp)
                )
                try:
                    dbCursor.execute(
                        "INSERT INTO rtcm_packages"
                        "(rtcm_package_id, mountpoint, obs_epoch, rtcm_msg_type, "
                        "sat_id, sat_signal, obs_code, obs_phase, obs_doppler, obs_snr) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ",
                        (
                            rtcmPackageId,
                            mountPoint,
                            obsEpochStr,
                            messageType,
                            satId,
                            satSignal,
                            obsCode,
                            obsPhase,
                            obsDoppler,
                            obsSnr
                        )
                    )
                    dbConnection.commit()
                except (Exception, Error) as error:
                    logging.error(
                        f"Failed to insert and commit data to databse with: {error}"
                    )
                    logging.error(
                        f" Data values is: {timeStampStr}, {mountPoint}, {obsEpoch}, "
                        f"{messageType}, {messageSize}"
                    )
        logging.debug(
            f"{mountPoint} "
            f"{timeStamp} "
            f"#:{messageType} "
            f"Size: {messageSize} "
            f"Epoch: {obsEpoch} "
            f"Sat. count: {satCount}"
        )
    if dbConnection:
        dbCursor.close()
        dbConnection.close()


async def rtcmStreamTasks(casterSettings, dbConnection):
    tasks = {}
    for mountpoint in casterSettings.mountpoints:
        tasks[mountpoint] = asyncio.create_task(
            procRtcmStream(casterSettings, mountpoint, dbConnection,), name=mountpoint
        )
    for mountpoint in casterSettings.mountpoints:
        await tasks[mountpoint]


async def getMountpoints(
    casterSettings: CasterSettings, sleepTime: int = 30, fail: int = 0
):
    ntripstream = NtripStream()
    mountpoints = []
    try:
        sourceTable = await ntripstream.requestSourcetable(casterSettings.casterUrl)
    except ConnectionError:
        fail += 1
        logging.error(
            f"{fail} failed attempt to NTRIP connect to {casterSettings.casterUrl}. "
            "Will retry in {sleepTime} seconds."
        )
        asyncio.sleep(sleepTime)
    except Exception:
        logging.error("Unknown error. Abort monitoring.")
    else:
        for row in sourceTable:
            sourceCols = row.split(sep=";")
            if sourceCols[0] == "STR":
                mountpoints.append(sourceCols[1])
        # casterSettings.mountpoints = mountpoints
        return mountpoints


def dbConnect(dbSettings):
    connection = connect(
        user=dbSettings.user,
        password=dbSettings.password,
        host=dbSettings.host,
        port=dbSettings.port,
        database=dbSettings.database
        )
    return connection


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--check",
        action="store_true",
        help="Check connection to Ntripcaster without committing data to database."
    )
    parser.add_argument(
        "-m",
        "--mountpoint",
        action="append",
        help="Name of mountpoint without leading / (e.g. PNT1)."
    )
    parser.add_argument(
        "-1", "--ntrip1", action="store_true", help="Use Ntrip 1 protocol."
    )
    parser.add_argument(
        "-l", "--logfile", help="Log to file. Default output is terminal."
    )
    parser.add_argument(
        "-v", "--verbosity", action="count", default=0, help="Increase verbosity level."
    )
    args = parser.parse_args()

    logLevel = logging.ERROR
    if args.verbosity == 1:
        logLevel = logging.WARNING
    elif args.verbosity == 2:
        logLevel = logging.INFO
    elif args.verbosity > 2:
        logLevel = logging.DEBUG
    if args.logfile:
        logging.basicConfig(
            level=logLevel,
            filename=args.logfile,
            format="%(asctime)s;%(levelname)s;%(message)s",
        )
    else:
        logging.basicConfig(
            level=logLevel, format="%(asctime)s;%(levelname)s;%(message)s"
        )

    casterSettings = CasterSettings()
    if args.mountpoint:
        casterSettings.mountpoints = args.mountpoint
    if casterSettings.mountpoints == []:
        casterSettings.mountpoints = asyncio.run(getMountpoints(casterSettings))
    logging.debug(f"Using mountpoints: {casterSettings.mountpoints}")
    if args.check:
        dbSettings = None
    else:
        dbSettings = DbSettings()

    asyncio.run(rtcmStreamTasks(casterSettings, dbSettings))
