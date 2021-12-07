#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Lars Stenseng
@mail: lars@stenseng.net
"""

import argparse
import asyncio
import logging
from math import pow
from signal import SIGINT, SIGTERM, signal
from sys import exit
from time import gmtime, strftime

from ntripstreams import NtripStream, Rtcm3
from psycopg2 import Error, connect
from settings import CasterSettings, DbSettings


def procSigint(signum, frame):
    logging.warning("Received SIGINT. Shutting down, Adjø!")
    exit(3)


def procSigterm(signum, frame):
    logging.warning("Received SIGTERM. Shutting down, Adjø!")
    exit(4)


signal(SIGINT, procSigint)
signal(SIGTERM, procSigterm)


def dbInsert(dbCursor, mountPoint, timeStamp, messageSize, messageType, data):
    us = int(timeStamp % 1 * 1000000)
    timeStampStr = strftime(f"%Y-%m-%d %H:%M:%S.{us}", gmtime(timeStamp))
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
        rtcmPackageId = dbInsertRtcmInfo(
            dbCursor,
            mountPoint,
            timeStampStr,
            messageType,
            messageSize,
            obsEpoch,
            satCount,
        )
        dbInsertGnssObs(dbCursor, mountPoint, rtcmPackageId, messageType, data)
    else:
        rtcmPackageId = dbInsertRtcmInfo(
            dbCursor,
            mountPoint,
            timeStampStr,
            messageType,
            messageSize,
        )
    return


def dbInsertRtcmInfo(
    dbCursor,
    mountPoint: str,
    timeStampStr: str,
    messageType: int,
    messageSize: int,
    obsEpoch: float = None,
    satCount: int = None,
):
    rtcmPackageId = None
    try:
        dbCursor.execute(
            "INSERT INTO rtcm_packages"
            "(mountpoint, receive_time, rtcm_obs_epoch, rtcm_msg_type, "
            "rtcm_msg_size, rtcm_sat_count) VALUES (%s, %s, %s, %s, %s, %s) "
            "RETURNING rtcm_package_id",
            (
                mountPoint,
                timeStampStr,
                obsEpoch,
                messageType,
                messageSize,
                satCount,
            ),
        )
        rtcmPackageId = dbCursor.fetchone()[0]
        logging.debug(f"Inserted package with id: {rtcmPackageId} into database.")
    except (Exception, Error) as error:
        logging.error(
            f"Failed to insert and commit data to databse with: {error}"
            f" Data values is: {timeStampStr}, {mountPoint}, {obsEpoch}, "
            f"{messageType}, {messageSize}"
        )
    return rtcmPackageId


def dbInsertGnssObs(dbCursor, mountPoint, rtcmPackageId, messageType, data):
    rtcmData = Rtcm3()
    if (messageType >= 1001 and messageType <= 1004) or (
        messageType >= 1071 and messageType <= 1077
    ):
        satType = "G"
    elif (messageType >= 1009 and messageType <= 1004) or (
        messageType >= 1081 and messageType <= 1087
    ):
        satType = "R"
    elif messageType >= 1091 and messageType <= 1097:
        satType = "E"
    elif messageType >= 1101 and messageType <= 1107:
        satType = "S"
    elif messageType >= 1111 and messageType <= 1117:
        satType = "J"
    elif messageType >= 1121 and messageType <= 1127:
        satType = "C"

    if messageType >= 1071 and messageType <= 1127:
        if messageType % 10 == 5:
            codeFineScaling = pow(2, -24)
            phaseFineScaling = pow(2, -29)
        elif messageType % 10 == 7:
            codeFineScaling = pow(2, -29)
            phaseFineScaling = pow(2, -31)

    obsEpoch = data[0][2] / 1000.0
    us = int(obsEpoch % 1 * 1000000)
    obsEpochStr = strftime(f"%Y-%m-%d %H:%M:%S.{us}", gmtime(obsEpoch))
    satSignals = rtcmData.msmSignalTypes(messageType, data[0][10])
    satCount = len(data[1])
    signalCount = len(satSignals)
    availObsNo = 0
    availObsMask = str(data[0][11])
    for satNo, sat in enumerate(data[1]):
        satRoughRange = sat[0] + sat[2] / 1024.0
        satRoughRangeRate = sat[3]
        for signalNo, satSignal in enumerate(satSignals):
            if availObsMask[satNo * signalCount + signalNo] == "1":
                print(
                    f"messageType {messageType}: {data[0][11]} "
                    f"{availObsNo} {satNo} {signalNo} "
                    f"{len(data[2][availObsNo])} "
                )
                availObsNo += 1
            satId = f"{satType}{satNo}"
            # # obsCode = satRoughRange + data[2][satNo][signalNo][0] * codeFineScaling
            # # obsPhase = satRoughRange + data[2][satNo][signalNo][1] * phaseFineScaling
            # # obsDoppler = satRoughRangeRate + data[2][satNo][signalNo][5]
            # # obsSnr = data[2][satNo][signalNo][4]
            # obsCode = data[2][availObsNo][0]
            # obsPhase = (
            #     f"{len(data[2])} {len(data[2][satNo])} {data[2][satNo][availObsNo]}"
            # )
            # obsDoppler = data[0][-1]
            # obsSnr = obsCode
            # print(
            #     f"rtcmPackageId:{rtcmPackageId},"
            #     f"mountPoint:{mountPoint},"
            #     f"obsEpochStr:{obsEpochStr},"
            #     f"messageType:{messageType},"
            #     f"satId:{satId},"
            #     f"satSignal:{satSignal},"
            #     f"obsCode:{obsCode},"
            #     f"obsPhase:{obsPhase},"
            #     f"obsDoppler:{obsDoppler},"
            #     f"obsSnr:{obsSnr}"
            # )
            # availObsNo += 1
        # try:
        #     dbCursor.execute(
        #         "INSERT INTO gnss_observations"
        #         "(rtcm_package_id, mountpoint, obs_epoch, rtcm_msg_type, "
        #         "sat_id, sat_signal, obs_code, obs_phase, obs_doppler, obs_snr) "
        #         "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ",
        #         (
        #             rtcmPackageId,
        #             mountPoint,
        #             obsEpochStr,
        #             messageType,
        #             satId,
        #             satSignal,
        #             obsCode,
        #             obsPhase,
        #             obsDoppler,
        #             obsSnr,
        #         )
        #     )
        # except (Exception, Error) as error:
        #     logging.error(f"Failed to insert and commit data to databse with: {error}")


async def procRtcmStream(
    casterSettings: CasterSettings,
    mountPoint: str,
    dbSettings: DbSettings = None,
    fail: int = 0,
    retry: int = 3,
):
    dbConnection = None
    ntripstream = NtripStream()
    rtcmMessage = Rtcm3()
    try:
        await ntripstream.requestNtripStream(
            casterSettings.casterUrl,
            mountPoint,
            casterSettings.user,
            casterSettings.password,
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
        except Exception:
            logging.info("Failed to decode RTCM frame.")
            break
        logging.debug(
            f"{mountPoint}: RTCM message #: {messageType}"
            f" '{rtcmMessage.messageDescription(messageType)}'."
        )
        dbInsert(dbCursor, mountPoint, timeStamp, len(rtcmFrame), messageType, data)
    if dbConnection:
        dbCursor.close()
        dbConnection.close()


async def rtcmStreamTasks(casterSettings, dbConnection):
    tasks = {}
    for mountpoint in casterSettings.mountpoints:
        tasks[mountpoint] = asyncio.create_task(
            procRtcmStream(
                casterSettings,
                mountpoint,
                dbConnection,
            ),
            name=mountpoint,
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


def dbConnect(dbSettings: DbSettings):
    connection = connect(
        user=dbSettings.user,
        password=dbSettings.password,
        host=dbSettings.host,
        port=dbSettings.port,
        database=dbSettings.database,
    )
    connection.autocommit = True
    return connection


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--check",
        action="store_true",
        help="Check connection to Ntripcaster without committing data to database.",
    )
    parser.add_argument(
        "-m",
        "--mountpoint",
        action="append",
        help="Name of mountpoint without leading / (e.g. PNT1).",
    )
    parser.add_argument(
        "-1",
        "--ntrip1",
        action="store_true",
        help="Use Ntrip 1 protocol.",
    )
    parser.add_argument(
        "-l",
        "--logfile",
        help="Log to file. Default output is terminal.",
    )
    parser.add_argument(
        "-v",
        "--verbosity",
        action="count",
        default=0,
        help="Increase verbosity level.",
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
