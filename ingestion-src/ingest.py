#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RTCM ingestion for UREGA.

@author: Lars Stenseng
@mail: lars@stenseng.net
"""

import argparse
import asyncio
import logging
from math import pow
from signal import ITIMER_REAL, SIGALRM, SIGINT, SIGTERM, setitimer, signal
from sys import exit
from time import gmtime, strftime, time
from typing import FrameType

from ntripstreams import NtripStream, Rtcm3
from psycopg2 import Error, connect, extras
from settings import CasterSettings, DbSettings


def procSigint(signum: int, frame: FrameType) -> None:
    """
    Signal handler for interrupt.

    Parameters
    ----------
    signum : int
        DESCRIPTION.
    frame : FrameType
        DESCRIPTION.

    Returns
    -------
    None
        DESCRIPTION.

    """
    logging.warning("Received SIGINT. Shutting down, Adjø!")
    exit(3)


def procSigterm(signum: int, frame: FrameType) -> None:
    """
    Signal handler for terminate.

    Parameters
    ----------
    signum : int
        DESCRIPTION.
    frame : FrameType
        DESCRIPTION.

    Returns
    -------
    None
        DESCRIPTION.

    """
    logging.warning("Received SIGTERM. Shutting down, Adjø!")
    exit(4)


def watchdogHandler(signum: int, frame: FrameType) -> None:
    """
    Signal handler for the watchdog.

    The watchdog checks which asyncio tasks that are running and restarts requested
    tasks.

    Parameters
    ----------
    signum : int
        DESCRIPTION.
    frame : FrameType
        DESCRIPTION.

    Returns
    -------
    None
        DESCRIPTION.

    """
    runningTasks = asyncio.all_tasks()
    runningTaskNames = [runningTask.get_name() for runningTask in runningTasks]
    if len(runningTasks) - 1 <= len(casterSettings.mountpoints):
        wantedTaskNames = []
        for wantedTask in casterSettings.mountpoints:
            if wantedTask not in runningTaskNames:
                wantedTaskNames.append(wantedTask)
                tasks[wantedTask] = asyncio.create_task(
                    procRtcmStream(
                        casterSettings,
                        wantedTask,
                        dbSettings,
                    ),
                    name=wantedTask,
                )
                logging.warning(f"{wantedTask} RTCM stream restarted.")
    return


def gnssEpochStr(messageType: int, obsEpoch: float) -> str:
    """
    Construct a SQL suited date/time string from a GNSS observation epoch.

    The date is adopted from the current computer date. To align the GNSS data with
    UTC date, the GNSS epoch modolus 1 day is compared with computer time modolus 1 day.


    Parameters
    ----------
    messageType : int
        RTCM message type.
    obsEpoch : float
        DESCRIPTION.

    Returns
    -------
    str
        DESCRIPTION.

    """
    now = time()
    nowSecOfDay = now % 86400
    nowSecOfDate = int(now - nowSecOfDay)

    obsSecOfDay = int(obsEpoch % 86400)
    us = int(obsEpoch % 1 * 1000000)

    if (obsSecOfDay - nowSecOfDay) < -5 * 3600:
        obsTime = nowSecOfDate + obsSecOfDay + 86400
    else:
        obsTime = nowSecOfDate + obsSecOfDay
    if (messageType >= 1009 and messageType <= 1012) or (
        messageType >= 1081 and messageType <= 1087
    ):
        obsTime = obsTime - 3 * 3600
    logging.debug(
        f"Msg:{messageType} CPU:{strftime(f'%Y-%m-%d %H:%M:%S.{us} z', gmtime(now))} "
        f"obsTime:{strftime(f'%Y-%m-%d %H:%M:%S.{us} z', gmtime(obsTime))} "
        f"timeDiff:{obsSecOfDay - nowSecOfDay}"
    )
    epochStr = strftime(f"%Y-%m-%d %H:%M:%S.{us} z", gmtime(obsTime))
    return epochStr


def dbInsert(dbCursor, mountPoint, timeStamp, messageSize, messageType, data):
    us = int(timeStamp % 1 * 1000000)
    timeStampStr = strftime(f"%Y-%m-%d %H:%M:%S.{us} z", gmtime(timeStamp))
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
        obsEpochStr = gnssEpochStr(messageType, data[0][2] / 1000.0)
        rtcmPackageId = dbInsertRtcmInfo(
            dbCursor,
            mountPoint,
            timeStampStr,
            messageType,
            messageSize,
            obsEpochStr,
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
    obsEpochStr: str = None,
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
                obsEpochStr,
                messageType,
                messageSize,
                satCount,
            ),
        )
        rtcmPackageId = dbCursor.fetchone()[0]
        dbCursor.connection.commit()
        logging.debug(
            f"Inserted info package with id: {rtcmPackageId} "
            f"and timestamp {timeStampStr} into database."
        )
    except (Exception, Error) as error:
        logging.error(
            f"Failed to insert and commit data to databse with: {error}"
            f" Data values is: {timeStampStr}, {mountPoint}, {obsEpochStr}, "
            f"{messageType}, {messageSize}"
        )
    return rtcmPackageId


def dbInsertGnssObs(dbCursor, mountPoint, rtcmPackageId, messageType, data):
    rtcmData = Rtcm3()
    if (messageType >= 1001 and messageType <= 1004) or (
        messageType >= 1071 and messageType <= 1077
    ):
        satType = "G"
    elif (messageType >= 1009 and messageType <= 1012) or (
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
            snrScaling = 1
        elif messageType % 10 == 7:
            codeFineScaling = pow(2, -29)
            phaseFineScaling = pow(2, -31)
            snrScaling = pow(2, -4)
        obsEpochStr = gnssEpochStr(messageType, data[0][2] / 1000.0)
        satSignals = rtcmData.msmSignalTypes(messageType, data[0][10])
        signalCount = len(satSignals)
        availSatMask = str(data[0][9])
        satId = []
        allObs = []
        for id in range(64):
            if availSatMask[id] == "1":
                satId.append(f"{satType}{id + 1:02d}")
        availObsNo = 0
        availObsMask = str(data[0][11])
        for satNo, sat in enumerate(data[1]):
            satRoughRange = sat[0] + sat[2] / 1024.0
            satRoughRangeRate = sat[3]
            for signalNo, satSignal in enumerate(satSignals):
                if availObsMask[satNo * signalCount + signalNo] == "1":
                    obsCode = satRoughRange + data[2][availObsNo][0] * codeFineScaling
                    obsPhase = satRoughRange + data[2][availObsNo][1] * phaseFineScaling
                    obsLockTimeIndicator = data[2][availObsNo][2]
                    obsSnr = data[2][availObsNo][4] * snrScaling
                    obsDoppler = satRoughRangeRate + data[2][availObsNo][5] * 0.0001
                    allObs.append(
                        (
                            rtcmPackageId,
                            mountPoint,
                            obsEpochStr,
                            messageType,
                            satId[satNo],
                            satSignal,
                            obsCode,
                            obsPhase,
                            obsDoppler,
                            obsSnr,
                            obsLockTimeIndicator,
                        )
                    )
                    availObsNo += 1
        try:
            extras.execute_values(
                dbCursor,
                "INSERT INTO gnss_observations "
                "(rtcm_package_id, mountpoint, obs_epoch, rtcm_msg_type, "
                "sat_id, sat_signal, obs_code, obs_phase, obs_doppler, "
                "obs_snr, obs_lock_time_indicator) "
                "VALUES %s",
                allObs,
            )
            dbCursor.connection.commit()
            logging.debug(
                f"Inserted obs package with id: {rtcmPackageId} "
                f"and timestamp {obsEpochStr} into database."
            )
        except (Exception, Error) as error:
            logging.error(f"Failed to insert and commit data to databse with: {error}")


async def procRtcmStream(
    casterSettings: CasterSettings,
    mountPoint: str,
    dbSettings: DbSettings = None,
    fail: int = 0,
    retry: int = 3,
):
    dbConnection = None
    dbCursor = None
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
            sleepTime = 5
            if fail >= retry:
                fail += 1
                sleepTime = 5 * fail
                if sleepTime > 300:
                    sleepTime = 300
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
    return connection


def main(dbSettings, casterSettings):
    signal(SIGINT, procSigint)
    signal(SIGTERM, procSigterm)
    signal(SIGALRM, watchdogHandler)
    setitimer(ITIMER_REAL, 0.2, 5)

    asyncio.run(rtcmStreamTasks(casterSettings, dbSettings))


tasks = {}
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
    main(dbSettings, casterSettings)
