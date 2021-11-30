#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Lars Stenseng
@mail: lars@stenseng.net
"""

from dataclasses import dataclass


@dataclass
class DbSettings:
    user: str = "postgres"
    password: str = "ShouldB3Secre1Ss0d0Something"
    host: str = "timescaledb"
    port: str = "5432"
    database: str = "UREGA"


@dataclass
class CasterSettings:
    casterUrl: str = "http://ntrip.gnss.org:2101"
    user: str = "ingest"
    password: str = "ingest"
    mountpoints = []
