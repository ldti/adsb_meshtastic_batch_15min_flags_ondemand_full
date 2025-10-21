#!/usr/bin/env python3
"""
ADS-B (dump1090 SBS) → Meshtastic bridge (15-minute batch mode, with flags, on-demand connect, 1 s send delay)

• Collects aircraft data from dump1090 SBS stream.
• Every N minutes (default 15) builds a compact summary ≤ MAX_MESSAGE_BYTES (UTF-8 safe).
• Connects to Meshtastic only when sending → waits 1 second → sends → disconnects.
• Includes flag emojis using PyModeS or the built-in ICAO allocation table.

Environment:
  DUMP1090_HOST, DUMP1090_PORT, MESHTASTIC_TCP_HOST, MESHTASTIC_CHANNEL_INDEX,
  BATCH_MINUTES, MAX_MESSAGE_BYTES, LOG_LEVEL, STATION_LAT, STATION_LON
"""

import argparse
import logging
import os
import random
import socket
import threading
import time
from typing import Dict, Optional, Tuple, List

from meshtastic.tcp_interface import TCPInterface

try:
    import pycountry
except Exception:
    pycountry = None

try:
    import pyModeS as pms  # type: ignore
except Exception:
    pms = None  # type: ignore

# ---------------------------
# Defaults
# ---------------------------
DEFAULT_DUMP1090_HOST = os.getenv("DUMP1090_HOST", "10.200.10.18")
DEFAULT_DUMP1090_PORT = int(os.getenv("DUMP1090_PORT", "30103"))
DEFAULT_MESH_HOST = os.getenv("MESHTASTIC_TCP_HOST", "10.200.10.16")
DEFAULT_MESH_CHANNEL_INDEX = int(os.getenv("MESHTASTIC_CHANNEL_INDEX", "4"))
DEFAULT_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
DEFAULT_BATCH_MINUTES = int(os.getenv("BATCH_MINUTES", "15"))
DEFAULT_MAX_BYTES = int(os.getenv("MAX_MESSAGE_BYTES", "200"))

STATION_LAT = float(os.getenv("STATION_LAT")) if os.getenv("STATION_LAT") else None
STATION_LON = float(os.getenv("STATION_LON")) if os.getenv("STATION_LON") else None

# ---------------------------
# ICAO allocation table (insert your full table here)
# ---------------------------
ICAO_ALLOC_RANGES = [
    {"start": int("000000",16), "end": int("003FFF",16), "country": "Unassigned"},
    {"start": int("004000",16), "end": int("0043FF",16), "country": "Zimbabwe"},
    {"start": int("006000",16), "end": int("006FFF",16), "country": "Mozambique"},
    {"start": int("008000",16), "end": int("00FFFF",16), "country": "South Africa"},
    {"start": int("010000",16), "end": int("017FFF",16), "country": "Egypt"},
    {"start": int("018000",16), "end": int("01FFFF",16), "country": "Libya"},
    {"start": int("020000",16), "end": int("027FFF",16), "country": "Morocco"},
    {"start": int("028000",16), "end": int("02FFFF",16), "country": "Tunisia"},
    {"start": int("030000",16), "end": int("0303FF",16), "country": "Botswana"},
    {"start": int("032000",16), "end": int("032FFF",16), "country": "Burundi"},
    {"start": int("034000",16), "end": int("034FFF",16), "country": "Cameroon"},
    {"start": int("035000",16), "end": int("0353FF",16), "country": "Comoros"},
    {"start": int("036000",16), "end": int("036FFF",16), "country": "Congo"},
    {"start": int("038000",16), "end": int("038FFF",16), "country": "Cote d'Ivoire"},
    {"start": int("03E000",16), "end": int("03EFFF",16), "country": "Gabon"},
    {"start": int("040000",16), "end": int("040FFF",16), "country": "Ethiopia"},
    {"start": int("042000",16), "end": int("042FFF",16), "country": "Equatorial Guinea"},
    {"start": int("044000",16), "end": int("044FFF",16), "country": "Ghana"},
    {"start": int("046000",16), "end": int("046FFF",16), "country": "Guinea"},
    {"start": int("048000",16), "end": int("0483FF",16), "country": "Guinea-Bissau"},
    {"start": int("04A000",16), "end": int("04A3FF",16), "country": "Lesotho"},
    {"start": int("04C000",16), "end": int("04CFFF",16), "country": "Kenya"},
    {"start": int("050000",16), "end": int("050FFF",16), "country": "Liberia"},
    {"start": int("054000",16), "end": int("054FFF",16), "country": "Madagascar"},
    {"start": int("058000",16), "end": int("058FFF",16), "country": "Malawi"},
    {"start": int("05A000",16), "end": int("05A3FF",16), "country": "Maldives"},
    {"start": int("05C000",16), "end": int("05CFFF",16), "country": "Mali"},
    {"start": int("05E000",16), "end": int("05E3FF",16), "country": "Mauritania"},
    {"start": int("060000",16), "end": int("0603FF",16), "country": "Mauritius"},
    {"start": int("062000",16), "end": int("062FFF",16), "country": "Niger"},
    {"start": int("064000",16), "end": int("064FFF",16), "country": "Nigeria"},
    {"start": int("068000",16), "end": int("068FFF",16), "country": "Uganda"},
    {"start": int("06A000",16), "end": int("06A3FF",16), "country": "Qatar"},
    {"start": int("06C000",16), "end": int("06CFFF",16), "country": "Central African Republic"},
    {"start": int("06E000",16), "end": int("06EFFF",16), "country": "Rwanda"},
    {"start": int("070000",16), "end": int("070FFF",16), "country": "Senegal"},
    {"start": int("074000",16), "end": int("0743FF",16), "country": "Seychelles"},
    {"start": int("076000",16), "end": int("0763FF",16), "country": "Sierra Leone"},
    {"start": int("078000",16), "end": int("078FFF",16), "country": "Somalia"},
    {"start": int("07A000",16), "end": int("07A3FF",16), "country": "Eswatini"},
    {"start": int("07C000",16), "end": int("07CFFF",16), "country": "Sudan"},
    {"start": int("080000",16), "end": int("080FFF",16), "country": "Tanzania"},
    {"start": int("084000",16), "end": int("084FFF",16), "country": "Chad"},
    {"start": int("088000",16), "end": int("088FFF",16), "country": "Togo"},
    {"start": int("08A000",16), "end": int("08AFFF",16), "country": "Zambia"},
    {"start": int("08C000",16), "end": int("08CFFF",16), "country": "Democratic Republic of the Congo"},
    {"start": int("090000",16), "end": int("090FFF",16), "country": "Angola"},
    {"start": int("094000",16), "end": int("0943FF",16), "country": "Benin"},
    {"start": int("096000",16), "end": int("0963FF",16), "country": "Cape Verde"},
    {"start": int("098000",16), "end": int("0983FF",16), "country": "Djibouti"},
    {"start": int("09A000",16), "end": int("09AFFF",16), "country": "The Gambia"},
    {"start": int("09C000",16), "end": int("09CFFF",16), "country": "Burkina Faso"},
    {"start": int("09E000",16), "end": int("09E3FF",16), "country": "Sao Tome and Principe"},
    {"start": int("0A0000",16), "end": int("0A7FFF",16), "country": "Algeria"},
    {"start": int("0A8000",16), "end": int("0A8FFF",16), "country": "Bahamas"},
    {"start": int("0AA000",16), "end": int("0AA3FF",16), "country": "Barbados"},
    {"start": int("0AB000",16), "end": int("0AB3FF",16), "country": "Belize"},
    {"start": int("0AC000",16), "end": int("0ACFFF",16), "country": "Colombia"},
    {"start": int("0AE000",16), "end": int("0AEFFF",16), "country": "Costa Rica"},
    {"start": int("0B0000",16), "end": int("0B0FFF",16), "country": "Cuba"},
    {"start": int("0B2000",16), "end": int("0B2FFF",16), "country": "El Salvador"},
    {"start": int("0B4000",16), "end": int("0B4FFF",16), "country": "Guatemala"},
    {"start": int("0B6000",16), "end": int("0B6FFF",16), "country": "Guyana"},
    {"start": int("0B8000",16), "end": int("0B8FFF",16), "country": "Haiti"},
    {"start": int("0BA000",16), "end": int("0BAFFF",16), "country": "Honduras"},
    {"start": int("0BC000",16), "end": int("0BC3FF",16), "country": "Saint Vincent and the Grenadines"},
    {"start": int("0BE000",16), "end": int("0BEFFF",16), "country": "Jamaica"},
    {"start": int("0C0000",16), "end": int("0C0FFF",16), "country": "Nicaragua"},
    {"start": int("0C2000",16), "end": int("0C2FFF",16), "country": "Panama"},
    {"start": int("0C4000",16), "end": int("0C4FFF",16), "country": "Dominican Republic"},
    {"start": int("0C6000",16), "end": int("0C6FFF",16), "country": "Trinidad and Tobago"},
    {"start": int("0C8000",16), "end": int("0C8FFF",16), "country": "Suriname"},
    {"start": int("0CA000",16), "end": int("0CA3FF",16), "country": "Antigua and Barbuda"},
    {"start": int("0CC000",16), "end": int("0CC3FF",16), "country": "Grenada"},
    {"start": int("0D0000",16), "end": int("0D7FFF",16), "country": "Mexico"},
    {"start": int("0D8000",16), "end": int("0DFFFF",16), "country": "Venezuela"},
    {"start": int("100000",16), "end": int("1FFFFF",16), "country": "Russian Federation"},
    {"start": int("300000",16), "end": int("33FFFF",16), "country": "Italy"},
    {"start": int("340000",16), "end": int("37FFFF",16), "country": "Spain"},
    {"start": int("380000",16), "end": int("3BFFFF",16), "country": "France"},
    {"start": int("3C0000",16), "end": int("3FFFFF",16), "country": "Germany"},
    {"start": int("400000",16), "end": int("43FFFF",16), "country": "United Kingdom"},
    {"start": int("440000",16), "end": int("447FFF",16), "country": "Austria"},
    {"start": int("448000",16), "end": int("44FFFF",16), "country": "Belgium"},
    {"start": int("450000",16), "end": int("457FFF",16), "country": "Bulgaria"},
    {"start": int("458000",16), "end": int("45FFFF",16), "country": "Denmark"},
    {"start": int("460000",16), "end": int("467FFF",16), "country": "Finland"},
    {"start": int("468000",16), "end": int("46FFFF",16), "country": "Greece"},
    {"start": int("470000",16), "end": int("477FFF",16), "country": "Hungary"},
    {"start": int("478000",16), "end": int("47FFFF",16), "country": "Norway"},
    {"start": int("480000",16), "end": int("487FFF",16), "country": "Netherlands"},
    {"start": int("488000",16), "end": int("48FFFF",16), "country": "Poland"},
    {"start": int("490000",16), "end": int("497FFF",16), "country": "Portugal"},
    {"start": int("498000",16), "end": int("49FFFF",16), "country": "Czech Republic"},
    {"start": int("4A0000",16), "end": int("4A7FFF",16), "country": "Romania"},
    {"start": int("4A8000",16), "end": int("4AFFFF",16), "country": "Sweden"},
    {"start": int("4B0000",16), "end": int("4B7FFF",16), "country": "Switzerland"},
    {"start": int("4B8000",16), "end": int("4BFFFF",16), "country": "Türkiye"},
    {"start": int("4C8000",16), "end": int("4C83FF",16), "country": "Cyprus"},
    {"start": int("4CA000",16), "end": int("4CAFFF",16), "country": "Ireland"},
    {"start": int("4CC000",16), "end": int("4CCFFF",16), "country": "Iceland"},
    {"start": int("4D0000",16), "end": int("4D03FF",16), "country": "Luxembourg"},
    {"start": int("4D2000",16), "end": int("4D23FF",16), "country": "Malta"},
    {"start": int("4D4000",16), "end": int("4D43FF",16), "country": "Monaco"},
    {"start": int("500000",16), "end": int("5003FF",16), "country": "San Marino"},
    {"start": int("501000",16), "end": int("5013FF",16), "country": "Albania"},
    {"start": int("501C00",16), "end": int("501FFF",16), "country": "Croatia"},
    {"start": int("502C00",16), "end": int("502FFF",16), "country": "Latvia"},
    {"start": int("503C00",16), "end": int("503FFF",16), "country": "Lithuania"},
    {"start": int("504C00",16), "end": int("504FFF",16), "country": "Republic of Moldova"},
    {"start": int("505C00",16), "end": int("505FFF",16), "country": "Slovakia"},
    {"start": int("506C00",16), "end": int("506FFF",16), "country": "Slovenia"},
    {"start": int("508000",16), "end": int("50FFFF",16), "country": "Ukraine"},
    {"start": int("510000",16), "end": int("5103FF",16), "country": "Belarus"},
    {"start": int("511000",16), "end": int("5113FF",16), "country": "Estonia"},
    {"start": int("512000",16), "end": int("5123FF",16), "country": "North Macedonia"},
    {"start": int("513000",16), "end": int("5133FF",16), "country": "Bosnia and Herzegovina"},
    {"start": int("514000",16), "end": int("5143FF",16), "country": "Georgia"},
    {"start": int("515000",16), "end": int("5153FF",16), "country": "Tajikistan"},
    {"start": int("600000",16), "end": int("6003FF",16), "country": "Armenia"},
    {"start": int("600800",16), "end": int("600BFF",16), "country": "Azerbaijan"},
    {"start": int("601000",16), "end": int("6013FF",16), "country": "Kyrgyzstan"},
    {"start": int("601800",16), "end": int("601BFF",16), "country": "Turkmenistan"},
    {"start": int("680000",16), "end": int("6803FF",16), "country": "Bhutan"},
    {"start": int("681000",16), "end": int("6813FF",16), "country": "Micronesia"},
    {"start": int("682000",16), "end": int("6823FF",16), "country": "Mongolia"},
    {"start": int("683000",16), "end": int("6833FF",16), "country": "Kazakhstan"},
    {"start": int("684000",16), "end": int("6843FF",16), "country": "Palau"},
    {"start": int("700000",16), "end": int("700FFF",16), "country": "Afghanistan"},
    {"start": int("702000",16), "end": int("702FFF",16), "country": "Bangladesh"},
    {"start": int("704000",16), "end": int("704FFF",16), "country": "Myanmar"},
    {"start": int("706000",16), "end": int("706FFF",16), "country": "Kuwait"},
    {"start": int("708000",16), "end": int("708FFF",16), "country": "Lao People's Democratic Republic"},
    {"start": int("70A000",16), "end": int("70AFFF",16), "country": "Nepal"},
    {"start": int("70C000",16), "end": int("70C3FF",16), "country": "Oman"},
    {"start": int("70E000",16), "end": int("70EFFF",16), "country": "Cambodia"},
    {"start": int("710000",16), "end": int("717FFF",16), "country": "Saudi Arabia"},
    {"start": int("718000",16), "end": int("71FFFF",16), "country": "Republic of Korea"},
    {"start": int("720000",16), "end": int("727FFF",16), "country": "Democratic People's Republic of Korea"},
    {"start": int("728000",16), "end": int("72FFFF",16), "country": "Iraq"},
    {"start": int("730000",16), "end": int("737FFF",16), "country": "Iran (Islamic Republic of)"},
    {"start": int("738000",16), "end": int("73FFFF",16), "country": "Israel"},
    {"start": int("740000",16), "end": int("747FFF",16), "country": "Jordan"},
    {"start": int("748000",16), "end": int("74FFFF",16), "country": "Lebanon"},
    {"start": int("750000",16), "end": int("757FFF",16), "country": "Malaysia"},
    {"start": int("758000",16), "end": int("75FFFF",16), "country": "Philippines"},
    {"start": int("760000",16), "end": int("767FFF",16), "country": "Pakistan"},
    {"start": int("768000",16), "end": int("76FFFF",16), "country": "Singapore"},
    {"start": int("770000",16), "end": int("777FFF",16), "country": "Sri Lanka"},
    {"start": int("778000",16), "end": int("77FFFF",16), "country": "Syrian Arab Republic"},
    {"start": int("780000",16), "end": int("7BFFFF",16), "country": "China"},
    {"start": int("7C0000",16), "end": int("7FFFFF",16), "country": "Australia"},
    {"start": int("800000",16), "end": int("83FFFF",16), "country": "India"},
    {"start": int("840000",16), "end": int("87FFFF",16), "country": "Japan"},
    {"start": int("880000",16), "end": int("887FFF",16), "country": "Thailand"},
    {"start": int("888000",16), "end": int("88FFFF",16), "country": "Viet Nam"},
    {"start": int("890000",16), "end": int("890FFF",16), "country": "Yemen"},
    {"start": int("894000",16), "end": int("894FFF",16), "country": "Bahrain"},
    {"start": int("895000",16), "end": int("8953FF",16), "country": "Brunei Darussalam"},
    {"start": int("896000",16), "end": int("896FFF",16), "country": "United Arab Emirates"},
    {"start": int("897000",16), "end": int("8973FF",16), "country": "Solomon Islands"},
    {"start": int("898000",16), "end": int("898FFF",16), "country": "Papua New Guinea"},
    {"start": int("899000",16), "end": int("8993FF",16), "country": "Taiwan, Province of China"},
    {"start": int("8A0000",16), "end": int("8A7FFF",16), "country": "Indonesia"},
    {"start": int("900000",16), "end": int("9003FF",16), "country": "Marshall Islands"},
    {"start": int("901000",16), "end": int("9013FF",16), "country": "Cook Islands"},
    {"start": int("902000",16), "end": int("9023FF",16), "country": "Samoa"},
    {"start": int("A00000",16), "end": int("AFFFFF",16), "country": "United States"},
    {"start": int("C00000",16), "end": int("C3FFFF",16), "country": "Canada"},
    {"start": int("C80000",16), "end": int("C87FFF",16), "country": "New Zealand"},
    {"start": int("C88000",16), "end": int("C88FFF",16), "country": "Fiji"},
    {"start": int("C8A000",16), "end": int("C8A3FF",16), "country": "Nauru"},
    {"start": int("C8C000",16), "end": int("C8C3FF",16), "country": "Saint Lucia"},
    {"start": int("C8D000",16), "end": int("C8D3FF",16), "country": "Tonga"},
    {"start": int("C8E000",16), "end": int("C8E3FF",16), "country": "Kiribati"},
    {"start": int("C90000",16), "end": int("C903FF",16), "country": "Vanuatu"},
    {"start": int("D00000",16), "end": int("DFFFFF",16), "country": "Reserved"},
    {"start": int("E00000",16), "end": int("E3FFFF",16), "country": "Argentina"},
    {"start": int("E40000",16), "end": int("E7FFFF",16), "country": "Brazil"},
    {"start": int("E80000",16), "end": int("E80FFF",16), "country": "Chile"},
    {"start": int("E84000",16), "end": int("E84FFF",16), "country": "Ecuador"},
    {"start": int("E88000",16), "end": int("E88FFF",16), "country": "Paraguay"},
    {"start": int("E8C000",16), "end": int("E8CFFF",16), "country": "Peru"},
    {"start": int("E90000",16), "end": int("E90FFF",16), "country": "Uruguay"},
    {"start": int("E94000",16), "end": int("E94FFF",16), "country": "Bolivia (Plurinational State of)"},
]
# ---------------------------
# Globals
# ---------------------------
aircraft_data: Dict[str, dict] = {}
batch_window_start = time.time()
data_lock = threading.Lock()
stop_event = threading.Event()

# ---------------------------
# Helpers
# ---------------------------
def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

def kft(alt_ft: Optional[int]) -> str:
    try:
        if alt_ft is None:
            return "?k"
        return f"{max(int(round(alt_ft / 1000.0)), 0)}k"
    except Exception:
        return "?k"

def human_coords_short(lat: Optional[float], lon: Optional[float]) -> str:
    try:
        if lat is None or lon is None:
            return "?,?"
        return f"{lat:.2f},{lon:.2f}"
    except Exception:
        return "?,?"

def flag_emoji(cc: Optional[str]) -> Optional[str]:
    if not cc or len(cc) != 2:
        return None
    base = 0x1F1E6
    a = ord(cc[0].upper()) - ord("A")
    b = ord(cc[1].upper()) - ord("A")
    if not (0 <= a < 26 and 0 <= b < 26):
        return None
    return chr(base + a) + chr(base + b)

def _country_to_cc(country: Optional[str]) -> Optional[str]:
    if not country:
        return None
    aliases = {
        "Viet Nam": "VN",
        "Türkiye": "TR",
        "Cote d'Ivoire": "CI",
        "Cote d Ivoire": "CI",
        "Swaziland": "SZ",
        "Laos": "LA",
        "Lao": "LA",
        "Burma": "MM",
        "Myanmar": "MM",
        "North Korea": "KP",
        "South Korea": "KR",
        "Taiwan (unofficial)": "TW",
    }
    if country in aliases:
        return aliases[country]
    if not pycountry:
        return None
    try:
        entry = pycountry.countries.lookup(country)
        return entry.alpha_2
    except Exception:
        return None

def _modes_country_lookup(icao_hex: str) -> Optional[str]:
    if not pms:
        return None
    icao_hex = (icao_hex or "").strip().upper()
    if len(icao_hex) != 6:
        return None
    try:
        c = pms.icao.country(icao_hex)
        if c:
            return c
    except Exception:
        pass
    try:
        icao24_mod = getattr(pms, "icao24", None)
        if icao24_mod:
            c = icao24_mod.country(icao_hex)
            if c:
                return c
    except Exception:
        pass
    return None

def registration_country_for_icao(hex_addr: str) -> Tuple[Optional[str], Optional[str]]:
    ctry = _modes_country_lookup(hex_addr)
    if ctry:
        return ctry, _country_to_cc(ctry)
    try:
        x = int((hex_addr or "").strip(), 16)
    except Exception:
        return (None, None)
    for r in ICAO_ALLOC_RANGES:
        if r["start"] <= x <= r["end"]:
            ctry = r["country"]
            return ctry, _country_to_cc(ctry)
    return (None, None)

def bounded_position(lat: float, lon: float) -> bool:
    return -90 <= lat <= 90 and -180 <= lon <= 180

# ---------------------------
# Meshtastic: connect → delay → send → close
# ---------------------------
def send_meshtastic_text(mesh_host: str, channel_index: int, text: str) -> bool:
    """Connect to Meshtastic, wait 1s, send, then close."""
    try:
        iface = TCPInterface(hostname=mesh_host)
        logging.debug("Connected to Meshtastic, waiting 1 second before send...")
        time.sleep(1.0)
        iface.sendText(text, channelIndex=channel_index)
        iface.close()
        logging.info("Meshtastic: sent %d bytes", len(text.encode("utf-8")))
        return True
    except Exception:
        logging.exception("Meshtastic send failed")
        return False

# ---------------------------
# SBS reader
# ---------------------------
def sbs_reader(host: str, port: int):
    backoff = 2.0
    while not stop_event.is_set():
        s = None
        try:
            logging.info("Connecting to dump1090 SBS at %s:%d ...", host, port)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(15)
            s.connect((host, port))
            logging.info("Connected to dump1090 SBS. Reading...")
            with s.makefile("r", encoding="utf-8", newline="") as f:
                for line in f:
                    if stop_event.is_set():
                        break
                    parts = [p.strip() for p in line.split(",")]
                    if not parts or parts[0] != "MSG":
                        continue
                    try:
                        tx_type = int(parts[1])
                        icao = parts[4].upper()
                    except Exception:
                        continue
                    now = time.time()
                    with data_lock:
                        st = aircraft_data.setdefault(
                            icao,
                            {"lat": None, "lon": None, "alt": None, "callsign": None, "last_seen": 0.0},
                        )
                        st["last_seen"] = now
                        if tx_type in (1, 2, 3, 4) and len(parts) > 10:
                            cs = parts[10].strip().upper()
                            if 2 <= len(cs) <= 8:
                                st["callsign"] = cs
                        if tx_type in (2, 3) and len(parts) > 15:
                            try:
                                alt = int(float(parts[11])) if parts[11] else None
                                lat = float(parts[14]) if parts[14] else None
                                lon = float(parts[15]) if parts[15] else None
                            except Exception:
                                alt = lat = lon = None
                            if lat is not None and lon is not None and bounded_position(lat, lon):
                                st["lat"], st["lon"] = lat, lon
                            if alt is not None and alt >= 100:
                                st["alt"] = alt
        except Exception as e:
            logging.error("SBS connection error: %s", e)
        finally:
            if s:
                try:
                    s.close()
                except Exception:
                    pass
            delay = min(backoff, 30) + random.uniform(0, 1.5)
            logging.info("Reconnecting to SBS in %.1fs", delay)
            stop_event.wait(delay)
            backoff = min(backoff * 1.7, 30)

# ---------------------------
# Batch building + byte-safe limiter
# ---------------------------
def _pick_examples(rows: List[Tuple[str, dict]], max_examples: int = 5):
    rows = sorted(rows, key=lambda x: (0 if x[1].get("callsign") else 1, -x[1].get("last_seen", 0)))
    return rows[:max_examples]

def _ident_for(icao: str, st: dict) -> str:
    return st.get("callsign") or icao

def _flag_for_icao(icao: str) -> str:
    country, cc = registration_country_for_icao(icao)
    f = flag_emoji(cc)
    return f + " " if f else ""

def build_summary(now: float, window_start: float) -> str:
    with data_lock:
        rows = [(icao, st.copy()) for icao, st in aircraft_data.items() if st.get("last_seen", 0) >= window_start]
    count = len(rows)
    if count == 0:
        return "✈️ 15m: no planes"
    examples = _pick_examples(rows, 6)
    parts = []
    for icao, st in examples:
        parts.append(f"{_flag_for_icao(icao)}{_ident_for(icao, st)} {kft(st.get('alt'))} {human_coords_short(st.get('lat'), st.get('lon'))}")
    head = f"✈️ {int(round((now - window_start) / 60.0))}m: {count} planes. "
    msg = head + "; ".join(parts)
    return msg

def hard_limit_bytes(msg: str, max_bytes: int) -> str:
    b = msg.encode("utf-8")
    if len(b) <= max_bytes:
        return msg
    ell = "…".encode("utf-8")
    keep = max_bytes - len(ell)
    if keep <= 0:
        return "…"
    trimmed = b[:keep]
    while True:
        try:
            return trimmed.decode("utf-8") + "…"
        except UnicodeDecodeError:
            trimmed = trimmed[:-1]

def batch_sender(mesh_host: str, channel_index: int, batch_minutes: int, max_bytes: int):
    global batch_window_start
    while not stop_event.is_set():
        stop_event.wait(batch_minutes * 60)
        if stop_event.is_set():
            break
        now = time.time()
        try:
            msg = build_summary(now, batch_window_start)
            msg = hard_limit_bytes(msg, max_bytes)
            ok = send_meshtastic_text(mesh_host, channel_index, msg)
            if ok:
                logging.info("Batch sent (%d bytes)", len(msg.encode("utf-8")))
            else:
                logging.warning("Batch NOT sent")
        except Exception:
            logging.exception("Failed to build/send batch message")
        finally:
            batch_window_start = now

# ---------------------------
# CLI & main
# ---------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Bridge dump1090 SBS → Meshtastic (15 min batches, 1 s delay)")
    p.add_argument("--dump1090-host", default=DEFAULT_DUMP1090_HOST)
    p.add_argument("--dump1090-port", type=int, default=DEFAULT_DUMP1090_PORT)
    p.add_argument("--mesh-host", default=DEFAULT_MESH_HOST)
    p.add_argument("--mesh-channel-index", type=int, default=DEFAULT_MESH_CHANNEL_INDEX)
    p.add_argument("--batch-minutes", type=int, default=DEFAULT_BATCH_MINUTES)
    p.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_BYTES)
    p.add_argument("--log-level", default=DEFAULT_LOG_LEVEL, choices=["DEBUG","INFO","WARNING","ERROR"])
    p.add_argument("--self-test", action="store_true", help="Build a synthetic message and exit")
    return p.parse_args()

def main():
    args = parse_args()
    setup_logging(args.log_level)
    if args.self_test:
        global batch_window_start
        batch_window_start = time.time() - 15 * 60
        with data_lock:
            for i, (icao, cs) in enumerate(
                [("738ABC","ELY32A"),("A1B2C3","N123AB"),("400123","BAW9"),("3C45F6","DLH4"),("4B8A9C","THY7"),("7C0F0F","QFA2")], 1):
                aircraft_data[icao] = {"callsign": cs, "alt": 29000+i*1000,
                                       "lat": 32+i*0.07, "lon": 34.8+i*0.06,
                                       "last_seen": time.time()}
        msg = build_summary(time.time(), batch_window_start)
        print(hard_limit_bytes(msg, args.max_bytes))
        return
    logging.info("SBS → Meshtastic | dump1090=%s:%d | mesh=%s | idx=%d | batch=%dm | max=%d bytes",
                 args.dump1090_host,args.dump1090_port,args.mesh_host,args.mesh_channel_index,
                 args.batch_minutes,args.max_bytes)
    threading.Thread(target=sbs_reader,args=(args.dump1090_host,args.dump1090_port),
                     daemon=True,name="sbs-reader").start()
    threading.Thread(target=batch_sender,args=(args.mesh_host,args.mesh_channel_index,
                     args.batch_minutes,args.max_bytes),daemon=True,name="batch-sender").start()
    try:
        while not stop_event.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        logging.info("Shutdown requested (Ctrl+C)")
    finally:
        stop_event.set()
        time.sleep(0.5)
        logging.info("Shutdown complete")

if __name__ == "__main__":
    main()
