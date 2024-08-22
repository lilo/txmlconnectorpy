import os
import datetime
import threading
import time
import sys
import random
import pathlib
import logging
import typer
import httpx
from retry import retry
from ctypes import cdll, c_char_p, c_bool, CFUNCTYPE
from sqlalchemy import select, func
from sqlalchemy.orm import Session
from db import engine
from models import Event, Security, Trade, Base
from dotenv import dotenv_values
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element
import xml.dom.minidom
from nthandler import NTEHandler, win32evtlog
from txdll_thread import lib, cmd_connect_ptr, cmd_securities_ptr, log_path_z_ptr, cmd_status_ptr, sfo_code


ntlogger = logging.getLogger('txproxy')
ntlogger.setLevel(logging.DEBUG)
ntehandler = NTEHandler("txproxy")
ntehandler.setLevel(logging.INFO)
ntlogger.addHandler(ntehandler)

stdout = logging.getLogger("txproxy")
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s %(filename)s:%(lineno)s %(levelname)s] (%(threadName)-10s) %(message)s')
stdout_handler.setFormatter(formatter)
ntlogger.addHandler(stdout_handler)

config = dotenv_values()

app = typer.Typer()

connected = threading.Event()
passed = threading.Event()
emergency_exit = threading.Event()


@app.command()
def init_db():
    """Init empty db"""
    ntlogger.debug("initing db")
    Base.metadata.create_all(engine)
    ntlogger.debug("db inited")


@app.command()
def gen_nssm_bat():
    """Generate nssm commands"""
    name = f"tx {config.get('SFO_CODE').lower()}_{config.get('LOGIN')}"
    display_name = name
    app_dir = pathlib.Path.cwd()
    template = rf'''# securities
.\nssm.exe install "{name}_securities" "{app_dir / 'venv/Scripts/python.exe'}"
.\nssm.exe set "{name}_securities" Application "{app_dir / 'venv/Scripts/python.exe'}"
.\nssm.exe set "{name}_securities" AppDirectory "{app_dir}"
.\nssm.exe set "{name}_securities" AppParameters "process.py securities"
.\nssm.exe set "{name}_securities" DisplayName "{display_name}_securities"
.\nssm.exe set "{name}_securities" Start SERVICE_DEMAND_START
.\nssm.exe set "{name}_securities" AppExit Default Exit 
.\nssm.exe set "{name}_securities" AppStdout "{app_dir / 'logs/log_securities.txt'}"
.\nssm.exe set "{name}_securities" AppStderr "{app_dir / 'logs/log_securities.txt'}"
# .\nssm.exe remove "{name}_securities" confirm

# trades
.\nssm.exe install "{name}_trades" "{app_dir / 'venv/Scripts/python.exe'}"
.\nssm.exe set "{name}_trades" Application "{app_dir / 'venv/Scripts/python.exe'}"
.\nssm.exe set "{name}_trades" AppDirectory "{app_dir}"
.\nssm.exe set "{name}_trades" AppParameters "process.py runserver"
.\nssm.exe set "{name}_trades" DisplayName "{display_name}_trades"
.\nssm.exe set "{name}_trades" Start SERVICE_DEMAND_START
.\nssm.exe set "{name}_trades" AppExit Default Exit 
.\nssm.exe set "{name}_trades" AppStdout "{app_dir / 'logs/runserver_log.txt'}"
.\nssm.exe set "{name}_trades" AppStderr "{app_dir / 'logs/runserver_log.txt'}"
# .\nssm.exe remove "{name}_trades" confirm
'''
    print(template)


@app.command()
def securities():
    """Download and extract securities.
      From Big XML Document returned by get_securities.
      """
    def command_thread():
        ntlogger.debug("command_thread started")
        while not inited.is_set():
            inited.wait()
        ntlogger.debug("inited.is_set()")
        conn_res = lib.SendCommand(cmd_connect_ptr)
        _ = lib.SendCommand(cmd_status_ptr)
        time.sleep(1)
        ntlogger.debug("SendCommand(cmd_status_ptr)")
        while not connected.is_set():
            ntlogger.debug("wait for connected.is_set")
            time.sleep(0.1)
            connected.wait()
        _ = lib.SendCommand(cmd_securities_ptr)
        time.sleep(1)
        ntlogger.debug("SendCommand(cmd_securities_ptr)")

    def init_thread():
        ntlogger.debug("init_thread started")
        initialized = lib.Initialize(log_path_z_ptr, 3) # log_level=3
        ntlogger.debug("Initialized")
        lib.SetCallback.restype = c_bool
        setcallback_res = lib.SetCallback(callback_securities)
        time.sleep(1)
        inited.set()
        ntlogger.debug("inited.set()")
        while not passed.is_set():
            passed.wait()
        ntlogger.debug(f"{passed.is_set()=}")
        un_init_res = lib.UnInitialize()
        ntlogger.debug("Un-initialized", extra={'msg_id': 1})
        ntlogger.info("Exiting", extra={'msg_id': 1})
    inited = threading.Event()
    init = threading.Thread(target=init_thread, name="Init")
    command = threading.Thread(target=command_thread, name="Command")
    init.start()
    command.start()
    while not passed.is_set():
        passed.wait()
    ntlogger.info("Exiting")
    sys.exit(0)
    os._exit(0)

@app.command()
def runserver():
    """Listen finam for new trades"""
    ntlogger.info("Runserver started")
    def command_thread():
        ntlogger.debug("command_thread started")
        while not inited.is_set():
            inited.wait()
        conn_res = lib.SendCommand(cmd_connect_ptr)
        while not connected.is_set():
            time.sleep(0.1)
            connected.wait()
        _ = lib.SendCommand(cmd_status_ptr)

    def init_thread():
        ntlogger.debug("init_thread started")
        initialized = lib.Initialize(log_path_z_ptr, 3) # log_level=3
        ntlogger.debug("Initialized")
        lib.SetCallback.restype = c_bool
        setcallback_res = lib.SetCallback(callback_trades)
        time.sleep(1)
        inited.set()
        while not passed.is_set():
            time.sleep(0.1)
        un_init_res = lib.UnInitialize()
        ntlogger.debug("Un-initialized", extra={'msg_id': 1})
        ntlogger.info("Exiting", extra={'msg_id': 1})
    inited = threading.Event()
    init = threading.Thread(target=init_thread, name="Init")
    command = threading.Thread(target=command_thread, name="Command")
    init.start()
    command.start()

@CFUNCTYPE(c_bool, c_char_p)
def callback_trades(data):
    e = ET.fromstring(data)
    match e:
        case Element(tag='trades'):
            @retry(tries=3, delay=1)
            def post_trade(trade):
                seccode = trade.find('seccode').text
                board = trade.find('board').text
                trade_xml = ET.tostring(ET.ElementTree(trade).getroot())
                trade_xml_pretty = xml.dom.minidom.parseString(trade_xml).toprettyxml()
                try:
                    security = session.scalars(select(Security).filter(Security.seccode == seccode).filter(Security.board == board)).first()
                    ntlogger.debug(f"{trade_xml_pretty}")
                    ntlogger.debug(f"{security.xml}".encode("utf-8"))
                    tradexml = httpx.post(config.get("BO_URL"), data={
                        "trade_xml": trade_xml_pretty,
                        "security_xml": security.xml
                        },
                        headers={
                            'Authorization': f'Bearer {token}',
                            'Accept': 'application/json'
                            },
                        timeout=120
                        )
                    tradexml.raise_for_status()
                    ntlogger.info("TradeXML created!")
                except:
                    content = getattr(tradexml, 'content', None)
                    ntlogger.debug(f"{content=}")
                    ntlogger.exception('http error')
                    raise
                
            trades_xml = ET.tostring(ET.ElementTree(e).getroot())
            trades_xml_pretty = xml.dom.minidom.parseString(trades_xml).toprettyxml()
            ntlogger.debug(f"{trades_xml_pretty}")
            try:
                token = httpx.post(
                    config.get("BO_TOKEN_URL"),
                    data={'username': config.get("BO_TOKEN_LOGIN"), 'password': config.get("BO_TOKEN_PASSWORD")},
                    timeout=30
                    ).json().get("access")
            except httpx.HTTPError:
                ntlogger.exception("Error while receiving TOKEN")
                return
            doc_type = e.tag
            with Session(engine) as session:
                event = Event(
                    date_added=datetime.datetime.now(),
                    data=data,
                    sfo_code=sfo_code,
                    is_processed=0,
                    doc_type=doc_type
                    )
                session.add(event)
                session.commit()
            trades = e.findall('trade')
            with Session(engine) as session:
                for trade in trades:
                    try:
                        post_trade(trade)
                    except:
                        ntlogger.exception("http error after retries")
                        continue

        case Element(tag='securities'):
            securities = e.findall('security')
            ntlogger.debug(f"Got Securities: {len(data)=} {len(securities)=}")
            extract_errors = 0
            if len(data) > 10_000_000:
                ntlogger.debug(" ================================ BIG =================================")
            with Session(engine) as session:
                for security_et in securities:
                    sec_dict = {e.tag: e.text for e in security_et}
                    sec_xml = ET.tostring(ET.ElementTree(security_et).getroot())
                    sec_xml_pretty = xml.dom.minidom.parseString(sec_xml).toprettyxml()
                    security = Security(
                        **sec_dict,
                        xml=sec_xml_pretty
                    )
                    session.add(security)
                    try:
                        session.commit()
                    except:
                        extract_errors += 1
                        continue
            ntlogger.debug(f"Extracted! {extract_errors=}")


@CFUNCTYPE(c_bool, c_char_p)
def callback_securities(data):
    e = ET.fromstring(data)
    match e:
        case Element(tag='server_status', attrib={'connected': 'true'}):
            ntlogger.info("Connected", extra={'msg_id': 1})
            connected.set()
            
        case Element(tag='server_status', attrib={'connected': 'error'}):
            encoded_data = data.decode('utf-8')
            ntlogger.debug(f"{encoded_data=}")
            ntlogger.error("Server status: ERRROR", extra={'msg_id': 1})
            passed.set()

        case Element(tag='server_status', attrib={'connected': 'false'}):
            ntlogger.error("Server status: FALSE", extra={'msg_id': 1})
            emergency_exit.set()
         
        case Element(tag='securities'):
            securities = e.findall('security')
            ntlogger.debug(f"Got Securities: {len(data)=} {len(securities)=}")
            extract_errors = 0
            if len(data) > 10_000_000:
                ntlogger.debug(" ================================ BIG =================================")
            with Session(engine) as session:
                for security_et in securities:
                    sec_dict = {e.tag: e.text for e in security_et}
                    sec_xml = ET.tostring(ET.ElementTree(security_et).getroot())
                    sec_xml_pretty = xml.dom.minidom.parseString(sec_xml).toprettyxml()
                    security = Security(
                        **sec_dict,
                        xml=sec_xml_pretty
                    )
                    session.add(security)
                    try:
                        session.commit()
                    except:
                        extract_errors += 1
                        continue
            ntlogger.debug(f"Extracted! {extract_errors=}")
            if connected.is_set() and len(data) > 10_000_000:
                passed.set()

if __name__ == "__main__":
    app()
