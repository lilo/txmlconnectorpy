from pathlib import Path
from ctypes import cdll, c_char, c_char_p, c_void_p, c_bool, CFUNCTYPE, POINTER
import tempfile
import time
import threading
import logging
import datetime
from dotenv import dotenv_values
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element
import xml.dom.minidom
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from db import engine
from models import Event, Security, Trade
import httpx

# logging.basicConfig(
#     level=logging.DEBUG,
#     format='[%(asctime)s %(levelname)s] (%(threadName)-10s) %(message)s',
#     filename='tx.log',
#     filemode='w'
#     )

config = dotenv_values()
dir_path = Path(__file__).parent
dll_path = dir_path / 'txmlconnector64.dll'
assert dll_path.is_file()
log_path_z = bytes(f"{str(dir_path / 'logs')}\0", 'utf-8')
lib = cdll.LoadLibrary(str(dll_path))
log_path_z_ptr = c_char_p(log_path_z)


login, password = config.get('LOGIN'), config.get('PASSWORD')
host, port = config.get('HOST'), config.get('PORT')
sfo_code = config.get('SFO_CODE')
cmd_connect = f'''
<command id="connect">
<login>{login}</login>
<password>{password}</password>
<host>{host}</host>
<port>{port}</port>
<session_timeout>25</session_timeout>
<request_timeout>10</request_timeout>
<rqdelay>1000</rqdelay>
</command>'''
cmd_connect_ptr = c_char_p(bytes(cmd_connect, "utf-8"))
lib.SendCommand.restype = c_char_p

cmd_status = f'<command id="server_status" />'
cmd_status_ptr = c_char_p(bytes(cmd_status, "utf-8"))

cmd_securities = f'<command id="get_securities" />'
cmd_securities_ptr = c_char_p(bytes(cmd_securities, "utf-8"))

cmd_disconnect = f'<command id="disconnect" />'
cmd_disconnect_ptr = c_char_p(bytes(cmd_disconnect, "utf-8"))

lib.UnInitialize.restype = c_char_p

connected = threading.Event()

@CFUNCTYPE(c_bool, c_char_p)
def callback(data):
    e = ET.fromstring(data)
    logging.debug(f"{e.tag=}")
    match e:
        case Element(tag='server_status', attrib={'connected': 'true'}):
            connected.set()
            logging.debug("Connected")
        case Element(tag='securities'):
            if e.tag == 'securities' and len(data) > 10_000_000: # TODO: case clause
                doc_type = 'securities_big'
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
        case Element(tag='trades'):
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
            token = httpx.post(
                            config.get("BO_TOKEN_URL"),
                            data={'username': config.get("BO_TOKEN_LOGIN"), 'password': config.get("BO_TOKEN_PASSWORD")},
                            timeout=30
                            ).json().get("access")
            trades = e.findall('trade')
            with Session(engine) as session:
                for trade in trades:
                    seccode = trade.find('seccode').text
                    board = trade.find('board').text
                    logging.debug(f"{seccode=}")
                    logging.debug(f"{board=}")
                    trade_xml = ET.tostring(ET.ElementTree(trade).getroot())
                    trade_xml_pretty = xml.dom.minidom.parseString(trade_xml).toprettyxml()
                    try:
                        security = session.scalars(select(Security).filter(Security.seccode == seccode).filter(Security.board == board)).first()
                        logging.debug(f"{trade_xml_pretty=}")
                        logging.debug(f"{security.xml=}")
                        tradexml = httpx.post(config.get("BO_URL"), data={
                            "trade_xml": trade_xml_pretty,
                            "security_xml": security.xml
                            },
                            headers={'Authorization': f'Bearer {token}'},
                            timeout=30
                            )
                    except BaseException as ex:
                        logging.exception('http post error')
        case _:
            logging.debug(f"callback data: {e.tag}")
    return True

def init_thread():
    initialized = lib.Initialize(log_path_z_ptr, 3) # log_level=3
    print(f'{initialized=}')
    lib.SetCallback.restype = c_bool
    setcallback_res = lib.SetCallback(callback)
    print(f"{setcallback_res=}")
    time.sleep(1)
    inited.set()
    while not passed.is_set():
        pass
    un_init_res = lib.UnInitialize()
    print(f"{un_init_res=}")

def command_thread():
    while not inited.is_set():
        inited.wait()
    conn_res = lib.SendCommand(cmd_connect_ptr)
    print(f"{conn_res=}")
    while not connected.is_set():
        time.sleep(0.1)
        connected.wait()
    status_res = lib.SendCommand(cmd_status_ptr)
    print(f"{status_res=}")
    sec_res = lib.SendCommand(cmd_securities_ptr) # TODO: securities_loaded threading.Event
    print(f"{sec_res=}")
    # time.sleep(30) # TODO:
    # disconn_res = lib.SendCommand(cmd_disconnect_ptr)
    # print(f"{disconn_res=}")
    # passed.set()

if __name__ == "__main__":
    inited = threading.Event()
    passed = threading.Event()
    init = threading.Thread(target=init_thread, name="Init")
    command = threading.Thread(target=command_thread, name="Command")
    init.start()
    command.start()
