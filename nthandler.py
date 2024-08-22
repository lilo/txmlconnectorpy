import logging
from logging import LogRecord
from logging.handlers import NTEventLogHandler
import typer
import win32evtlog
import winerror

class NTEHandler(NTEventLogHandler):
    def getMessageID(self, record: LogRecord) -> int:
        default = super().getMessageID(record)
        return getattr(record, 'msg_id', default)
    
    def getEventCategory(self, record: LogRecord) -> int:
        default = super().getEventCategory(record)
        return getattr(record, 'event_category', default)
    
    def getEventType(self, record: LogRecord) -> int:
        default = super().getEventType(record)
        return getattr(record, 'event_type', default)


logger = logging.getLogger(name="nthandler")
logger.addHandler(NTEHandler("nthandler"))
logger.setLevel(logging.DEBUG)

app = typer.Typer()

@app.command()
def log(): # win32evtlog.EVENTLOG_INFORMATION_TYPE
    logger.debug("pew", extra={'msg_id': win32evtlog.EVENTLOG_SUCCESS})
    logger.info("pew", extra={'msg_id': win32evtlog.EVENTLOG_SUCCESS})
    logger.warning("pew", extra={'msg_id': win32evtlog.EVENTLOG_SUCCESS})
    logger.error("pew", extra={'msg_id': win32evtlog.EVENTLOG_SUCCESS})

if __name__ == '__main__':
    app()