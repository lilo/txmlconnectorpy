from datetime import datetime
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Float, BLOB, Text, DateTime
from sqlalchemy import UniqueConstraint
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Float, BLOB, Text
from sqlalchemy.orm import relationship

from db import Base


class Event(Base):
    __tablename__ = "events"

    id = Column(Integer, primary_key=True, index=True)
    data = Column(Text)
    date_added = Column(Text)
    is_processed = Column(Text)
    doc_type = Column(Text) # either trades or securities
    date_processed = Column(Text)
    msg = Column(Text)
    sfo_code = Column(Text)


class Trade(Base):
    __tablename__ = "trades"
    id = Column(Integer, primary_key=True, index=True)
    sync_date = Column(DateTime)
    sync_src = Column(Text) # Either 
    secid = Column(Text) # 6840</secid>
    tradeno = Column(Text) # 7552577248</tradeno>
    orderno = Column(Text) # 35583714769</orderno>
    board = Column(Text) # TQBR</board>
    seccode = Column(Text) # MOEX</seccode>
    client = Column(Text) # 626B8/626B8</client>
    buysell = Column(Text) # B</buysell>
    union = Column(Text) # 748405RDGQP</union>
    time = Column(Text) # 19.04.2023 10:13:03</time>
    brokerref = Column(Text) # comment
    value = Column(Text) # 5597.5</value>
    comission = Column(Text) #0.56</comission>
    price = Column(Text) # 111.95</price>
    quantity = Column(Text) # 5</quantity>
    items = Column(Text) # 50</items>
    yield_ = Column(Text) # 0.0</yield>
    currentpos = Column(Text) # 0</currentpos>
    accruedint = Column(Text) # 0.0</accruedint>
    tradetype = Column(Text) # T</tradetype>
    settlecode = Column(Text) # Y2</settlecode>
    __table_args__ = (UniqueConstraint('tradeno', name='tradeno_uc'),)


    def get_trade_date(self):
        trade_date = datetime.strptime(self.time, "%d.%m.%Y %H:%M:%S")
        return trade_date

    def get_trade_timestamp(self):
        trade_date = datetime.strptime(self.time, "%d.%m.%Y %H:%M:%S")
        return trade_date

    def get_side_rec_id(self):
        return "FINAM-BROK"

    def get_fee_side_pay_id(self):
        return "SFO-TEST" # TODO: env

    def get_side_market_id(self):
        return "MOEX"

    def get_side_broker_id(self):
        return "FINAM-BROK" # TODO: создать

    def get_side_sell_id(self):
        if self.buysell == "S":
            return "SFO-TEST" # TODO: env
        else:
            return "NKCBANK"

    def get_side_buy_id(self):
        if self.buysell == "B":
            return "SFO-TEST" # TODO: env
        else:
            return "NKCBANK"

    def get_price_currency_id(self):
        return Security # TODO: "ищем в тэге security - currencyid",

    def get_trade_date(self):
        return "time (обрезать до даты)" # TODO:

    def get_trade_timestamp(self):
        return "time (полный timestamp)"
    
    def as_json(self):
        return {
		"external_id": self.tradeno,
		"order": self.orderno,
		"account": self.client,
		"trade_date": self.get_trade_date(),
		"trade_timestamp": self.get_trade_timestamp(),
		"side_buy_id": self.get_side_buy_id(),
		"side_sell_id": self.get_side_sell_id(),
		"side_broker_id": self.get_side_broker_id(),
		"side_market_id": self.get_side_market_id(),

		"add_info": "tradetype. settlecode. venue. board",

		"version_effective_date": self.get_trade_date(),
		"price": self.price,
		"side_buy_id": "или СФО, для которого работает инстанс адаптера, или АО НКЦ - в зависимости от buysell",
		"side_sell_id": "или СФО, для которого работает инстанс адаптера, или АО НКЦ - в зависимости от buysell",
		"side_broker_id": "ID брокера Финам",
		"side_market_id": "ID Московской биржи",
		"add_info": "tradetype. settlecode. venue. board",
		"version_effective_date": self.get_trade_date(),
		"price": self.price,
		"price_currency_id": self.get_price_currency_id(),
		"quantity": "items, надо посмотреть на данные",
		"date_settlement": "определяем на основе settlecode: в нем будет указано сколько торговых дней между датой сделки и расчетов. в БО заведем календари для фондового рынка, валютного рынка и т.д. будем запрашивать сдвиг на нужное количество дней у API БО (этот ендпоинт уже есть: calendar-offset/<int:calendar_id>/<str:start_date>/<str:offset_type>/<str:offset_value>/, CalendarOffsetView)",
		"date_delivery": "=date_settlement",
		"settlement_terms": "CLEARING",
		"equity_id": "искать по secid или seccode", # TODO:

		"fee_amount": self.comission,

		"fee_currency_id": "RUB id",

		"fee_side_pay_id": self.get_fee_side_pay_id(),
		"fee_side_rec_id": self.get_side_rec_id(),
		"equity_id": "искать по secid или seccode",
		"fee_amount": self.comission,
		"fee_currency_id": "RUB id",
		"fee_side_pay_id": "СФО, для которого работает инстанс адаптера",
		"fee_side_rec_id": "ID брокера Финам",
		"fee_date": self.get_trade_date()
	}


class Security(Base):
    __tablename__ = "securities"

    id = Column(Integer, primary_key=True, index=True)
    sec_tz = Column(Text) # "![CDATA[Russian Standard Time]]"
    board = Column(Text) # "TQBR"
    seccode = Column(Text) # "MOEX"
    instrclass = Column(Text) # "E"
    currency = Column(Text) # "RUR"
    shortname = Column(Text) # "МосБиржа"
    decimals = Column(Text) # 2
    market = Column(Text) # 1
    minstep = Column(Text) # 0.01
    lotsize = Column(Text) # 10
    lotdivider = Column(Text) # 1
    point_cost = Column(Text) # 1
    opmask = Column(Text) # <opmask usecredit="yes" bymarket="yes" nosplit="yes" fok="yes" ioc="yes" immorcancel="yes" cancelbalance="yes"/>
    sectype = Column(Text) # "SHARE"
    quotestype = Column(Text) # 1
    currencyid = Column(Text) # "RUB"
    MIC = Column(Text) # 'XHKG'
    xml = Column(Text)
    __table_args__ = (UniqueConstraint("seccode", "board", name='sec_seccode_board_uc'),)
    date_processed = Column(Text)
    msg = Column(Text)
    sfo_code = Column(Text)
