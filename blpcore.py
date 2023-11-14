import pandas as pd
import blpapi
from itertools import starmap
# from xbbg.core import utils, conn, process
import core.overrides as overrides
from collections import OrderedDict

_CON_SYM_ = '_xcon_'
_PORT_ = 8194
SESSION_TERMINATED = blpapi.Name("SessionTerminated")

def fmt_dt(dt, fmt='%Y-%m-%d') -> str:
    return pd.Timestamp(dt).strftime(fmt)


def create_request(
        service: str,
        request: str,
        settings: list = None,
        ovrds: list = None,
        append: dict = None,
        **kwargs,
) -> blpapi.request.Request:
    srv = bbg_service(service=service, **kwargs)
    req = srv.createRequest(request)

    list(starmap(req.set, settings if settings else []))
    if ovrds:
        ovrd = req.getElement('overrides')
        for fld, val in ovrds:
            item = ovrd.appendElement()
            item.setElement('fieldId', fld)
            item.setElement('value', val)
    if append:
        for key, val in append.items():
            vals = [val] if isinstance(val, str) else val
            for v in vals: req.append(key, v)

    return req

def init_request(request: blpapi.request.Request, tickers, flds, **kwargs):
    while bbg_session(**kwargs).tryNextEvent(): pass

    if isinstance(tickers, str): tickers = [tickers]
    for ticker in tickers: request.append('securities', ticker)

    if isinstance(flds, str): flds = [flds]
    for fld in flds: request.append('fields', fld)

    adjust = kwargs.pop('adjust', None)
    if isinstance(adjust, str) and adjust:
        if adjust == 'all':
            kwargs['CshAdjNormal'] = True
            kwargs['CshAdjAbnormal'] = True
            kwargs['CapChg'] = True
        else:
            kwargs['CshAdjNormal'] = 'normal' in adjust or 'dvd' in adjust
            kwargs['CshAdjAbnormal'] = 'abn' in adjust or 'dvd' in adjust
            kwargs['CapChg'] = 'split' in adjust

    if 'start_date' in kwargs: request.set('startDate', kwargs.pop('start_date'))
    if 'end_date' in kwargs: request.set('endDate', kwargs.pop('end_date'))

    for elem_name, elem_val in overrides.proc_elms(**kwargs):
        request.set(elem_name, elem_val)

    ovrds = request.getElement('overrides')
    for ovrd_fld, ovrd_val in overrides.proc_ovrds(**kwargs):
        ovrd = ovrds.appendElement()
        ovrd.setElement('fieldId', ovrd_fld)
        ovrd.setElement('value', ovrd_val)

def connect_bbg(**kwargs) -> blpapi.session.Session:
    # logger = logs.get_logger(connect_bbg, **kwargs)

    if isinstance(kwargs.get('sess', None), blpapi.session.Session):
        session = kwargs['sess']
        # logger.debug(f'Using Bloomberg session {session} ...')
    else:
        sess_opts = blpapi.SessionOptions()
        sess_opts.setServerHost('localhost')
        sess_opts.setServerPort(kwargs.get('port', _PORT_))
        session = blpapi.Session(sess_opts)

    # logger.debug('Connecting to Bloomberg ...')
    if session.start(): return session
    else: raise ConnectionError('Cannot connect to Bloomberg')

def bbg_session(**kwargs) -> blpapi.session.Session:
    port = kwargs.get('port', _PORT_)
    con_sym = f'{_CON_SYM_}//{port}'

    if con_sym in globals():
        if getattr(globals()[con_sym], '_Session__handle', None) is None:
            del globals()[con_sym]

    if con_sym not in globals():
        globals()[con_sym] = connect_bbg(**kwargs)

    return globals()[con_sym]

def bbg_service(service: str, **kwargs) -> blpapi.service.Service:
    port = kwargs.get('port', _PORT_)
    serv_sym = f'{_CON_SYM_}/{port}{service}'

    log_info = f'Initiating service {service} ...'
    if serv_sym in globals():
        if getattr(globals()[serv_sym], '_Service__handle', None) is None:
            log_info = f'Restarting service {service} ...'
            del globals()[serv_sym]

    if serv_sym not in globals():
        # logger.debug(log_info)
        bbg_session(**kwargs).openService(service)
        globals()[serv_sym] = bbg_session(**kwargs).getService(service)

    return globals()[serv_sym]

def send_request(request: blpapi.request.Request, **kwargs):
    # logger = logs.get_logger(send_request, **kwargs)
    try:
        bbg_session(**kwargs).sendRequest(request=request)
    except blpapi.InvalidStateException as e:

        # Delete existing connection and send again
        port = kwargs.get('port', _PORT_)
        con_sym = f'{_CON_SYM_}//{port}'
        if con_sym in globals(): del globals()[con_sym]

        # No error handler for 2nd trial
        bbg_session(**kwargs).sendRequest(request=request)

def flatten(iterable, maps=None, unique=False) -> list:

    if iterable is None: return []
    if maps is None: maps = dict()

    if isinstance(iterable, (str, int, float)):
        return [maps.get(iterable, iterable)]

    x = [maps.get(item, item) for item in _to_gen_(iterable)]
    return list(set(x)) if unique else x


def _to_gen_(iterable):
    from collections.abc import Iterable

    for elm in iterable:
        if isinstance(elm, Iterable) and not isinstance(elm, (str, bytes)):
            yield from _to_gen_(elm)
        else: yield elm

def rec_events(func, **kwargs):
    timeout_counts = 0
    responses = [blpapi.Event.PARTIAL_RESPONSE, blpapi.Event.RESPONSE]
    timeout = kwargs.pop('timeout', 500)
    while True:
        ev = bbg_session(**kwargs).nextEvent(timeout=timeout)
        if ev.eventType() in responses:
            for msg in ev:
                for r in func(msg=msg, **kwargs):
                    yield r
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
        elif ev.eventType() == blpapi.Event.TIMEOUT:
            timeout_counts += 1
            if timeout_counts > 20:
                break
        else:
            for _ in ev:
                if getattr(ev, 'messageType', lambda: None)() \
                    == SESSION_TERMINATED: break

def bbg_session(**kwargs) -> blpapi.session.Session:
    port = kwargs.get('port', _PORT_)
    con_sym = f'{_CON_SYM_}//{port}'

    if con_sym in globals():
        if getattr(globals()[con_sym], '_Session__handle', None) is None:
            del globals()[con_sym]

    if con_sym not in globals():
        globals()[con_sym] = connect_bbg(**kwargs)

    return globals()[con_sym]

def process_hist(msg: blpapi.message.Message, **kwargs) -> dict:
    kwargs.pop('(>_<)', None)
    if not msg.hasElement('securityData'): return {}
    ticker = msg.getElement('securityData').getElement('security').getValue()
    for val in msg.getElement('securityData').getElement('fieldData').values():
        if val.hasElement('date'):
            yield OrderedDict([('ticker', ticker)] + [
                (str(elem.name()), elem.getValue()) for elem in val.elements()
            ])

def bdh(tickers, flds=None, start_date=None, end_date='today', adjust=None, **kwargs):

    if flds is None: flds = ['Last_Price']
    e_dt = fmt_dt(end_date, fmt='%Y%m%d')
    if start_date is None: start_date = pd.Timestamp(e_dt) - pd.Timedelta(weeks=8)
    s_dt = fmt_dt(start_date, fmt='%Y%m%d')

    request = create_request(
        service='//blp/refdata',
        request='HistoricalDataRequest',
        **kwargs,
    )
    init_request(
        request=request, tickers=tickers, flds=flds,
        start_date=s_dt, end_date=e_dt, adjust=adjust, **kwargs
    )
    send_request(request=request, **kwargs)

    res = pd.DataFrame(rec_events(process_hist, **kwargs))
    if kwargs.get('raw', False): return res
    if res.empty or any(fld not in res for fld in ['ticker', 'date']):
        return pd.DataFrame()

    return (
        res
        .set_index(['ticker', 'date'])
        .unstack(level=0)
        .rename_axis(index=None, columns=[None, None])
        .swaplevel(0, 1, axis=1)
        .reindex(columns=flatten(tickers), level=0)
        .reindex(columns=flatten(flds), level=1)
    )

tickers = ['NVDA US Equity', 'AAPL US Equity']
fields = ['High', 'Low', 'Last_Price']
start_date = '2023-09-01'
end_date = '2023-09-20'

hist_tick_data = bdh(tickers=tickers, flds=fields, start_date=start_date, end_date=end_date)

print(hist_tick_data)