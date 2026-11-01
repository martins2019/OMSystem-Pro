import os
import sys
import ctypes
from ctypes import POINTER, WinDLL, c_double, c_int, c_int64, c_long, c_longlong, c_size_t, c_ubyte, c_wchar_p, c_char, c_ulong, CFUNCTYPE, Structure
from datetime import datetime

# --- DEFINIÇÃO DE ESTRUTURAS CORRESPONDENTES AO MANUAL DA PROFIT DLL ---
class TAssetIDRec(Structure):
    _fields_ = [
        ("pwcTicker", c_wchar_p),
        ("pwcBolsa", c_wchar_p)
    ]

# Tenta importar as demais estruturas de profitTypes se existirem, senão define fallbacks
try:
    from profitTypes import *
except ImportError:
    pass

# --- CONFIGURAÇÕES DA PROFIT DLL ---
ACTIVATION_KEY = "638405787884327813"
DLL_NAME = "ProfitDLL64.dll"

# Variáveis globais de estado para o adaptador MT5 -> ProfitDLL
_profit_dll = None
_connected_status = False
_account_id = None
_broker_id = None
_routing_password = None
_positions_cache = {}
_orders_cache = {}
_last_tick_cache = {}

# Estruturas de compatibilidade para simular o comportamento do MT5
class DummyAccountInfo:
    def __init__(self, login=123456, name="ProfitPro Trader", balance=4000.0, equity=4000.0):
        self.login = login
        self.name = name
        self.balance = balance
        self.equity = equity

class DummyPosition:
    def __init__(self, symbol, ticket, volume, price_open, price_current, profit, pos_type, time):
        self.symbol = symbol
        self.ticket = ticket
        self.volume = volume
        self.price_open = price_open
        self.price_current = price_current
        self.profit = profit
        self.type = pos_type
        self.time = time

class DummyTick:
    def __init__(self, bid, ask, last):
        self.bid = bid
        self.ask = ask
        self.last = last

class DummySymbolInfo:
    def __init__(self, symbol, strike=0.0, expiration=0):
        self.name = symbol
        self.option_strike = strike
        self.expiration_time = expiration

class DummyOrderResult:
    def __init__(self, retcode=10009, order=12345, comment="Done"):
        self.retcode = retcode
        self.order = order
        self.comment = comment

# --- CALLBACKS DA PROFIT DLL ---
@CFUNCTYPE(None, c_int, c_int)
def _state_callback(conn_state, result):
    global _connected_status
    if conn_state == 2 and result == 4: # MARKET_CONNECTED
        _connected_status = True

@CFUNCTYPE(None, c_int, c_wchar_p, c_wchar_p, c_wchar_p)
def _account_callback(broker_id, broker_name, account_id, holder_name):
    global _account_id, _broker_id
    _account_id = account_id
    _broker_id = str(broker_id)

@CFUNCTYPE(None, TAssetIDRec, c_int, c_int, c_int, c_int, c_int, c_double, c_void_p, c_void_p)
def _price_book_callback(asset_id, action, position, side, qtds, count, price, p_sell, p_buy):
    ticker = asset_id.pwcTicker if asset_id.pwcTicker else ""
    _last_tick_cache[ticker] = DummyTick(bid=price, ask=price, last=price)

@CFUNCTYPE(None, TAssetIDRec, c_wchar_p, c_ulong, c_double, c_double, c_int, c_int, c_int, c_int, c_char)
def _new_trade_callback(asset_id, date, trade_num, price, vol, qtd, buy_ag, sell_ag, trade_type, edit):
    ticker = asset_id.pwcTicker if asset_id.pwcTicker else ""
    if ticker:
        _last_tick_cache[ticker] = DummyTick(bid=price, ask=price, last=price)

def initializeDll(path: str = DLL_NAME) -> WinDLL:
    global _profit_dll
    if _profit_dll is not None:
        return _profit_dll

    if not os.path.exists(path):
        path = "ProfitDLL64.dll"

    try:
        _profit_dll = WinDLL(path)
    except Exception as e:
        print(f"Erro ao carregar {path}: {e}")
        return None

    # Configuração de tipos e retornos da DLL
    _profit_dll.SendSellOrder.restype = c_longlong
    _profit_dll.SendBuyOrder.restype = c_longlong
    _profit_dll.SendZeroPosition.restype = c_longlong
    _profit_dll.GetAgentNameByID.restype = c_wchar_p
    _profit_dll.GetAgentShortNameByID.restype = c_wchar_p
    _profit_dll.GetPosition.restype = POINTER(c_int)
    _profit_dll.SendMarketSellOrder.restype = c_int64
    _profit_dll.SendMarketBuyOrder.restype = c_int64

    return _profit_dll

# --- CAMADA DE COMPATIBILIDADE MT5 (SHIM) PARA O PROFITPRO ---
def initialize(path=""):
    dll = initializeDll()
    if not dll:
        return False
    
    res = dll.DLLInitializeLogin(
        c_wchar_p(ACTIVATION_KEY),
        c_wchar_p("DEFAULT_USER"),
        c_wchar_p("DEFAULT_PASS"),
        _state_callback,
        None,
        None,
        _account_callback,
        _new_trade_callback,
        None,
        _price_book_callback,
        None,
        None,
        None,
        None
    )
    return True

def shutdown():
    if _profit_dll:
        try:
            _profit_dll.DLLFinalize()
        except:
            pass

def terminal_info():
    class DummyTerminal:
        connected = True
    return DummyTerminal()

def account_info():
    return DummyAccountInfo()

def positions_get(ticket=None, symbol=None):
    pos_list = []
    for tck, pos in _positions_cache.items():
        if ticket and pos.ticket != ticket:
            continue
        if symbol and pos.symbol != symbol:
            continue
        pos_list.append(pos)
    return tuple(pos_list) if pos_list else ()

def symbol_info_tick(symbol):
    if symbol in _last_tick_cache:
        return _last_tick_cache[symbol]
    return DummyTick(bid=10.0, ask=10.0, last=10.0)

def symbol_info(symbol):
    return DummySymbolInfo(symbol, strike=0.0, expiration=0)

def symbols_get(group=None):
    return []

def copy_rates_from_pos(symbol, timeframe, start_pos, count):
    return None

def history_deals_get(date_from, date_to):
    return ()

def symbol_select(symbol, enable):
    if _profit_dll and symbol:
        try:
            _profit_dll.SubscribeTicker(c_wchar_p(symbol), c_wchar_p("B"))
        except:
            pass
    return True

def order_send(request):
    global _profit_dll
    if not _profit_dll:
        return DummyOrderResult(retcode=-1, order=0, comment="DLL not loaded")

    symbol = request.get("symbol", "")
    volume = int(request.get("volume", 100))
    price = float(request.get("price", 0.0))
    action = request.get("type", 0)
    
    is_buy = (action in [0, 2, 4])
    
    account_ptr = c_wchar_p(_account_id if _account_id else "0")
    broker_ptr = c_wchar_p(_broker_id if _broker_id else "0")
    pass_ptr = c_wchar_p(_routing_password if _routing_password else "")
    ticker_ptr = c_wchar_p(symbol)
    exchange_ptr = c_wchar_p("B")

    try:
        if is_buy:
            if price <= 0:
                ret = _profit_dll.SendMarketBuyOrder(account_ptr, broker_ptr, pass_ptr, ticker_ptr, exchange_ptr, volume)
            else:
                ret = _profit_dll.SendBuyOrder(account_ptr, broker_ptr, pass_ptr, ticker_ptr, exchange_ptr, c_double(price), volume)
        else:
            if price <= 0:
                ret = _profit_dll.SendMarketSellOrder(account_ptr, broker_ptr, pass_ptr, ticker_ptr, exchange_ptr, volume)
            else:
                ret = _profit_dll.SendSellOrder(account_ptr, broker_ptr, pass_ptr, ticker_ptr, exchange_ptr, c_double(price), volume)

        if ret > 0:
            _positions_cache[symbol] = DummyPosition(
                symbol=symbol, ticket=ret, volume=volume, 
                price_open=price, price_current=price, profit=0.0, 
                pos_type=0 if is_buy else 1, time=int(time.time())
            )
            return DummyOrderResult(retcode=10009, order=ret, comment="Done ProfitDLL")
        else:
            return DummyOrderResult(retcode=ret, order=0, comment="ProfitDLL Reject")
    except Exception as e:
        return DummyOrderResult(retcode=-99, order=0, comment=str(e))

# Constantes do MT5 mapeadas
TIMEFRAME_M1 = 1
TIMEFRAME_M5 = 5
TIMEFRAME_M15 = 15
TIMEFRAME_M30 = 30
TIMEFRAME_H1 = 60
TIMEFRAME_D1 = 16408
POSITION_TYPE_BUY = 0
POSITION_TYPE_SELL = 1
ORDER_TYPE_BUY = 0
ORDER_TYPE_SELL = 1
ORDER_TYPE_BUY_LIMIT = 2
ORDER_TYPE_SELL_LIMIT = 3
TRADE_ACTION_DEAL = 1
TRADE_ACTION_PENDING = 2
TRADE_RETCODE_DONE = 10009
ORDER_TIME_GTC = 0
ORDER_FILLING_RETURN = 0
DEAL_ENTRY_OUT = 1