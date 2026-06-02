"""
=================================================================
Real-time Crypto Trading Advisor
Layer: Data Ingestion (Bronze Layer Feeder)
Component: Binance WebSocket Producer

Architecture:
  Binance WSS → JSON Parser → Validator → Kafka (crypto_trades)
                                        ↘ Kafka (crypto_trades_dlq)  ← bad records

Features:
  - Exponential backoff reconnection
  - Schema validation + DLQ routing
  - Prometheus metrics exposure
  - Graceful shutdown (SIGTERM)
=================================================================
"""

import json
import logging # print out information 
import os # control env , read env var from docker
import signal # catch process signal 
import sys # provide interaction with interpreter 
import time # provide time function 
from datetime import datetime, timezone # 
from typing import Optional # hinting type

import websocket # lib provide websocket connection
from kafka import KafkaProducer  # pull data to kafka topic
from kafka.errors import KafkaError # handling kafka error
from prometheus_client import Counter, Gauge, Histogram, start_http_server  # prometheus metrics

# ================================Logging config ==============================================
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"), # os.getenv("LOG_LEVEL", "INFO") -> read LOG_LEVEL from env, if not found, use "INFO"
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("crypto.producer")
# logging.getLogger("crypto.producer") -> get logger instance


#==============================Configuration==========================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_TRADES    = os.getenv("KAFKA_TOPIC_TRADES", "crypto_trades")
TOPIC_DLQ       = os.getenv("KAFKA_TOPIC_DLQ",   "crypto_trades_dlq")
SYMBOL          = os.getenv("SYMBOL", "btcusdt").lower()
WS_URL          = f"wss://stream.binance.com:9443/ws/{SYMBOL}@trade"  # f"wss://stream.binance.com:9443/ws/{SYMBOL}@trade" -> f-string
METRICS_PORT    = int(os.getenv("METRICS_PORT", "8000")) # int(os.getenv("METRICS_PORT", "8000")) -> convert env var to int, if not found, use "8000"
"""
vì khi ta build producer thành 1 image 
-> thì producer.py sẽ dc nạp vào service producer 
-> nên khi run producer.py 
-> os.getenv sẽ tìm trong môi trường của producer service

"""


# Prometheus Metrics 
msgs_produced   = Counter("producer_messages_total",   "Total messages sent to Kafka", ["topic"])  # Total messages sent to Kafka topic
msgs_failed     = Counter("producer_failures_total",   "Total Kafka send failures") # Total Kafka send failures
msgs_dlq        = Counter("producer_dlq_total",        "Total records routed to DLQ") # Total records routed to DLQ
ws_reconnects   = Counter("producer_ws_reconnects",    "WebSocket reconnection count") # WebSocket reconnection count
last_trade_ts   = Gauge("producer_last_trade_timestamp_ms", "Last received trade timestamp")
produce_latency = Histogram(
    "producer_kafka_latency_ms",
    "Kafka produce latency in ms",
    buckets=[1, 5, 10, 25, 50, 100, 250, 500],
)

# ── Required Fields in Binance Trade Event ────────────────────────────────────
REQUIRED_FIELDS = {"e", "E", "s", "t", "p", "q", "T", "m"}

# =========================== Shutdown Flag 
# Khi gui data den kafka -> no gom data vao RAM va gui theo batch 
# when user type Ctrl+C or docker stop -> nhung transaction trong RAM se bi mat
#  Solution : # assign _shutdown = false
# Dung thuvien signal -> when ctrl+c -> shutdown = true -> gui notfication -> 
# -> when shutdown -> _call handle_signal -> set shutdown = True --> websocket lose connection 

_shutdown = False 

def handle_signal(signum, _frame): 
    global _shutdown # global vaiable can be modified in local scope
    logger.info(f"Signal {signum} received - Initiating graceful shutdown") # print out the signal received
    _shutdown = True # set shutdown flag to True

signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT,  handle_signal)

#=========================== Kafka Producer Factory ============================
# this part will build and configure kafka producer
# configration : 
# - JSON serialisation -> 
"""
    Build a KafkaProducer with:
      - JSON serialisation
      - Idempotence (acks=all + retries) for zero data loss
      - Compression for bandwidth efficiency
"""
def create_producer() -> KafkaProducer:
    
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer= lambda v: json.dumps(v).encode("utf-8"), # because kafka accept binary data
        key_serializer = lambda k: k.encode("utf-8") if k else None,
        # Reliability # dam bao ko mat du lieu
        acks="all",  # all kafka broker replicate message
        retries=5, # retry 5 times if failed to send message
        retry_backoff_ms=300, # retry every 300ms
        # Performance #  wait 5ms to fill batch and send
        batch_size=16384,           # 16KB batch
        linger_ms=5,                # wait 5ms to fill batch
        compression_type="gzip",    # compress data to save bandwidth (gzip is built-in Python)
        # Timeouts 
        request_timeout_ms=30_000, # if send message but kafka dont res -> throw exception after 30s
        max_block_ms=10_000, # max time to wait for 
    )


#============================ Message Validation & Transformation =============
# function: check and confirm data valid 
def validate_trade(raw:dict) -> tuple[bool, Optional[str]]:
    missing = REQUIRED_FIELDS - raw.keys() 
    # 8 - 8 = {} -> valid , 
    # 8 - 7 = {'extra_field'} -> invalid 
    # 8 - 9 = {} -> valid 
    if missing: 
        return False, f"Missing required fields: {list(missing)}"
    try: 
        price = float(raw["p"])
        qty = float(raw["q"])
        if price <= 0 or qty <= 0:
            return False, f"Non-positive values: price={price}, qty={qty}"
    except (ValueError, TypeError) as e:
        return False, f"Type cast error: {e}"
    return True, None

def transform_trade(raw:dict) -> dict:
    # map key name from binance to our schema
    trade_ts_ms = int(raw["T"]) # T is trade time in ms
    ingest_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    return {
        "trade_id":       str(raw["t"]),
        "symbol":         str(raw["s"]).upper(),
        "price":          float(raw["p"]),
        "quantity":       float(raw["q"]),
        "is_buyer_maker": bool(raw["m"]),   # True = this is a SELL order
        # Timestamps (all UTC milliseconds)
        "trade_time_ms":  trade_ts_ms,
        "event_time_ms":  int(raw["E"]),
        "ingest_time_ms": ingest_ts,
        # Derived
        "notional_usd":   round(float(raw["p"]) * float(raw["q"]), 4),
        "latency_ms":     ingest_ts - trade_ts_ms,
        # fix DeprecationWarning for UTC datetime in python 3.12+
        "date_partition": datetime.now(timezone.utc).strftime("%Y/%m/%d")
    }


# ================= Kafka send Helpers =============================
# this part will receive data after validate and send to kafka 

def send_to_kafka(
    producer: KafkaProducer, 
    topic: str,
    key: str,
    payload: dict
) -> None: 
    start = time.perf_counter()  # perf_counter() : bấm đồng hồ bắt đầu tính giờ
    future = producer.send(topic, key=key, value=payload)
    # send funct 
    # -> dont wait for kafka response 
    # -> instead of it throw an event to RAM 
    # and return future object and code continue run 
    
    # callback funct -> run if send suscessful
    def on_success(metadata): 
        elapsed_ms = (time.perf_counter() - start) * 1000 # calulate latency = end - start 
        # report latency for promotheus show on Grafana 
        produce_latency.observe(elapsed_ms) 
        # increase success counter
        msgs_produced.labels(topic=topic).inc()

        # đổi thành logger.info để dễ quan sát dữ liệu đang được gửi
        logger.info(
            f"→ {topic}  partition={metadata.partition}  "
            f"offset={metadata.offset}  latency={elapsed_ms:.1f}ms"
        )
        
    def on_error(excp): 
        msgs_failed.inc() # +1 fail for msgs_failed counter
        logger.error(f"Kafka send failed: {excp}", exc_info=True) 
        # logger.error will save "Kafka send faild " + 
    
    future.add_callback(on_success)
    # future.add_callback will run callback funct if send suscessful
    future.add_errback(on_error)
    # future.add_errback will run callback funct if send faild 
    # how it check success or failed? -> from metadata -> 
    
"""

Main Thread
    |
    | producer.send(...)
    v
Producer Buffer (RAM)
    |
    | return Future ngay lập tức
    v
Main Thread tiếp tục chạy

----------------------------

Background Sender Thread
    |
    | lấy record từ buffer
    v
Gửi batch tới Kafka Broker
    |
    +--> thành công -> callback
    |
    +--> thất bại -> errback

"""
# Tại sao send() lại trả về Future ngay lập tức mà không đợi kafka?
"""
Code
    ↓
producer.send() 
    ↓ (chỉ mất ~0.01ms)
→ Đẩy message vào Buffer RAM
→ Trả về Future object ngay lập tức ← Code tiếp tục chạy

[Background Sender Threads] (chạy ngầm)
    ↓
Gom batch → Nén → Gửi thật đến Kafka
    ↓
Nhận response → Cập nhật Future → Gọi on_success / on_error

"""


#========================== Websocket Handlers =====================
class BinanceTradeProducer: 
    def __init__(self):
        self.producer: Optional[KafkaProducer] = None
        self.ws: Optional[websocket.WebSocketApp] = None
        self._backoff = 1  # seconds
    
    def start(self):
        # Prometheus server
        logger.info(f"Starting Prometheus metrics on port {METRICS_PORT}")
        start_http_server(METRICS_PORT)

        # Kafka producer
        logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP}")
        self.producer = create_producer() # build kafka producer 

        # Binance stream 
        logger.info(f"Subscribing to Binance stream: {WS_URL}")
        self._run_ws()

    def _run_ws(self):
        while not _shutdown:
            self.ws = websocket.WebSocketApp( # websocket.WebSocketApp( ) -> create websocket object
                url = WS_URL,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
                # above parameter is a function that will be called when 
                # the corresponding event occurs
                # on_open() -> websocket open successfully
                # on_message() -> receive message from server
                # on_error() -> error occurs
                # on_close() -> websocket close
            )

            #function run_forever()
            # 1. create connection (TCP + TLS) ->  connect to Binance server (wss://stream.binance.com:9443) -> connection susccess -> call on_open()
            # 2. Event Loop -> listen to event from server:  block main thread and run Event loop -> when binance send data to socket -> call on_message() 
            # 3. handle ping-pong -> auto send ping every 20 and if not receive pong -> time out
            # 4. Thoát ra khi kết nối đóng :
            # -> hàm run_foreve() chỉ thoát ra khi:
            # -> Binance server ngắt kết nối
            # -> Timeout (ko nhận dc pong trong 10s)
            # -> on_message() -> ws.close() (_shutdown = true)

            self.ws.run_forever(ping_interval=20, ping_timeout=10) 
    
            if _shutdown: # shutdown = True -> thoát ra
                break 

            ws_reconnects.inc() 
            logger.warning(f"WebSocket disconnected — reconnecting in {self._backoff}s")
            time.sleep(self._backoff)
            self._backoff = min(self._backoff * 2, 60) # double backoff, mãx = 60 s

        if self.producer:
            logger.info("Flushing Kafka producer buffer...") #
            self.producer.flush() # send remaining data in RAM to kafka broker
            self.producer.close() 
        logger.info("Producer shutdown complete.")
    
    # Callback khi connection thành công
    def _on_open(self,ws):
        self._backoff = 1
        logger.info(f"✓ WebSocket connected to {WS_URL}")

    def _on_message(self,ws, message:str):
        if _shutdown: 
            ws.close()
            return 

        try: 
            data = json.loads(message) # decode json 
        except json.JSONDecodeError as e:
            logger.warning(f"JSON parse error: {e} | raw={message[:120]}")
            msgs_dlq.inc()
            return
        
        # ----------- VAlidation 
        valid, reason = validate_trade(data)
        if not valid: 
            dlq_payload = {
                "raw_message":  message,
                "error_reason": reason,
                "received_at":  int(datetime.now(timezone.utc).timestamp() * 1000),
            }
            # send to DLQ
            send_to_kafka(self.producer, TOPIC_DLQ, "dlq", dlq_payload)
            msgs_dlq.inc()
            logger.debug(f"Record routed to DLQ: {reason}")
            return

        # ------------Transform
        trade = transform_trade(data) # data after validate -> data correct format 
        last_trade_ts.set(trade["trade_time_ms"])
        send_to_kafka(self.producer, TOPIC_TRADES, trade["trade_id"], trade)

        """ Luồng xử lý on_message:
        Binance gửi message (chuỗi string JSON thô)
                ↓
            Kiểm tra shutdown
                ↓ (nếu chạy bình thường)
        json.loads() → Giải mã string thành dict Python
                ↓ (nếu JSON lỗi → DLQ + return)
        validate_trade() → Kiểm tra data hợp lệ
                ↓ (nếu không hợp lệ → DLQ + return)
        transform_trade() → Làm sạch + tạo biến mới
                ↓
        send_to_kafka(TOPIC_TRADES) → Gửi vào Kafka
        """
        
    
    def _on_error(self,ws, error):
        logger.error(f"WebSocket error: {error}")
    
    def _on_close(self, ws, close_status_code, close_msg):
        logger.info(f"WebSocket closed — status={close_status_code} msg={close_msg}")

    """ on_error
        
        Mạng bị đứt
            ↓
        on_error(ws, error)   ← In log lỗi
            ↓
        on_close(ws, 1006, "")  ← In log đóng kết nối
            ↓
        run_forever() thoát ra
            ↓
        while loop → reconnect
    """

"""
tóm lại đến dòng run_forever() 
-> tạo connection đến binance và dừng tại đó để binance server send data về (file producer.py 
-> mục đích tạo 1 connection websocket với binance , connection chỉ dừng khi có lỗi hoặc ngắt 
-----------------------------------
python producer.py
        ↓
Khởi tạo BinanceTradeProducer()
        ↓
start() → Bật Prometheus → Tạo KafkaProducer (Luồng ẩn sinh ra)
        ↓
_run_ws() → Tạo WebSocketApp (gắn sẵn 4 hàm callback)
        ↓
run_forever() → Mở TCP tới Binance → DỪNG TẠI ĐÂY
        ↓ (Binance push data)
_on_message() → validate → transform → send_to_kafka
        ↓ (Luồng ẩn gom batch và gửi)
Kafka Broker nhận được data
        ↓ (Lỗi / Timeout / _shutdown=True)
run_forever() thoát ra → _on_close() được gọi
        ↓
while not _shutdown: → Reconnect hoặc thoát hẳn
        ↓ (nếu thoát)
producer.flush() → producer.close() → Kết thúc

"""


#======= Entry point -==================
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info(" Crypto Trading Advisor — Binance Producer")
    logger.info(f" Symbol: {SYMBOL.upper()}")
    logger.info(f" Kafka:  {KAFKA_BOOTSTRAP}")
    logger.info(f" Topic:  {TOPIC_TRADES}")
    logger.info("=" * 60)
    
    BinanceTradeProducer().start()


# Note: viêt file dockerfile -> tạo image cho producer.py thành 1 service