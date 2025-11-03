import asyncio
import json
import serial
import time
import paho.mqtt.client as mqtt
from environs import Env
import pycrc

def modbus_crc(data_bytes):
    return pycrc.ModbusCRC(data_bytes)

env = Env()
# Dev: nếu bạn để file .env cạnh main3.py, bật dòng dư.i:
env.read_env(path="/home/root/mqtt/.env")

# MQTT connection
MQTT_BROKER    = env.str("MQTT_BROKER", "")

MQTT_PORT      = env.int("MQTT_PORT", 1883)
MQTT_CLIENT_ID = env.str("MQTT_CLIENT_ID", "remi-subscriber")
MQTT_QOS       = env.int("MQTT_QOS", 1)       
MQTT_KEEPALIVE = env.int("MQTT_KEEPALIVE", 60)
MQTT_USERNAME  = env.str("MQTT_USERNAME", None)
MQTT_PASSWORD  = env.str("MQTT_PASSWORD", None)
MQTT_TLS       = env.bool("MQTT_TLS", False)   
                                               
# Topics (kiểu 2 thiết bị: device-scop
MQTT_TOPIC_BASE = env.str("MQTT_TOPIC_BASE", "relay")
DEVICE_ID       = env.str("DEVICE_ID", None)  # mỗi thiết bị đặt ID khác nha
if DEVICE_ID:
    MQTT_TOPIC_CONTROL = env.str("MQTT_TOPIC_CONTROL", f"{MQTT_TOPIC_BASE}/{DEVICE_ID}/control")

    MQTT_TOPIC_STATUS  = env.str("MQTT_TOPIC_STATUS",  f"{MQTT_TOPIC_BASE}/{DEVICE_ID}/status")
else:
    # Fallback nếu quên đặt DEVICE_ID → dùng kiểu phẳng
    MQTT_TOPIC_CONTROL = env.str("MQTT_TOPIC_CONTROL", f"{MQTT_TOPIC_BASE}/control")
    MQTT_TOPIC_STATUS  = env.str("MQTT_TOPIC_STATUS",  f"{MQTT_TOPIC_BASE}/status")

# Kiểm tra hợp lệ cơ bản cho topic
for name, val in [("MQTT_TOPIC_CONTROL", MQTT_TOPIC_CONTROL),
                  ("MQTT_TOPIC_STATUS",  MQTT_TOPIC_STATUS)]:

    if not val or any(c.isspace() for c in val):
        raise ValueError(f"{name} không hợp lệ: '{val}'")

# Serial / Modbus (MYIR Remi UART)
SERIAL_PORT      = env.str("SERIAL_PORT", "/dev/")
SERIAL_BAUD      = env.int("SERIAL_BAUD", 9600)
MODBUS_SLAVE_ID  = env.int("MODBUS_SLAVE_ID", 1)
READ_RETRY       = env.int("READ_RETRY", 3)
WRITE_DELAY_SEC  = env.float("WRITE_DELAY_SEC", 0.3)
READ_DELAY_SEC   = env.float("READ_DELAY_SEC", 0.2) 
s = serial.Serial(
    port=SERIAL_PORT,
    baudrate=SERIAL_BAUD,
    bytesize=8,
    parity=serial.PARITY_NONE,
    stopbits=1,
    timeout=2,
)
serial_lock = asyncio.Lock()

# Tạo asyncio loop s.m để callback Paho sử dụng
loop = asyncio.get_event_loop()

# ----------------------------
# Đọc trạng thái 8 relay (Read Coils 0..7)
# ----------------------------

async def read_relay_status():
    """
    Trả về 1 byte trạng thái (bit0..bit7), hoặc None nếu thất bại sau READ_RETRY lần.
    Yêu cầu: [slave, 0x01, addr_hi=0x00, addr_lo=0x00, qty_hi=0x00, qty_lo=0x08, crc_lo, crc_hi]
    Phản hồi: [slave, 0x01, byte_count=0x01, status_byte, crc_lo, crc_hi] (6 b]
    """                                                                                  
    for attempt in range(READ_RETRY):                                                          
        cmd = [MODBUS_SLAVE_ID, 0x01, 0x00, 0x00, 0x00, 0x08]                                  
        crc = modbus_crc(cmd)                                                                  
        cmd += [crc & 0xFF, (crc >> 8) & 0xFF]  # CRC Low trư.c, High sau                    
                                                                                               
        async with serial_lock:             
            try:                                                  
                s.reset_input_buffer()                                                         
                                                                                               
            except Exception:                                                                  
                pass                                                                           
            s.write(bytearray(cmd))                                                            
            await asyncio.sleep(READ_DELAY_SEC)                                                
            response = s.read(6)                                                               
                                                                                               
        print(f"[Đọc lần {attempt+1}] RX:", list(response))                              
                                                                                              
        # Kiểm tra độ dài + header + CRC                                                 
        if len(response) == 6 and response[0] == MODBUS_SLAVE_ID and response[1] == 0x01:    
            data_wo_crc = response[:-2]                                                        
            crc_received = response[-2] + (response[-1] << 8)                                  
            crc_calculated = modbus_crc(list(data_wo_crc))                                     
            if crc_received == crc_calculated:
                return response[3]  # status_byte

            else:
                print("[!] CRC không kh.p, bỏ qua phản hồi.")                         
        await asyncio.sleep(0.2)                                                              
                                                                                              
    print(f"[!] Không đọc được trạng thái sau {READ_RETRY} lần thử.")              
    return None 
async def control_relay(relay_index: int, turn_on: bool) -> bool:  
    """                                                                                        
    relay_index: 0..7                                                                          
    turn_on: True/False                                                                        
    """                                                                                        
    if not (0 <= relay_index <= 7):                                                            
        print("[!] Relay index phải từ 0 đến 7.")                                      
        return False                                                                           
                                                                                               
#    current_byte = await read_relay_status()                                                   
#    if current_byte is None:                                                                   
#        print("[!] Không thể đọc trạng thái hiện tại.")                          
#        return False                                                                           

    relay_state = [False] * 8  # CH0 đến CH7                                                                                               
    relay_state[relay_index] = turn_on    

    desired_byte = 0
    for i in range(8):
        if relay_state[i]:
            desired_byte |= (1 << i)

#    desired_byte = current_byte                                                                
#    if turn_on:                                                                                
#        desired_byte |= (1 << relay_index)
#    else:
#        desired_byte &= ~(1 << relay_index)

    status_byte = await read_relay_status()
    for i in range(8):
        actual = bool(status_byte & (1 << i))
        expected = relay_state[i]
        if actual != expected:
            print(f"[!] CH{i} sai trạng thái. Mong muốn: {expected}, thực tế: {actual}")

    # Khung Write Multiple Coils: func=0x0F, addr=0x0000, qty=0x0008, byte_count=0x01, data=desired_byte
    cmd = [MODBUS_SLAVE_ID, 0x0F, 0x00, 0x00, 0x00, 0x08, 0x01, desired_byte]
    crc = modbus_crc(cmd)
    cmd += [crc & 0xFF, (crc >> 8) & 0xFF]

    async with serial_lock:
        s.write(bytearray(cmd))
        await asyncio.sleep(WRITE_DELAY_SEC)
        response = s.read(8)

    print("[Ghi] RX:", list(response))

    # Echo mong đợi: [slave, 0x0F, addr_hi, addr_lo, qty_hi, qty_lo, crc_lo, crc_hi]
    if len(response) == 8 and response[0] == MODBUS_SLAVE_ID and response[1] == 0x0F:
        print(f"[CH{relay_index}] Đã {'BẬT' if turn_on else 'TẮT'} thành công.")
    # Gửi trạng thái m.i lên topic STATUS     
        status_byte = await read_relay_status()
        if status_byte is not None:                                          
            status = {f"CH{i}": bool(status_byte & (1 << i)) for i in range(8)}            
            mqtt_client.publish(MQTT_TOPIC_STATUS, json.dumps(status), qos=MQTT_QOS, retain=False) 
            print("[STATUS] Đã gửi trạng thái lên MQTT.")
        return True                                                                      
    else:                                                                                
        print("[!] Phản hồi không hợp lệ hoặc không nhận được.")      
        return False 
async def handle_mqtt_message(payload_raw: str):                      
    try:                                                                                 
        payload = json.loads(payload_raw)                                               
        relay = int(payload.get("relay"))                                              
        state = str(payload.get("state", "")).strip().lower()                         
                                                                                        
        if relay < 0 or relay > 7:                                                     
            print("[!] 'relay' phải từ 0..7.")                                       
            return                                                                     
        if state not in ("on", "off"):                                            
            print("[!] Trạng thái không hợp lệ. Dùng 'on' hoặc 'off'.")
            return                                                                
                                                                                 
        # Tránh thao tác thừa                                                   
        status_byte = await read_relay_status()                                   
        if status_byte is None:                                                  
            print("[!] Không đọc được trạng thái relay.")                
            return                                                                  
                                                                                      
        current_state = bool(status_byte & (1 << relay))                               
        desired_state = (state == "on")                                              
                                                                                                                                                                                                                                       
        await control_relay(relay, desired_state)                                      
                                                                                    
    except Exception as e:         
        print("[!] Lỗi xử lý tin nhắn MQTT:", e)  

mqtt_client = mqtt.Client(                                                              
    client_id=MQTT_CLIENT_ID,                                                            
    protocol=mqtt.MQTTv311,                                                    
    transport="tcp",                                                                     
    clean_session=False
)                                                               
#    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,                           
                                                                                    
if MQTT_USERNAME and MQTT_PASSWORD:
    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
if MQTT_TLS:                                                                            
    mqtt_client.tls_set() 
def on_connect(client, userdata, flags, reasonCode, properties=None):
    """
    Paho v2: reasonCode là object có .is_success (bool hoặc method) và .value (int).
    Paho v1: reasonCode là int (0 = success).
    """
    # Lấy value nếu có (v2), fallback sang chính reasonCode (v1)
    rc_value = getattr(reasonCode, "value", reasonCode)
    rc_is_success = getattr(reasonCode, "is_success", None)

    ok = False
    try:
        # Trường hợp .is_success là method
        if callable(rc_is_success):
            ok = bool(rc_is_success())
        # Trường hợp .is_success là bool
        elif isinstance(rc_is_success, bool):
            ok = rc_is_success
        else:
            # Fallback: so sánh value v.i 0 (ACCEPTED)
            ok = (int(rc_value) == 0)
    except Exception:
        # Cuối cùng: nếu v1, reasonCode là int
        ok = (isinstance(reasonCode, int) and reasonCode == 0)

    if ok:
        print(f"[MQTT] ✅ Kết nối broker thành công. Subscribing: {MQTT_TOPIC_CONTROL} (QoS={MQTT_QOS})")
        client.subscribe(MQTT_TOPIC_CONTROL, qos=MQTT_QOS)
    else:    print(f"[MQTT] ❌ Kết nối thất bại: {reasonCode} (value={rc_value})")

def on_disconnect(client, userdata, reasonCode, properties=None):
    rc_value = getattr(reasonCode, "value", reasonCode)
    print(f"[MQTT] 🔌 Disconnected: {reasonCode} (value={rc_value})")
mqtt_client.on_disconnect = on_disconnect

def on_message(client, userdata, message):
    payload_str = message.payload.decode(errors="ignore")
    print(f"[MQTT] Nhận tin nhắn tại {message.topic}: {payload_str}")
    # Đẩy xử lý sang asyncio loop
    asyncio.run_coroutine_threadsafe(handle_mqtt_message(payload_str), loop)
 
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Kết nối broker
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=MQTT_KEEPALIVE)
mqtt_client.loop_start()

# ----------------------------
# Khởi động asyncio loop chính
# ----------------------------
print(f"[START] Device='{DEVICE_ID or '(no-id)'}' | control: {MQTT_TOPIC_CONTROL} | status: {MQTT_TOPIC_STATUS}")
loop.run_forever()


