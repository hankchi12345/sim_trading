import alpaca_trade_api as tradeapi
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta
import logging
import os
import asyncio
import json
import time
import configparser
import websocket
import pytz
import threading

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_config():
    config = configparser.ConfigParser()
    config_file_path = 'config.ini'
    if not os.path.exists(config_file_path):
        logger.critical(f"錯誤：config.ini 檔案未找到於 {os.path.abspath(config_file_path)}。請確保檔案存在。")
        raise FileNotFoundError(f"config.ini 檔案未找到。")
    config.read(config_file_path)
    if 'alpaca' not in config:
        logger.critical("錯誤：config.ini 中缺少 [alpaca] 部分。請檢查配置文件格式。")
        raise ValueError("config.ini 格式錯誤：缺少 [alpaca] 部分。")
    
    required_keys = ['api_key', 'secret_key', 'base_url']
    for key in required_keys:
        if key not in config['alpaca']:
            logger.critical(f"錯誤：config.ini 中 [alpaca] 部分缺少 '{key}'。")
            raise ValueError(f"config.ini 格式錯誤：[alpaca] 部分缺少 '{key}'。")

    return config['alpaca']['api_key'], config['alpaca']['secret_key'], config['alpaca']['base_url']

def fetch_15min_data(api, symbol, start_date, end_date):
    try:
        timezone = pytz.timezone('America/New_York')
        start_datetime = timezone.localize(datetime.combine(start_date, datetime.min.time()))
        end_datetime = timezone.localize(datetime.combine(end_date, datetime.max.time()))

        bars = api.get_crypto_bars(
            symbol,
            timeframe='15Min',
            start=start_datetime.isoformat(),
            end=end_datetime.isoformat()
        ).df

        if bars.empty:
            logger.warning(f"抓取 {symbol} 數據失敗：Alpaca 返回空數據。")
            return None
        
        bars.index = bars.index.tz_convert('America/New_York') 
        bars.index.name = 'timestamp'

        logger.info(f"15分鐘數據 (最近5條):\n{bars.tail(5)}")
        return bars
    except Exception as e:
        logger.error(f"從 Alpaca 抓取 {symbol} 數據失敗: {str(e)}")
        return None

def calculate_kdj(data):
    try:
        data.columns = [col.lower() for col in data.columns]
        
        kdj_result = ta.kdj(data['high'], data['low'], data['close'], length=9)

        # 所以它的輸出列名會是 K_9_3, D_9_3, J_9_3
        expected_k_col = f"K_{9}_3"
        expected_d_col = f"D_{9}_3"
        expected_j_col = f"J_{9}_3"

        if expected_k_col in kdj_result.columns and \
           expected_d_col in kdj_result.columns and \
           expected_j_col in kdj_result.columns:
            
            final_kdj = kdj_result.rename(columns={
                expected_k_col: 'K_9',
                expected_d_col: 'D_9',
                expected_j_col: 'J_9'
            })
            
            final_kdj = final_kdj[['K_9', 'D_9', 'J_9']]
            logger.info(f"KDJ計算結果 (最近1條):\n{final_kdj.tail(1)}")
            return final_kdj
        else:
            logger.error(f"KDJ計算結果中未找到預期的K, D, J列 ({expected_k_col}, {expected_d_col}, {expected_j_col})。實際列名: {kdj_result.columns.tolist()}")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"KDJ計算失敗: {str(e)}")
        return pd.DataFrame()

def place_order(api, symbol, buying_power, side):
    try:
        quotes = api.get_latest_crypto_quotes([symbol])
        if symbol not in quotes or not quotes[symbol]:
            logger.error(f"無法獲取 {symbol} 最新加密貨幣報價。")
            return False
        
        latest_quote = quotes[symbol]
        
        if side == 'buy':
            api.submit_order(
                symbol=symbol,
                notional=buying_power,
                side='buy',
                type='market',
                time_in_force='gtc'
            )
            logger.info(f"成功下買單: 將用 ${buying_power:.2f} 購買 {symbol}")
            return True
        elif side == 'sell':
            logger.warning("place_order 函數的 'sell' 邏輯未實現，請使用 close_position 進行賣出操作。")
            return False
        else:
            logger.error(f"不支援的訂單邊: {side}")
            return False

    except Exception as e:
        logger.error(f"下單失敗 {symbol} ({side}): {str(e)}")
        return False

def close_position(api, symbol):
    try:
        # close_position 會直接嘗試關閉，如果沒有倉位會報錯 404，但那通常是預期的
        api.close_position(symbol)
        logger.info(f"關閉 {symbol} 倉位成功。")
        return True
    except tradeapi.rest.APIError as e:
        # 如果是 404 Not Found 錯誤，表示沒有該倉位，視為成功關閉 (即沒有倉位)
        if e.status_code == 404 and "position does not exist" in str(e).lower():
            logger.info(f"嘗試關閉 {symbol} 倉位，但沒有該倉位，視為成功。")
            return True
        else:
            logger.error(f"關閉 {symbol} 倉位失敗: {str(e)}")
            return False
    except Exception as e:
        logger.error(f"關閉 {symbol} 倉位失敗 (非Alpaca API錯誤): {str(e)}")
        return False

async def websocket_stream(api_key, secret_key, symbol, api):
    ws_url = "wss://stream.data.alpaca.markets/v1beta3/crypto/us"

    def on_message(ws, message):
        try:
            data = json.loads(message)
            if not data or 'T' not in data[0]:
                 if data[0].get('msg') == 'connected':
                     logger.info("WebSocket 已連接。")
                     ws.send(json.dumps({"action": "auth", "key": api_key, "secret": secret_key}))
                     ws.send(json.dumps({"action": "subscribe", "quotes": [symbol], "trade_updates": []}))
                 return

            message_type = data[0]['T']

            if message_type == 'trade_update':
                order = data[0].get('order', {})
                logger.info(f"交易更新: 訂單ID {order.get('id')} 狀態 {order.get('status')} 類型 {order.get('order_type')} 邊 {order.get('side')} 數量 {order.get('qty')} 價格 {order.get('filled_avg_price')}")
            elif message_type == 'q':
                quote = data[0]
                
                try:
                    # position = api.get_position(symbol)
                    # plpc = float(position.unrealized_plpc)
                    # market_value = float(position.market_value)
                    # if plpc < -0.03:
                    #     logger.info(f"{symbol} 虧損達3% (WS觸發止損)")
                    #     close_position(api, symbol)
                    pass # 暫時不處理 WS 中的止損，讓主循環處理
                except Exception as e:
                    # 處理 404 錯誤，不要頻繁記錄為 WARNING
                    if "position does not exist" not in str(e).lower() and "404 client error" not in str(e).lower():
                        logger.warning(f"WS: 獲取 {symbol} 倉位失敗: {e}")
            elif message_type == 's':
                logger.info(f"WS訂閱狀態: {data[0].get('streams')}")

        except Exception as e:
            logger.error(f"WebSocket處理訊息錯誤: {str(e)}")

    def on_error(ws, error):
        logger.error(f"WebSocket錯誤: {error}")

    def on_close(ws, close_status_code, close_msg):
        logger.info(f"WebSocket關閉. 狀態碼: {close_status_code}, 訊息: {close_msg}")
        logger.info("嘗試重新連接 WebSocket...")
        time.sleep(5)
        asyncio.run(websocket_stream(api_key, secret_key, symbol, api))

    def on_open(ws):
        logger.info("WebSocket連接成功，正在認證並訂閱。")
        ws.send(json.dumps({"action": "auth", "key": api_key, "secret": secret_key}))
        ws.send(json.dumps({"action": "subscribe", "quotes": [symbol], "trade_updates": []}))

    ws = websocket.WebSocketApp(ws_url, on_message=on_message, on_error=on_error, on_close=on_close, on_open=on_open)
    ws.run_forever(ping_interval=30, ping_timeout=10)


if __name__ == "__main__":
    symbol = "ETH/USDC"
    initial_cash = 200000.0

    try:
        api_key, secret_key, base_url = load_config()
    except (FileNotFoundError, ValueError) as e:
        logger.critical(f"配置加載失敗，程式終止：{e}")
        exit(1)

    api = tradeapi.REST(api_key, secret_key, base_url, api_version='v2')
    
    ws_thread = None
    try:
        if ws_thread is None or not ws_thread.is_alive():
            ws_thread = threading.Thread(target=lambda: asyncio.run(websocket_stream(api_key, secret_key, symbol, api)), daemon=True)
            ws_thread.start()
            logger.info("WebSocket Stream 啟動成功。")
        else:
            logger.info("WebSocket Stream 已在運行。")
    except Exception as e:
        logger.error(f"啟動 WebSocket Stream 失敗: {e}")
        exit(1)


    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30)

    while True:
        try:
            account = api.get_account()
            buying_power = float(account.buying_power)
            logger.info(f"帳戶可用購買力: ${buying_power:.2f}")

            data = fetch_15min_data(api, symbol, start_date, end_date)
            if data is None or data.empty:
                logger.warning("未能獲取有效的15分鐘K線數據或數據為空，等待1分鐘後重試。")
                time.sleep(60)
                continue
            
            data.columns = [col.lower() for col in data.columns]
            
            kdj = calculate_kdj(data)
            if kdj.empty or len(kdj) < 1:
                logger.warning("KDJ計算失敗或數據不足。")
                time.sleep(60)
                continue
            
            latest_kdj = kdj.iloc[-1]
            prev_kdj = kdj.iloc[-2] if len(kdj) >= 2 else None 

            current_position = None
            try:
                # 使用 list_positions() 來獲取所有倉位，然後判斷是否存在特定 symbol 的倉位
                positions = api.list_positions()
                for p in positions:
                    if p.symbol == symbol:
                        current_position = p
                        break
            except tradeapi.rest.APIError as e:
                # 處理 404 錯誤，當沒有任何倉位時，list_positions() 可能會返回 404
                if e.status_code != 404: # 排除 404 錯誤，因為可能是沒有倉位
                    logger.warning(f"獲取所有倉位信息失敗 (非404): {e}")
                current_position = None # 確保在錯誤情況下將倉位設為 None
            except Exception as e:
                logger.warning(f"獲取所有倉位信息失敗: {e}")
                current_position = None


            if latest_kdj['K_9'] < 20 and current_position is None:
                logger.info(f"{symbol} KDJ K ({latest_kdj['K_9']:.2f}) < 20 且無持倉，觸發買進信號。")
                place_order(api, symbol, buying_power, 'buy')
            
            elif latest_kdj['K_9'] > 80 and current_position is not None:
                logger.info(f"{symbol} KDJ K ({latest_kdj['K_9']:.2f}) > 80 且有持倉，觸發賣出信號。")
                close_position(api, symbol)
            else:
                pos_info = "無持倉" if current_position is None else f"有持倉 {current_position.qty}"
                logger.info(f"無明確交易信號。當前 {symbol} K: {latest_kdj['K_9']:.2f}, D: {latest_kdj['D_9']:.2f}, J: {latest_kdj['J_9']:.2f}. {pos_info}")
            
            logger.info("等待 15 分鐘後重新檢查交易信號...")
            time.sleep(15 * 60)

        except Exception as e:
            logger.error(f"主迴圈發生未預期錯誤: {str(e)}")
            time.sleep(60)
