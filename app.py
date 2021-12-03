import sys
import json, time
import pandas as pd
from websocket import create_connection, WebSocketConnectionClosedException
from threading import Thread

if len(sys.argv) != 2:
    print("Usage: app.py TICKER")
    sys.exit(1)


def main(ticker="btcusdt"):
    ws = None
    thread = None
    thread_running = False
    thread_keepalive = None

    param = [f"{ticker}@miniTicker"]
    method = "SUBSCRIBE"

    thread_keepalive = Thread(target=websocket_keepalive)

    thread = Thread(
        target=websocket_thread,
        args=(param, method, {"thread": thread_keepalive, "running": thread_running}),
    )
    thread.start()


def get_minute_data(client, sym, interval, lookback):
    dat = client.get_historical_klines(sym, interval, lookback)
    frame = pd.DataFrame(dat)
    frame = frame.iloc[:, :6]
    frame.columns = ["Time", "Open", "High", "Low", "Close", "Volume"]
    frame = frame.set_index("Time")
    frame.index = pd.to_datetime(frame.index, unit="ms")
    frame = frame.astype(float)
    return frame


def data_to_frame(dat):
    dat = json.loads(dat)
    frame = pd.DataFrame([dat])
    # frame = frame.iloc[:, :6]
    # frame.columns = ["Time", "Open", "High", "Low", "Close", "Volume"]
    # frame = frame.set_index("Time")
    # frame.index = pd.to_datetime(frame.index, unit="ms")
    # frame = frame.astype(float)
    print(frame)
    return frame


def websocket_thread(
    params, method="SUBSCRIBE", misc={"thread": None, "running": False}
):
    global ws
    thread, running = [misc[key] for key in misc]

    if thread == None:
        print("> No defined thread to run.")
        return

    ws = create_connection("wss://stream.binance.com:9443/ws")
    ws.send(
        json.dumps(
            {
                "method": method,
                "params": params,
                "id": 1,
            }
        )
    )

    thread.start()
    while not running:
        try:
            data = ws.recv()
            if data != "":
                msg = json.loads(data)
            else:
                msg = {}
        except ValueError as e:
            print(e)
            print("{} - data: {}".format(e, data))
        except Exception as e:
            print(e)
            print("{} - data: {}".format(e, data))

        if "result" not in msg:
            # data processing:
            data_to_frame(data)

    try:
        if ws:
            ws.close()
    except WebSocketConnectionClosedException:
        pass
    finally:
        thread.join()


def websocket_keepalive(interval=30):
    global ws
    while ws.connected:
        ws.ping("keepalive")
        time.sleep(interval)


if __name__ == "__main__":
    main(sys.argv[1])
