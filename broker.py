import threading
import time

class EventBroker:
    def __init__(self):
        self.canales = {}
        self.lamport_clock = 0
        self.lock = threading.Lock()
        threading.Thread(target=self.heartbeat, daemon=True).start()

    def subscribe(self, canal, callback):
        with self.lock:
            if canal not in self.canales:
                self.canales[canal] = []
            self.canales[canal].append(callback)

    def publish(self, canal, mensaje):
        with self.lock:
            self.lamport_clock += 1
            mensaje["lamport"] = self.lamport_clock
        if canal in self.canales:
            for callback in self.canales[canal]:
                callback(mensaje)

    def heartbeat(self):
        while True:
            time.sleep(5)
            with self.lock:
                print(f"[Heartbeat] Broker activo | Reloj Lamport: {self.lamport_clock} | Canales: {list(self.canales.keys())}")