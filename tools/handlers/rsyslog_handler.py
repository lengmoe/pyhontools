from logging.handlers import SysLogHandler
import socket

class RsyslogHandler(SysLogHandler):
    def emit(self, record):
        try:
            msg = self.format(record)
            if self.ident:
                msg = self.ident + msg
            if self.append_nul:
                msg += '\000'

            prio = '<%d>' % self.encodePriority(self.facility, self.mapPriority(record.levelname))
            prio = prio.encode('utf-8')
            msg = msg.encode('utf-8')
            msg = prio + msg
            if self.unixsocket:
                try:
                    self.socket.send(msg)
                except OSError:
                    self.socket.close()
                    self._connect_unixsocket(self.address)
                    self.socket.send(msg)
            elif self.socktype == socket.SOCK_DGRAM:
                self.socket.sendto(msg[:1024], self.address)
            else:
                self.socket.sendall(msg)
        except Exception:
            self.handleError(record)
