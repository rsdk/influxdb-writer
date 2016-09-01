from concurrent.futures import ThreadPoolExecutor
from time import time

from influxdb import InfluxDBClient


class MMW:
    def __init__(self, host='192.168.0.134', port=8087, username='root', password='root', database='example',
                 measurement_name='rpi_strom', tags={"building": "G2", "level": "1", "roomnr": "1.43"},
                 timeout=4):
        self.client = InfluxDBClient(host=host, port=port, username=username,
                                     password=password, database=database, timeout=timeout)
        # self.client.create_database(database)
        self.measurement_name = measurement_name
        self.tags = tags
        self.json_core = {
            "measurement": self.measurement_name,
            "tags": self.tags
        }
        self.send_buffer = []
        self.last_send = time()
        self.tpe = ThreadPoolExecutor(max_workers=10)
        self.BUFFERSIZE = 2000
        self.WRITETIME = 1

    def stop(self):
        """
        Graceful shutdown.
        Write remaining rows from the buffer and wait till the ThreadPoolExecutor is finished.
        """
        self.commit()
        self.tpe.shutdown(wait=True)

    def write_value(self, fields_dict):
        """
        Append a value to the buffer and send if the buffer is full
        or if it's time.

        :param fields_dict: values to be sent
        """
        jc = self.json_core.copy()
        jc["time"] = int(time() * 1e9)  # from seconds to nanaoseconds
        jc["fields"] = fields_dict
        self.send_buffer.append(jc)
        self.check_and_send_to_db()

    def check_and_send_to_db(self):
        """
        Check if the buffer is full or it is too long since the last send.
        If True, give the work to the ThreadPool and register a callback.
        Finally empty the buffer and reset the timer.
        """
        if len(self.send_buffer) >= self.BUFFERSIZE or (time() - self.last_send) > self.WRITETIME:
            print(len(self.send_buffer))
            sb = self.send_buffer.copy()
            future = self.tpe.submit(self.client.write_points, sb)
            # self.client.write_points(self.send_buffer)
            future.add_done_callback(mycallback)
            self.send_buffer = []
            self.last_send = time()

    def commit(self):
        """
        Write remaining values from the buffer.
        """
        print(len(self.send_buffer))
        try:
            self.client.write_points(self.send_buffer)
        except Exception as e:
            print('network error:', e)
        self.send_buffer = []
        self.last_send = time()

    def count(self):
        """
        Count the values in the Table.

        :return: Database Resultset.
        """
        resultset = ''
        try:
            resultset = self.client.query('select count(L) as Count from rpi_strom;')
        except Exception as e:
            print('network error:', e)
        return resultset


def mycallback(f):
    """
    Callback function for the future.
    Prints error message if the Database write returns False.

    :param f: future
    """
    try:
        if f.result() is False:
            print('error@', f)
    except Exception as e:
        print('network error:', e)
