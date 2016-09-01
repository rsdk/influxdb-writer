import random
from time import time, sleep

import measurement_writer

mymmw = measurement_writer.MMW()

t0 = time()
i = 0
for i in range(100):
    fields = {'W': random.uniform(2, 20),
              'L': random.uniform(30, 33),
              'A': random.uniform(60, 90)}
    mymmw.write_value(fields)
    sleep(0.1)
mymmw.commit()
mymmw.stop()
print('Number of rows:', mymmw.count())

print(i, time() - t0, 'Nr of rows per second: {:0.0f}'.format(i / (time() - t0)))
